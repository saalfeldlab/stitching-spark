package org.janelia.stitching;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.CloudURI;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;
import org.janelia.flatfield.FlatfieldCorrection;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.scijava.Context;
import org.scijava.plugin.Parameter;

import ij.ImagePlus;
import net.imagej.ops.OpService;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.ClampingConverter;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealConverter;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;
import scala.Tuple2;

public class DeconvolutionSpark
{
	private static class DeconvolutionCmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-i", aliases = { "--inputConfigurationPath" }, required = true,
				usage = "Path to an input tile configuration file. Multiple configurations (channels) can be passed at once.")
		private List< String > inputChannelsPaths;

		@Option(name = "-p", aliases = { "--psfPath" }, required = true,
				usage = "Path to the point-spread function images. In case of multiple input channels, their corresponding PSFs must be passed in the same order.")
		private List< String > psfPaths;

		@Option(name = "-n", aliases = { "--numIterations" }, required = false,
				usage = "Number of iterations to perform for the deconvolution algorithm.")
		private int numIterations = 10;

		@Option(name = "-b", aliases = { "--backgroundValue" }, required = false,
				usage = "Background value of the data for each channel. If omitted, the pivot value estimated in the Flatfield Correction step will be used.")
		private Double backgroundValue = null;

		private boolean parsedSuccessfully = false;

		public DeconvolutionCmdArgs( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}

			if ( inputChannelsPaths.size() != psfPaths.size() )
				throw new IllegalArgumentException( "Number of input channels should match the number of PSFs" );

			// make sure that input paths are absolute file paths if running on a traditional filesystem
			for ( int i = 0; i < inputChannelsPaths.size(); ++i )
				if ( !CloudURI.isCloudURI( inputChannelsPaths.get( i ) ) )
					inputChannelsPaths.set( i, Paths.get( inputChannelsPaths.get( i ) ).toAbsolutePath().toString() );

			for ( int i = 0; i < psfPaths.size(); ++i )
				if ( !CloudURI.isCloudURI( psfPaths.get( i ) ) )
					psfPaths.set( i, Paths.get( psfPaths.get( i ) ).toAbsolutePath().toString() );
		}
	}

	private static class OpServiceContainer implements AutoCloseable
	{
		@Parameter
		private Context context;

		@Parameter
		private OpService ops;

		public OpServiceContainer()
		{
			new Context( OpService.class ).inject( this );
		}

		public OpService ops()
		{
			return ops;
		}

		@Override
		public void close() throws Exception
		{
			if ( context != null )
			{
				context.dispose();
				context = null;
				ops = null;
			}
		}
	}

	private static final int MAX_PARTITIONS = 15000;

	public static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void main( final String[] args ) throws Exception
	{
		final DeconvolutionCmdArgs parsedArgs = new DeconvolutionCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		final DataProviderType dataProviderType = DataProviderFactory.detectType( parsedArgs.inputChannelsPaths.iterator().next() );
		final DataProvider dataProvider = DataProviderFactory.create( dataProviderType );

		final String outputImagesPath = PathResolver.get( PathResolver.getParent( parsedArgs.inputChannelsPaths.iterator().next() ), "decon-tiles" );
		dataProvider.createFolder( outputImagesPath );

		final Map< Integer, Map< Integer, TileInfo > > channelDeconTilesMap = new TreeMap<>();

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "DeconvolutionSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			// initialize input tiles
			final List< Tuple2< TileInfo, Integer > > tilesAndChannelIndices = new ArrayList<>();
			for ( int ch = 0; ch < parsedArgs.inputChannelsPaths.size(); ++ch )
				for ( final TileInfo tile : dataProvider.loadTiles( parsedArgs.inputChannelsPaths.get( ch ) ) )
					tilesAndChannelIndices.add( new Tuple2<>( tile, ch ) );

			// initialize background value for each channel
			final List< Double > channelBackgroundValues = new ArrayList<>();
			for ( final String channelPath : parsedArgs.inputChannelsPaths )
				channelBackgroundValues.add( parsedArgs.backgroundValue != null ? parsedArgs.backgroundValue : FlatfieldCorrection.getPivotValue( dataProvider, channelPath ) );

			// initialize flatfields for each channel
			final List< RandomAccessiblePairNullable< U, U > > channelFlatfields = new ArrayList<>();
			for ( final String channelPath : parsedArgs.inputChannelsPaths )
				channelFlatfields.add( FlatfieldCorrection.loadCorrectionImages( dataProvider, channelPath, tilesAndChannelIndices.iterator().next()._1().numDimensions() ) );
			final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedChannelFlatfields = sparkContext.broadcast( channelFlatfields );

			final List< Tuple2< TileInfo, Integer > > deconTilesAndChannelIndices = sparkContext.parallelize(
					tilesAndChannelIndices,
					Math.min( tilesAndChannelIndices.size(), MAX_PARTITIONS )
			).map( tileAndChannelIndex->
				{
					final TileInfo tile = tileAndChannelIndex._1();
					final int channelIndex = tileAndChannelIndex._2();

					final DataProvider localDataProvider = DataProviderFactory.create( dataProviderType );

					// load tile image
					final RandomAccessibleInterval< T > tileImg = TileLoader.loadTile( tile, localDataProvider );

					// load PSF image
					final ImagePlus psfImp = localDataProvider.loadImage( parsedArgs.psfPaths.get( channelIndex ) );
					Utils.workaroundImagePlusNSlices( psfImp );
					final RandomAccessibleInterval< T > psfImg = ImagePlusImgs.from( psfImp );

					// convert to float type for the deconvolution to work properly
					final RandomAccessibleInterval< FloatType > tileImgFloat = Converters.convert( tileImg, new RealConverter<>(), new FloatType() );
					final RandomAccessibleInterval< FloatType > psfImgFloat = Converters.convert( psfImg, new RealConverter<>(), new FloatType() );

					// apply flatfield correction
					final RandomAccessibleInterval< FloatType > sourceImgFloat;
					final RandomAccessiblePairNullable< U, U > flatfield = broadcastedChannelFlatfields.value().get( channelIndex );
					if ( flatfield != null )
					{
						final FlatfieldCorrectedRandomAccessible< FloatType, U > flatfieldCorrectedTileImg = new FlatfieldCorrectedRandomAccessible<>( tileImgFloat, flatfield.toRandomAccessiblePair() );
						final RandomAccessibleInterval< U > correctedImg = Views.interval( flatfieldCorrectedTileImg, tileImgFloat );
						sourceImgFloat = Converters.convert( correctedImg, new RealConverter<>(), new FloatType() );
					}
					else
					{
						sourceImgFloat = tileImgFloat;
					}

					// subtract background
					final double backgroundValue = channelBackgroundValues.get( channelIndex );
					final RandomAccessibleInterval< FloatType > sourceImgNoBackground = subtractBackground( sourceImgFloat, backgroundValue );
					final RandomAccessibleInterval< FloatType > psfImgNoBackground = subtractBackground( psfImgFloat, backgroundValue );

					// run decon
					final RandomAccessibleInterval< FloatType > deconImg;
					try ( final OpServiceContainer opServiceContainer = new OpServiceContainer() )
					{
						deconImg = opServiceContainer.ops().deconvolve().richardsonLucy( sourceImgNoBackground, psfImgNoBackground, parsedArgs.numIterations );
					}

					// rescale data back to 16-bit (use the original range)
					final Pair< Double, Double > inputDataMinMax = getMinMax( tileImg );
					final Pair< Double, Double > outputDataMinMax = getMinMax( deconImg );
					final ClampingConverter< FloatType, T > deconConverter = new ClampingConverter<>(
							outputDataMinMax.getA(), outputDataMinMax.getB(),
							inputDataMinMax.getA(), inputDataMinMax.getB()
						);
					final RandomAccessibleInterval< T > convertedDeconImg = Converters.convert( deconImg, deconConverter, Util.getTypeFromInterval( tileImg ) );

					// save resulting decon tile
					final ImagePlus deconImp = Utils.copyToImagePlus( convertedDeconImg );
					final String deconTilePath = PathResolver.get( outputImagesPath, Utils.addFilenameSuffix( PathResolver.getFileName( tile.getFilePath() ), "-decon" ) );
					localDataProvider.saveImage( deconImp, deconTilePath );

					// build resulting decon tile metadata
					final TileInfo deconTile = tile.clone();
					deconTile.setFilePath( deconTilePath );

					return new Tuple2<>( deconTile, tileAndChannelIndex._2() );
				}
			).collect();

			broadcastedChannelFlatfields.destroy();

			// group decon tiles into channels
			for ( final Tuple2< TileInfo, Integer > deconTileAndChannelIndex : deconTilesAndChannelIndices )
			{
				final TileInfo deconTile = deconTileAndChannelIndex._1();
				final int channelIndex = deconTileAndChannelIndex._2();

				if ( !channelDeconTilesMap.containsKey( channelIndex ) )
					channelDeconTilesMap.put( channelIndex, new TreeMap<>() );

				channelDeconTilesMap.get( channelIndex ).put( deconTile.getIndex(), deconTile );
			}
		}

		// save resulting decon tiles metadata
		for ( final Entry< Integer, Map< Integer, TileInfo > > channelDeconTiles : channelDeconTilesMap.entrySet() )
		{
			dataProvider.saveTiles(
					channelDeconTiles.getValue().values().toArray( new TileInfo[ 0 ] ),
					Utils.addFilenameSuffix( parsedArgs.inputChannelsPaths.get( channelDeconTiles.getKey() ), "-decon" )
				);
		}

		System.out.println( "Done" );
	}

	private static < T extends NativeType< T > & RealType< T > > RandomAccessibleInterval< T > subtractBackground(
			final RandomAccessibleInterval< T > img,
			final double backgroundValue )
	{
		final RandomAccessibleInterval< T > ret = new ArrayImgFactory< T >().create( img, Util.getTypeFromInterval( img ) );
		final Cursor< T > imgCursor = Views.flatIterable( img ).cursor();
		final Cursor< T > retCursor = Views.flatIterable( ret ).cursor();
		while ( imgCursor.hasNext() || retCursor.hasNext() )
			retCursor.next().setReal( Math.max( imgCursor.next().getRealDouble() - backgroundValue, 0 ) );
		return ret;
	}

	private static < T extends NativeType< T > & RealType< T > > Pair< Double, Double > getMinMax( final RandomAccessibleInterval< T > img )
	{
		double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
		for ( final T val : Views.iterable( img ) )
		{
			min = Math.min( val.getRealDouble(), min );
			max = Math.max( val.getRealDouble(), max );
		}
		return new ValuePair<>( min, max );
	}
}
