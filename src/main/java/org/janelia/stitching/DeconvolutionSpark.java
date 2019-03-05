package org.janelia.stitching;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.stitching.TileLoader.TileType;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.scijava.Context;
import org.scijava.plugin.Parameter;

import ij.ImagePlus;
import net.imagej.ops.OpService;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.util.Grids;
import net.imglib2.converter.ClampingConverter;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealConverter;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Scale3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;
import scala.Tuple2;
import scala.Tuple3;

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

		@Option(name = "-z", aliases = { "--psfStepZ" }, required = true,
				usage = "PSF Z step in microns.")
		private double psfStepZ;

		@Option(name = "-n", aliases = { "--numIterations" }, required = false,
				usage = "Number of iterations to perform for the deconvolution algorithm.")
		private int numIterations = 10;

		@Option(name = "-v", aliases = { "--backgroundValue" }, required = false,
				usage = "Background intensity value which will be subtracted from the data and the PSF (one per input channel). If omitted, the pivot value estimated in the Flatfield Correction step will be used (default).")
		private List< Double > backgroundIntensityValues = null;

		@Option(name = "-f", aliases = { "--outputFloat" }, required = false,
				usage = "If specified, the output images are saved as 32-bit float images. If omitted, they are converted into the value range of the input datatype (default).")
		private boolean exportAsFloat = false;

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

			if ( backgroundIntensityValues != null && backgroundIntensityValues.size() != inputChannelsPaths.size() && backgroundIntensityValues.size() != 1 )
				throw new IllegalArgumentException( "Background intensity values should be provided for each input channel" );
		}
	}

	private static class OpServiceContainer
	{
		private static final OpServiceContainer INSTANCE = new OpServiceContainer();

		public static OpServiceContainer getInstance()
		{
			return INSTANCE;
		}

		@Parameter
		private Context context;

		@Parameter
		private OpService ops;

		private OpServiceContainer()
		{
			new Context( OpService.class ).inject( this );
		}

		public OpService ops()
		{
			return ops;
		}
	}

	private static final int[] DEFAULT_BLOCK_SIZE = {128, 128, 64};
	private static final int MAX_PARTITIONS = 15000;

	public static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void main( final String[] args ) throws Exception
	{
		final DeconvolutionCmdArgs parsedArgs = new DeconvolutionCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			throw new IllegalArgumentException( "argument format mismatch" );

		final DataProviderType dataProviderType = DataProviderFactory.detectType( parsedArgs.inputChannelsPaths.iterator().next() );
		final DataProvider dataProvider = DataProviderFactory.create( dataProviderType );

		final String outputImagesPath = PathResolver.get( PathResolver.getParent( parsedArgs.inputChannelsPaths.iterator().next() ), "decon-tiles" );
		dataProvider.createFolder( outputImagesPath );

		final List< Map< Integer, TileInfo > > channelDeconTilesMap;

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "DeconvolutionSpark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.speculation", "true" ) // will restart tasks that run for too long (if decon hangs which may happen occasionally)
			) )
		{
			// load input tile metadata
			final List< TileInfo[] > inputTileChannels = new ArrayList<>();
			for ( final String inputChannelPath : parsedArgs.inputChannelsPaths )
				inputTileChannels.add( dataProvider.loadTiles( inputChannelPath ) );

			// initialize background value for each channel
			final List< Double > channelBackgroundValues = new ArrayList<>();
			for ( int channel = 0; channel < parsedArgs.inputChannelsPaths.size(); ++channel )
			{
				final String channelPath = parsedArgs.inputChannelsPaths.get( channel );

				final double backgroundValue;
				if ( parsedArgs.backgroundIntensityValues != null )
				{
					if ( parsedArgs.backgroundIntensityValues.size() == 1 )
						backgroundValue = parsedArgs.backgroundIntensityValues.get( 0 ); // keep backwards compatibility with the older usage (allow the same value for all input channels)
					else
						backgroundValue = parsedArgs.backgroundIntensityValues.get( channel );

					System.out.println( "User-specified background value for " + PathResolver.getFileName( channelPath ) + ": " + backgroundValue );
				}
				else
				{
					backgroundValue = FlatfieldCorrection.getPivotValue( dataProvider, channelPath );
					System.out.println( "Get background value from the flatfield attributes for " + PathResolver.getFileName( channelPath ) + ": " + backgroundValue );
				}
				channelBackgroundValues.add( backgroundValue );
			}

			// initialize flatfields for each channel
			final List< RandomAccessiblePairNullable< U, U > > channelFlatfields = new ArrayList<>();
			for ( final String channelPath : parsedArgs.inputChannelsPaths )
				channelFlatfields.add( FlatfieldCorrection.loadCorrectionImages( dataProvider, channelPath, inputTileChannels.get( 0 )[ 0 ].numDimensions() ) );
			final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedChannelFlatfields = sparkContext.broadcast( channelFlatfields );

			// get tile image type
			final ImageType inputImageType = inputTileChannels.get( 0 )[ 0 ].getType();
			if ( inputImageType.equals( ImageType.GRAY32 ) && !parsedArgs.exportAsFloat )
			{
				System.out.println( "Input data type is already float, no conversion is needed" );
				parsedArgs.exportAsFloat = true;
			}

			// set appropriate block size for processing
			final int[] processingBlockSize;
			if ( TileLoader.getTileType( inputTileChannels.get( 0 )[ 0 ], dataProvider ) == TileType.N5_DATASET )
				processingBlockSize = TileLoader.getTileN5DatasetAttributes( inputTileChannels.get( 0 )[ 0 ], dataProvider ).getBlockSize();
			else
				processingBlockSize = DEFAULT_BLOCK_SIZE;

			// create processing blocks for each tile to be parallelized
			final List< Tuple3< Integer, TileInfo, Interval > > channelIndicesAndTileBlocks = new ArrayList<>();
			for ( int ch = 0; ch < inputTileChannels.size(); ++ch )
				for ( final TileInfo tile : inputTileChannels.get( ch ) )
					for ( final Interval processingBlock : Grids.collectAllContainedIntervals( tile.getSize(), processingBlockSize ) )
						channelIndicesAndTileBlocks.add( new Tuple3<>( ch, tile, processingBlock ) );

			// set output N5 dataset paths for float decon tiles
			final List< Map< Integer, String > > channelDeconTilesFloatN5DatasetPaths = new ArrayList<>();
			for ( int ch = 0; ch < inputTileChannels.size(); ++ch )
			{
				final Map< Integer, String > tileIndexToDatasetPath = new TreeMap<>();
				for ( final TileInfo tile : inputTileChannels.get( ch ) )
					tileIndexToDatasetPath.put( tile.getIndex(), PathResolver.get( getChannelName( parsedArgs.inputChannelsPaths.get( ch ) ), PathResolver.getFileName( tile.getFilePath() ) + "-decon-float" ) );
				channelDeconTilesFloatN5DatasetPaths.add( tileIndexToDatasetPath );
			}

			// create N5 datasets for output decon tiles
			final String n5DeconTilesFloatPath = PathResolver.get( outputImagesPath, "decon-tiles-float.n5" );
			final N5Writer n5DeconTilesFloatWriter = dataProvider.createN5Writer( n5DeconTilesFloatPath );
			for ( int ch = 0; ch < inputTileChannels.size(); ++ch )
				for ( final TileInfo tile : inputTileChannels.get( ch ) )
					n5DeconTilesFloatWriter.createDataset( channelDeconTilesFloatN5DatasetPaths.get( ch ).get( tile.getIndex() ), tile.getSize(), processingBlockSize, DataType.FLOAT32, new GzipCompression() );

			sparkContext.parallelize( channelIndicesAndTileBlocks, Math.min( channelIndicesAndTileBlocks.size(), MAX_PARTITIONS ) ).foreach( tileBlockAndChannelIndex ->
				{
					final int channelIndex = tileBlockAndChannelIndex._1();
					final TileInfo tile = tileBlockAndChannelIndex._2();
					final Interval processingBlock = tileBlockAndChannelIndex._3();

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

					// rescale PSF with respect to the pixel resolution
					final long[] rescaledPsfDimensions = Intervals.dimensionsAsLongArray( psfImgFloat );
					rescaledPsfDimensions[ 2 ] = Math.round( psfImgFloat.dimension( 2 ) * ( parsedArgs.psfStepZ / tile.getPixelResolution( 2 ) ) );
					final Scale3D psfScalingTransform = new Scale3D( 1, 1, parsedArgs.psfStepZ / tile.getPixelResolution( 2 ) );
					final RandomAccessible< FloatType > interpolatedRescaledPsfImg = RealViews.affine( Views.interpolate( Views.extendBorder( psfImgFloat ), new NLinearInterpolatorFactory<>() ), psfScalingTransform );
					final RandomAccessibleInterval< FloatType > rescaledPsfImg = Views.interval( interpolatedRescaledPsfImg, new FinalInterval( rescaledPsfDimensions ) );
					System.out.println( "Rescaled PSF size is " + Arrays.toString( rescaledPsfDimensions ) );

					// pad the processing block by half the size of the rescaled PSF
					final long[] paddedProcessingBlockMin = new long[ processingBlock.numDimensions() ], paddedProcessingBlockMax = new long[ processingBlock.numDimensions() ];
					for ( int d = 0; d < processingBlock.numDimensions(); ++d )
					{
						paddedProcessingBlockMin[ d ] = Math.max( processingBlock.min( d ) - rescaledPsfDimensions[ d ], tileImg.min( d ) );
						paddedProcessingBlockMax[ d ] = Math.min( processingBlock.max( d ) + rescaledPsfDimensions[ d ], tileImg.max( d ) );
					}
					final Interval paddedProcessingBlock = new FinalInterval( paddedProcessingBlockMin, paddedProcessingBlockMax );

					// get padded processing block image
					final RandomAccessibleInterval< FloatType > paddedProcessingBlockImg = Views.interval( sourceImgFloat, paddedProcessingBlock );

					// subtract background
					final double backgroundValue = channelBackgroundValues.get( channelIndex );
					final RandomAccessibleInterval< FloatType > paddedProcessingBlockImgNoBackground = subtractBackground( paddedProcessingBlockImg, backgroundValue );
					final RandomAccessibleInterval< FloatType > psfImgNoBackground = subtractBackground( rescaledPsfImg, backgroundValue );

					// normalize the PSF
					double psfSum = 0;
					for ( final FloatType val : Views.iterable( psfImgNoBackground ) )
						psfSum += val.get();
					for ( final FloatType val : Views.iterable( psfImgNoBackground ) )
						val.set( ( float ) ( val.get() / psfSum ) );

					// run decon
					final RandomAccessibleInterval< FloatType > paddedProcessingBlockDeconImg = OpServiceContainer.getInstance().ops().deconvolve().richardsonLucy(
							paddedProcessingBlockImgNoBackground,
							psfImgNoBackground,
							parsedArgs.numIterations
						);

					// crop the deconvolved processing block from the padded image
					final RandomAccessibleInterval< FloatType > processingBlockDeconImg =
							Views.interval( // 3. Crop the unpadded interval
									Views.translate( // 2. Translated it to its padded position
											Views.zeroMin( // 1. Set the resulting image position to 0
													paddedProcessingBlockDeconImg
												),
											Intervals.minAsLongArray( paddedProcessingBlock )
										),
								processingBlock
							);

					// save the resulting decon block into the N5 dataset for this tile
					final N5Writer localN5DeconTilesFloatWriter = localDataProvider.createN5Writer( n5DeconTilesFloatPath );
					final String outputDatasetPath = channelDeconTilesFloatN5DatasetPaths.get( channelIndex ).get( tile.getIndex() );
					final long[] gridOffset = new long[ processingBlockSize.length ];
					Arrays.setAll( gridOffset, d -> processingBlockDeconImg.min( d ) / processingBlockSize[ d ] );
					N5Utils.saveBlock( processingBlockDeconImg, localN5DeconTilesFloatWriter, outputDatasetPath, gridOffset );
				}
			);

			broadcastedChannelFlatfields.destroy();

			// create resulting tile configuration for decon N5 float output
			final List< Map< Integer, TileInfo > > channelDeconTilesFloatMap = new ArrayList<>();
			for ( int ch = 0; ch < inputTileChannels.size(); ++ch )
			{
				final Map< Integer, TileInfo > tileIndexToDeconTileFloat = new TreeMap<>();
				for ( final TileInfo tile : inputTileChannels.get( ch ) )
				{
					final TileInfo deconTileFloat = tile.clone();
					deconTileFloat.setFilePath( PathResolver.get( n5DeconTilesFloatPath, channelDeconTilesFloatN5DatasetPaths.get( ch ).get( tile.getIndex() ) ) );
					deconTileFloat.setType( ImageType.GRAY32 );
					tileIndexToDeconTileFloat.put( deconTileFloat.getIndex(), deconTileFloat );
				}
				channelDeconTilesFloatMap.add( tileIndexToDeconTileFloat );
			}

			if ( !parsedArgs.exportAsFloat )
			{
				System.out.println( "Need to convert data from float to " + inputImageType + ", collecting min/max values of the resulting decon stack for each channel..." );

				// collect stack min and max values of the resulting deconvolved collection of tiles for each channel
				final List< Tuple2< Double, Double > > channelGlobalMinMaxIntensityValues = new ArrayList<>();
				for ( final Map< Integer, TileInfo > deconTilesFloatMap : channelDeconTilesFloatMap )
				{
					final Tuple2< Double, Double > globalDeconMinMaxValues = sparkContext.parallelize(
							new ArrayList<>( deconTilesFloatMap.values() ),
							Math.min( deconTilesFloatMap.size(), MAX_PARTITIONS )
					).map( deconTile ->
						{
							final DataProvider localDataProvider = DataProviderFactory.create( dataProviderType );

							// load 32-bit decon tile image
							final RandomAccessibleInterval< FloatType > deconTileImg = TileLoader.loadTile( deconTile, localDataProvider );

							// find min and max intensity values of the image
							double minValue = Double.POSITIVE_INFINITY, maxValue = Double.NEGATIVE_INFINITY;
							for ( final FloatType val : Views.iterable( deconTileImg ) )
							{
								minValue = Math.min( val.get(), minValue );
								maxValue = Math.max( val.get(), maxValue );
							}

							return new Tuple2<>( minValue, maxValue );
						}
					).treeReduce( ( a, b ) -> new Tuple2<>(
							Math.min( a._1(), b._1() ),
							Math.max( a._2(), b._2() )
						),
						Integer.MAX_VALUE  // max possible aggregation depth
					);

					channelGlobalMinMaxIntensityValues.add( globalDeconMinMaxValues );
				}

				// log stats
				System.out.println( "Rescaling the intensity range of the resulting decon data..." );
				for ( int ch = 0; ch < channelGlobalMinMaxIntensityValues.size(); ++ch )
				{
					System.out.println( String.format(
							"  Channel %d: [%.2f, %.2f] -> [%.2f, %.2f]",
							ch,
							channelGlobalMinMaxIntensityValues.get( ch )._1(),
							channelGlobalMinMaxIntensityValues.get( ch )._2(),
							inputImageType.getType().getMinValue(),
							inputImageType.getType().getMaxValue()
						) );
				}

				// create channel indices and decon tile metadata to be parallelized
				final List< Tuple2< Integer, TileInfo > > channelIndicesAndDeconTilesFloat = new ArrayList<>();
				for ( int ch = 0; ch < channelDeconTilesFloatMap.size(); ++ch )
					for ( final TileInfo deconTileFloat : channelDeconTilesFloatMap.get( ch ).values() )
						channelIndicesAndDeconTilesFloat.add( new Tuple2<>( ch, deconTileFloat ) );

				// set output N5 dataset paths for converted decon tiles
				final List< Map< Integer, String > > channelDeconTilesConvertedN5DatasetPaths = new ArrayList<>();
				for ( int ch = 0; ch < inputTileChannels.size(); ++ch )
				{
					final Map< Integer, String > tileIndexToDatasetPath = new TreeMap<>();
					for ( final TileInfo tile : inputTileChannels.get( ch ) )
						tileIndexToDatasetPath.put( tile.getIndex(), PathResolver.get( getChannelName( parsedArgs.inputChannelsPaths.get( ch ) ), PathResolver.getFileName( tile.getFilePath() ) + "-decon" ) );
					channelDeconTilesConvertedN5DatasetPaths.add( tileIndexToDatasetPath );
				}

				// set output N5 path where converted tiles will be stored
				final String n5DeconTilesPath = PathResolver.get( outputImagesPath, "decon-tiles.n5" );

				sparkContext.parallelize( channelIndicesAndDeconTilesFloat, Math.min( channelIndicesAndDeconTilesFloat.size(), MAX_PARTITIONS ) ).foreach( channelIndexAndDeconTileFloat ->
					{
						final int channelIndex = channelIndexAndDeconTileFloat._1();
						final TileInfo deconTileFloat = channelIndexAndDeconTileFloat._2();
						final DataProvider localDataProvider = DataProviderFactory.create( dataProviderType );

						// load 32-bit decon tile image
						final RandomAccessibleInterval< FloatType > deconTileFloatImg = TileLoader.loadTile( deconTileFloat, localDataProvider );

						// convert the image data to original data type mapping the intensities of the resulting stack into the value range of the target data type
						final Tuple2< Double, Double > globalDeconMinMaxValues = channelGlobalMinMaxIntensityValues.get( channelIndex );
						final ClampingConverter< FloatType, T > rescalingConverter = new ClampingConverter<>(
								globalDeconMinMaxValues._1(), globalDeconMinMaxValues._2(),
								inputImageType.getType().getMinValue(), inputImageType.getType().getMaxValue()
							);
						@SuppressWarnings( "unchecked" )
						final RandomAccessibleInterval< T > convertedDeconTileImg = Converters.convert( deconTileFloatImg, rescalingConverter, ( T ) inputImageType.getType() );

						// save the converted decon tile image as an N5 dataset
						final N5Writer localN5DeconTilesWriter = localDataProvider.createN5Writer( n5DeconTilesPath );
						final String outputDatasetPath = channelDeconTilesConvertedN5DatasetPaths.get( channelIndex ).get( deconTileFloat.getIndex() );
						N5Utils.save( convertedDeconTileImg, localN5DeconTilesWriter, outputDatasetPath, processingBlockSize, new GzipCompression() );

						// delete intermediate 32-bit decon tile N5 dataset
						final N5Writer localN5DeconTilesFloatWriter = localDataProvider.createN5Writer( n5DeconTilesFloatPath );
						final String intermediateFloatDatasetPath = channelDeconTilesFloatN5DatasetPaths.get( channelIndex ).get( deconTileFloat.getIndex() );
						localN5DeconTilesFloatWriter.remove( intermediateFloatDatasetPath );
					}
				);

				// delete N5 container for intermediate 32-bit decon
				n5DeconTilesFloatWriter.remove();

				// create resulting tile configuration for decon N5 converted output
				channelDeconTilesMap = new ArrayList<>();
				for ( int ch = 0; ch < inputTileChannels.size(); ++ch )
				{
					final Map< Integer, TileInfo > tileIndexToDeconTile = new TreeMap<>();
					for ( final TileInfo tile : inputTileChannels.get( ch ) )
					{
						final TileInfo deconTile = tile.clone();
						deconTile.setFilePath( PathResolver.get( n5DeconTilesPath, channelDeconTilesConvertedN5DatasetPaths.get( ch ).get( tile.getIndex() ) ) );
						tileIndexToDeconTile.put( deconTile.getIndex(), deconTile );
					}
					channelDeconTilesMap.add( tileIndexToDeconTile );
				}
			}
			else
			{
				// if no conversion is required, use the existing float configuration
				channelDeconTilesMap = channelDeconTilesFloatMap;
			}
		}

		// save resulting decon tiles metadata
		for ( int ch = 0; ch < channelDeconTilesMap.size(); ++ch )
		{
			dataProvider.saveTiles(
					channelDeconTilesMap.get( ch ).values().toArray( new TileInfo[ 0 ] ),
					Utils.addFilenameSuffix( parsedArgs.inputChannelsPaths.get( ch ), "-decon" )
				);
		}

		System.out.println( "Done" );
	}

	private static < T extends NativeType< T > & RealType< T > > RandomAccessibleInterval< T > subtractBackground(
			final RandomAccessibleInterval< T > img,
			final double backgroundValue )
	{
		final RandomAccessibleInterval< T > ret = new ArrayImgFactory<>( Util.getTypeFromInterval( img ) ).create( img );
		final Cursor< T > imgCursor = Views.flatIterable( img ).cursor();
		final Cursor< T > retCursor = Views.flatIterable( ret ).cursor();
		while ( imgCursor.hasNext() || retCursor.hasNext() )
			retCursor.next().setReal( imgCursor.next().getRealDouble() - backgroundValue );
		return ret;
	}

	private static String getChannelName( final String tileConfigPath )
	{
		final String filename = PathResolver.getFileName( tileConfigPath );
		final int lastDotIndex = filename.lastIndexOf( '.' );
		final String filenameWithoutExtension = lastDotIndex != -1 ? filename.substring( 0, lastDotIndex ) : filename;
		return filenameWithoutExtension;
	}
}
