package org.janelia.stitching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.CloudN5WriterSupplier;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.util.ImageImporter;

import ij.ImagePlus;
import loci.formats.FormatException;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import scala.Tuple2;

public class ConvertCZITilesToN5Spark
{
	public static final String tilesN5ContainerName = "tiles.n5";
	public static void convertTilesToN5(
			final JavaSparkContext sparkContext,
			final String inputTilesPath,
			final String outputN5Path,
			final int[] blockSize,
			final Compression n5Compression ) throws IOException
	{
		final DataProvider inputDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputTilesPath ) );
		final TileInfo[] inputTiles = inputDataProvider.loadTiles( inputTilesPath );

		final CloudN5WriterSupplier cloudN5WriterSupplier = new CloudN5WriterSupplier( outputN5Path );

		final Map< String, TileInfo[] > outputTilesChannels = convertTilesToN5(
				sparkContext,
				inputTiles,
				outputN5Path,
				cloudN5WriterSupplier,
				blockSize,
				n5Compression
			);

		saveTilesChannels( inputTilesPath, outputTilesChannels );
	}

	// Used by the parser so this step doesn't need to open the CZI file again to find out the number of channels
	// before submitting data conversion tasks.
	public static void createTargetDirectories(
			final String outputN5Path,
			final int numChannels) throws IOException
	{
		final CloudN5WriterSupplier cloudN5WriterSupplier = new CloudN5WriterSupplier( outputN5Path );
		final N5Writer n5 = cloudN5WriterSupplier.get();
		for ( int ch = 0; ch < numChannels; ++ch )
		{
			final String channelName = getChannelName( ch );
			n5.createGroup( channelName );
		}
	}

	public static < T extends NumericType< T > & NativeType< T > > Map< String, TileInfo[] > convertTilesToN5(
			final JavaSparkContext sparkContext,
			final TileInfo[] inputTiles,
			final String outputN5Path,
			final N5WriterSupplier n5Supplier,
			final int[] blockSize,
			final Compression n5Compression ) throws IOException
	{
		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		// find out whether tile images are stored in separate .czi files or in a single .czi container
		final boolean singleCziContainer = Arrays.asList( inputTiles ).stream().map( tileInfo -> tileInfo.getFilePath() ).distinct().count() == 1;

		final List< Tuple2< Integer, TileInfo > > outputChannelTileTuples = sparkContext
				.parallelize( Arrays.asList( inputTiles ) )
				.flatMap( inputTile ->
					{
						final List< String > outputTileChannelDatasetPaths = convertTileToN5(
								inputTile,
								n5Supplier.get(),
								blockSize,
								n5Compression,
								singleCziContainer
							);

						final List< Tuple2< Integer, TileInfo > > outputChannelTiles = new ArrayList<>();
						for ( int ch = 0; ch < outputTileChannelDatasetPaths.size(); ++ch )
						{
							final String outputChannelTilePath = PathResolver.get( outputN5Path, outputTileChannelDatasetPaths.get( ch ) );
							final TileInfo outputChannelTile = inputTile.clone();
							outputChannelTile.setFilePath( outputChannelTilePath );
							outputChannelTiles.add( new Tuple2<>( Integer.valueOf( ch ), outputChannelTile ) );
						}
						return outputChannelTiles.iterator();
					}
				).collect();

		// group tiles by channel index
		final Map< Integer, List< TileInfo > > tilesGroupedByChannels = new TreeMap<>();
		for ( final Tuple2< Integer, TileInfo > outputChannelTileTuple : outputChannelTileTuples )
		{
			final Integer channelIndex = outputChannelTileTuple._1();
			final TileInfo outputTile = outputChannelTileTuple._2();
			if ( !tilesGroupedByChannels.containsKey( channelIndex ) )
				tilesGroupedByChannels.put( channelIndex, new ArrayList<>() );
			tilesGroupedByChannels.get( channelIndex ).add( outputTile );
		}

		// convert to the expected format (mapping from channel name to an array of tiles)
		final Map< String, TileInfo[] > outputTilesChannels = new LinkedHashMap<>();
		for ( final Entry< Integer, List< TileInfo > > tilesChannelGroup : tilesGroupedByChannels.entrySet() )
			outputTilesChannels.put( getChannelName( tilesChannelGroup.getKey() ), tilesChannelGroup.getValue().toArray( new TileInfo[ 0 ] ) );

		return outputTilesChannels;
	}

	private static < T extends NumericType< T > & NativeType< T > > List< String > convertTileToN5(
			final TileInfo inputTile,
			final N5Writer n5,
			final int[] blockSize,
			final Compression n5Compression,
			final boolean singleCziContainer ) throws IOException, FormatException
	{
		if ( inputTile.numDimensions() != blockSize.length )
			throw new RuntimeException( "dimensionality mismatch" );

		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final ImagePlus[] imps = ImageImporter.openBioformatsImageSeries( inputTile.getFilePath() );

		final ImagePlus imp;
		if ( singleCziContainer )
		{
			if ( inputTile.getIndex() >= imps.length )
				throw new RuntimeException( "Identified that all tile images are stored in a single .czi container, but there are not enough images in the loaded image series (file=" + inputTile.getFilePath() + ", numImages=" + imps.length + ", tileIndex=" + inputTile.getIndex() );
			imp = imps[ inputTile.getIndex() ];
		}
		else
		{
			if ( imps.length != 1 )
				throw new RuntimeException( "Expected one tile image per .czi file, got " + imps.length + " images in file " + inputTile.getFilePath() );
			imp = imps[ 0 ];
		}

		final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );

		System.out.println( "Loaded tile image of size " + Arrays.toString( Intervals.dimensionsAsLongArray( img ) ) );

		if ( img.numDimensions() >= 5 && img.dimension( 4 ) != 1 )
			throw new UnsupportedOperationException( "Multiple timepoints are not supported" );

		final List< String > channelTileDatasetPaths = new ArrayList<>();

		final boolean multichannel = img.numDimensions() > inputTile.numDimensions();
		final int channelDim = 2;

		for ( int ch = 0; ch < ( multichannel ? img.dimension( channelDim ) : 1 ); ++ch )
		{
			final RandomAccessibleInterval< T > channelImg = Views.dropSingletonDimensions( multichannel ? Views.hyperSlice( img, channelDim, ch ) : img );

			if ( !Intervals.equalDimensions( channelImg, new FinalInterval( inputTile.getSize() ) ) )
			{
				throw new RuntimeException( String.format(
						"Image size %s does not match the value from metadata %s, filepath: %s",
						Arrays.toString( Intervals.dimensionsAsLongArray( channelImg ) ),
						Arrays.toString( inputTile.getSize() ),
						inputTile.getFilePath()
					) );
			}

			final String channelName = getChannelName( ch );
			final String channelTileDatasetPath = PathResolver.get( channelName, channelName + "_" + PathResolver.getFileName( inputTile.getFilePath() + ( singleCziContainer ? "_tile" + inputTile.getIndex() : "" ) ) );
			N5Utils.save( channelImg, n5, channelTileDatasetPath, blockSize, n5Compression );
			channelTileDatasetPaths.add( channelTileDatasetPath );
		}

		return channelTileDatasetPaths;
	}

	private static String getChannelName( final int channel )
	{
		return "c" + channel;
	}

	private static void saveTilesChannels( final String inputPath, final Map< String, TileInfo[] > newTiles ) throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputPath ) );
		final String baseDir = PathResolver.getParent( inputPath );
		for ( final Entry< String, TileInfo[] > tilesChannel : newTiles.entrySet() )
		{
			final String channelName = tilesChannel.getKey();
			final TileInfo[] newChannelTiles = tilesChannel.getValue();
			final String newConfigPath = PathResolver.get( baseDir, channelName + "-n5.json" );
			dataProvider.saveTiles( newChannelTiles, newConfigPath );
		}
	}
}
