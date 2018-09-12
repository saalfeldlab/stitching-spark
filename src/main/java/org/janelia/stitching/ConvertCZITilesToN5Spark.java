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
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.janelia.util.ImageImporter;

import ij.ImagePlus;
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

		saveTilesChannels( inputTilesPath, outputTilesChannels, outputN5Path );
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

		final List< Tuple2< Integer, TileInfo > > outputChannelTileTuples = sparkContext
				.parallelize( Arrays.asList( inputTiles ) )
				.flatMap( inputTile ->
					{
						final List< String > outputTileChannelDatasetPaths = convertTileToN5(
								inputTile,
								n5Supplier.get(),
								blockSize,
								n5Compression
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
			final Compression n5Compression ) throws IOException
	{
		if ( inputTile.numDimensions() != blockSize.length )
			throw new RuntimeException( "dimensionality mismatch" );

		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final ImagePlus imp = ImageImporter.openImage( inputTile.getFilePath() );
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
			final String channelTileDatasetPath = PathResolver.get( channelName, channelName + "_" + PathResolver.getFileName( inputTile.getFilePath() ) );
			N5Utils.save( channelImg, n5, channelTileDatasetPath, blockSize, n5Compression );
			channelTileDatasetPaths.add( channelTileDatasetPath );
		}

		return channelTileDatasetPaths;
	}

	private static String getChannelName( final int channel )
	{
		return "c" + channel;
	}

	private static void saveTilesChannels( final String inputPath, final Map< String, TileInfo[] > newTiles, final String n5Path ) throws IOException
	{
		final DataProvider dataProvider = new CloudN5WriterSupplier( n5Path ).getDataProvider();
		for ( final Entry< String, TileInfo[] > tilesChannel : newTiles.entrySet() )
		{
			final String channelName = tilesChannel.getKey();
			final TileInfo[] newChannelTiles = tilesChannel.getValue();
			final String newConfigPath = PathResolver.get( n5Path, channelName + "_" + Utils.addFilenameSuffix( PathResolver.getFileName( inputPath ), "-converted-n5" ) );
			dataProvider.saveTiles( newChannelTiles, newConfigPath );
		}
	}
}
