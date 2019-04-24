package org.janelia.stitching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import scala.Tuple3;

public class ConvertTIFFTilesToN5Spark
{
	private static final int MAX_PARTITIONS = 15000;

	public static void convertTilesToN5(
			final JavaSparkContext sparkContext,
			final List< String > inputChannelsPaths,
			final String outputN5Path,
			final int[] blockSize,
			final Compression n5Compression ) throws IOException
	{
		final Map< String, TileInfo[] > inputTilesChannels = getTilesChannels( inputChannelsPaths );
		final CloudN5WriterSupplier cloudN5WriterSupplier = new CloudN5WriterSupplier( outputN5Path );

		final Map< String, TileInfo[] > outputTilesChannels = convertTilesToN5(
				sparkContext,
				inputTilesChannels,
				outputN5Path,
				cloudN5WriterSupplier,
				blockSize,
				n5Compression
			);

		saveTilesChannels( inputChannelsPaths, outputTilesChannels );
	}

	public static < T extends NumericType< T > & NativeType< T > > Map< String, TileInfo[] > convertTilesToN5(
			final JavaSparkContext sparkContext,
			final Map< String, TileInfo[] > inputTilesChannels,
			final String outputN5Path,
			final N5WriterSupplier n5Supplier,
			final int[] blockSize,
			final Compression n5Compression ) throws IOException
	{
		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final List< Tuple3< String, Integer, TileInfo > > inputChannelIndexTileTuples = new ArrayList<>();
		for ( final Entry< String, TileInfo[] > entry : inputTilesChannels.entrySet() )
		{
			final String channelName = entry.getKey();
			n5Supplier.get().createGroup( channelName );

			for ( int i = 0; i < entry.getValue().length; ++i )
				inputChannelIndexTileTuples.add( new Tuple3<>( entry.getKey(), i, entry.getValue()[ i ] )  );
		}

		final List< Tuple3< String, Integer, TileInfo > > outputChannelIndexTileTuples = sparkContext
				.parallelize( inputChannelIndexTileTuples, Math.min( inputChannelIndexTileTuples.size(), MAX_PARTITIONS ) )
				.map( inputChannelIndexTileTuple ->
					{
						final String channelName = inputChannelIndexTileTuple._1();
						final Integer index = inputChannelIndexTileTuple._2();
						final TileInfo inputTile = inputChannelIndexTileTuple._3();

						final String outputTileDatasetPath = convertTileToN5(
								inputTile,
								n5Supplier.get(),
								channelName,
								blockSize,
								n5Compression
							);

						final String outputTilePath = PathResolver.get( outputN5Path, outputTileDatasetPath );
						final TileInfo outputTile = inputTile.clone();
						outputTile.setFilePath( outputTilePath );
						return new Tuple3<>( channelName, index, outputTile );
					}
				).collect();

		final Map< String, TileInfo[] > outputTilesChannels = new LinkedHashMap<>();
		for ( final Entry< String, TileInfo[] > entry : inputTilesChannels.entrySet() )
			outputTilesChannels.put( entry.getKey(), new TileInfo[ entry.getValue().length ] );

		for ( final Tuple3< String, Integer, TileInfo > outputChannelIndexTileTuple : outputChannelIndexTileTuples )
		{
			final String channelName = outputChannelIndexTileTuple._1();
			final Integer index = outputChannelIndexTileTuple._2();
			final TileInfo outputTile = outputChannelIndexTileTuple._3();
			outputTilesChannels.get( channelName )[ index ] = outputTile;
		}

		return outputTilesChannels;
	}

	private static < T extends NumericType< T > & NativeType< T > > String convertTileToN5(
			final TileInfo inputTile,
			final N5Writer n5,
			final String outputGroupPath,
			final int[] blockSize,
			final Compression n5Compression ) throws IOException
	{
		if ( inputTile.numDimensions() != blockSize.length )
			throw new RuntimeException( "dimensionality mismatch" );

		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final String tileDatasetPath = PathResolver.get( outputGroupPath, PathResolver.getFileName( inputTile.getFilePath() ) );
		final ImagePlus imp = ImageImporter.openImage( inputTile.getFilePath() );
		final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );

		final RandomAccessibleInterval< T > imgExtendedDimensions = img.numDimensions() < inputTile.numDimensions() && inputTile.getSize( 2 ) == 1 ? Views.stack( img ) : img;

		if ( !Intervals.equalDimensions( imgExtendedDimensions, new FinalInterval( inputTile.getSize() ) ) )
		{
			throw new RuntimeException( String.format(
					"Image size %s does not match the value from metadata %s, filepath: %s",
					Arrays.toString( Intervals.dimensionsAsLongArray( imgExtendedDimensions ) ),
					Arrays.toString( inputTile.getSize() ),
					inputTile.getFilePath()
				) );
		}

		N5Utils.save( imgExtendedDimensions, n5, tileDatasetPath, blockSize, n5Compression );
		return tileDatasetPath;
	}

	private static Map< String, TileInfo[] > getTilesChannels( final List< String > inputChannelsPath ) throws IOException
	{
		final Map< String, TileInfo[] > tilesChannels = new LinkedHashMap<>();
		for ( final String inputPath : inputChannelsPath )
		{
			final String channelName = getChannelName( inputPath );
			final DataProvider inputDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputPath ) );
			final TileInfo[] channelTiles = inputDataProvider.loadTiles( inputPath );
			tilesChannels.put( channelName, channelTiles );
		}
		return tilesChannels;
	}

	private static String getChannelName( final String tileConfigPath )
	{
		final String filename = PathResolver.getFileName( tileConfigPath );
		final int lastDotIndex = filename.lastIndexOf( '.' );
		final String filenameWithoutExtension = lastDotIndex != -1 ? filename.substring( 0, lastDotIndex ) : filename;
		return filenameWithoutExtension;
	}

	private static void saveTilesChannels( final List< String > inputChannelsPath, final Map< String, TileInfo[] > newTiles ) throws IOException
	{
		for ( final String inputPath : inputChannelsPath )
		{
			final String channelName = getChannelName( inputPath );
			final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputPath ) );
			final TileInfo[] newChannelTiles = newTiles.get( channelName );
			final String newConfigPath = Utils.addFilenameSuffix( inputPath, "-n5" );
			dataProvider.saveTiles( newChannelTiles, newConfigPath );
		}
	}
}
