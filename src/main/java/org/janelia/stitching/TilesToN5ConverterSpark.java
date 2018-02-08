package org.janelia.stitching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.janelia.stitching.TilesToN5Converter.CloudN5WriterSupplier;
import org.janelia.stitching.TilesToN5Converter.TilesToN5Arguments;

import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import scala.Tuple3;

public class TilesToN5ConverterSpark
{
	private static final int MAX_PARTITIONS = 15000;

	/**
	 * Converts a stack of TIFF images to N5 breaking the images down into cells of given size using Spark.
	 *
	 * @param sparkContext
	 * 			Spark context
	 * @param outputPath
	 * 			Base N5 path for constructing new tile paths
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 * @param tilesChannels
	 * 			Input tile configurations for channels
	 * @param blockSize
	 * 			Output block size
	 * @param compression
	 * 			Output N5 compression
	 * @return
	 * 			New tile configurations with updated paths
	 * @throws IOException
	 */
	public static < T extends NumericType< T > & NativeType< T > > Map< String, TileInfo[] > convertTiffToN5(
			final JavaSparkContext sparkContext,
			final Map< String, TileInfo[] > inputTilesChannels,
			final String outputN5Path,
			final N5WriterSupplier n5Supplier,
			final int blockSize,
			final Compression n5Compression ) throws IOException
	{
		final int dimensionality = inputTilesChannels.values().iterator().next()[ 0 ].numDimensions();

		final int[] blockSizeArr = new int[ dimensionality ];
		Arrays.fill( blockSizeArr, blockSize );

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

						final String outputTileDatasetPath = TilesToN5Converter.convertTileToN5(
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

	public static void main( final String... args ) throws IOException
	{
		final TilesToN5Arguments parsedArgs = new TilesToN5Arguments( args );
		if ( !parsedArgs.parsedSuccessfully() )
			System.exit( 1 );

		final Map< String, TileInfo[] > inputTilesChannels = TilesToN5Converter.getTilesChannels( parsedArgs.getInputChannelsPath() );
		final CloudN5WriterSupplier cloudN5WriterSupplier = new CloudN5WriterSupplier( parsedArgs.getN5OutputPath() );

		System.out.println( "Converting tiles to N5..." );

		final Map< String, TileInfo[] > outputTilesChannels;
		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "TilesToN5Converter" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			outputTilesChannels = convertTiffToN5(
					sparkContext,
					inputTilesChannels,
					parsedArgs.getN5OutputPath(),
					cloudN5WriterSupplier,
					parsedArgs.getBlockSize(),
					new GzipCompression()
				);
		}

		TilesToN5Converter.saveTilesChannels( parsedArgs.getInputChannelsPath(), outputTilesChannels, parsedArgs.getN5OutputPath() );
		System.out.println( "Done" );
	}
}
