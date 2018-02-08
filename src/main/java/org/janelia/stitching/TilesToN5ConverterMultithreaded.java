package org.janelia.stitching;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.janelia.stitching.TilesToN5Converter.CloudN5WriterSupplier;
import org.janelia.stitching.TilesToN5Converter.TilesToN5Arguments;
import org.janelia.util.concurrent.MultithreadedExecutor;

import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;

public class TilesToN5ConverterMultithreaded
{
	/**
	 * Converts a stack of TIFF images to N5 breaking the images down into cells of given size using multithreading.
	 *
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
			final String outputN5Path,
			final N5WriterSupplier n5Supplier,
			final Map< String, TileInfo[] > inputTilesChannels,
			final int blockSize,
			final Compression n5Compression ) throws IOException
	{
		final int dimensionality = inputTilesChannels.values().iterator().next()[ 0 ].numDimensions();

		final int[] blockSizeArr = new int[ dimensionality ];
		Arrays.fill( blockSizeArr, blockSize );

		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final Map< String, TileInfo[] > outputTilesChannels = new LinkedHashMap<>();

		try ( final MultithreadedExecutor threadPool = new MultithreadedExecutor() )
		{
			for ( final Entry< String, TileInfo[] > entry : inputTilesChannels.entrySet() )
			{
				final String channelName = entry.getKey();
				n5Supplier.get().createGroup( channelName );

				final TileInfo[] inputTiles = entry.getValue();
				final TileInfo[] outputTiles = new TileInfo[ inputTiles.length ];

				threadPool.run( index ->
						{
							try
							{
								final TileInfo inputTile = inputTiles[ index ];

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
								outputTiles[ index ] = outputTile;

								if ( index % threadPool.getNumThreads() == 0 )
									System.out.println( "  " + ( index + 1 ) + " out of " + inputTiles.length );
							}
							catch ( final IOException e )
							{
								throw new RuntimeException( e );
							}
						},
						inputTiles.length
					);

				outputTilesChannels.put( channelName, outputTiles );
			}
		}
		catch ( final InterruptedException | ExecutionException e )
		{
			throw new RuntimeException( e );
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

		final Map< String, TileInfo[] > outputTilesChannels = convertTiffToN5(
				parsedArgs.getN5OutputPath(),
				cloudN5WriterSupplier,
				inputTilesChannels,
				parsedArgs.getBlockSize(),
				new GzipCompression()
			);

		TilesToN5Converter.saveTilesChannels( parsedArgs.getInputChannelsPath(), outputTilesChannels, parsedArgs.getN5OutputPath() );
		System.out.println( "Done" );
	}
}
