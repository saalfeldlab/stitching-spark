package org.janelia.util;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.util.concurrent.MultithreadedExecutor;

import ij.IJ;
import ij.ImagePlus;

public class TiffSliceReaderBenchmark
{
	private static final Random rnd = new Random();
	private static int testImagesCount, repeats;

	public static void main( final String[] args ) throws IOException, InterruptedException, ExecutionException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		System.out.println( "tiles count = " + tiles.length );
		final TileInfo[] tiles1 = args.length > 1 ? TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) ) : null;
		testImagesCount = Math.min( 10, tiles.length );
		repeats = 5;

		try ( final MultithreadedExecutor threadPool = new MultithreadedExecutor( 50 ) )
		{
			final long elapsedSlice = benchmarkSliceReader( tiles, threadPool, ( int ) ( tiles[ 0 ].numDimensions() == 2 ? 1 : tiles[ 0 ].getSize( 2 ) / 2 ) );
			System.out.println();
			final long elapsedFullImage = benchmarkFullImageReader( tiles1 != null ? tiles1 : tiles, threadPool );
			System.out.println();

			System.out.println( "Slice reader: " + elapsedSlice / 1e9 );
			System.out.println( "Full image reader: " + elapsedFullImage / 1e9 );
		}
	}

	private static long benchmarkSliceReader( final TileInfo[] tiles, final MultithreadedExecutor threadPool, final int slice ) throws InterruptedException, ExecutionException, IOException
	{
		return benchmark( tiles, threadPool, slice );
	}

	private static long benchmarkFullImageReader( final TileInfo[] tiles, final MultithreadedExecutor threadPool ) throws InterruptedException, ExecutionException, IOException
	{
		return benchmark( tiles, threadPool, null );
	}

	private static long benchmark( final TileInfo[] tiles, final MultithreadedExecutor threadPool, final Integer slice ) throws InterruptedException, ExecutionException
	{
		final int startImage = Math.max( rnd.nextInt( tiles.length ) - testImagesCount, 0 );
		//final int startImage = 0;
		System.out.println( "Benchmarking " + ( slice != null ? "slice" : "full image" ) + " reader: images " + startImage + ".." + ( startImage + testImagesCount - 1 ) );

		final List< TileInfo > forOpening = new ArrayList<>();
		for ( int i = startImage; i < startImage + testImagesCount; ++i )
			forOpening.add( tiles[ i ] );
		forOpening.add( tiles[ startImage ] );

		long elapsedTotal = System.nanoTime();

		final AtomicInteger counter = new AtomicInteger( forOpening.size() * repeats );
		threadPool.run( ( idx, i ) ->
			{
				for ( int repeat = 0; repeat < repeats; ++repeat )
				{
					long elapsed = System.nanoTime();
					final int sliceToOpen = /*slice*/ rnd.nextInt( ( int ) tiles[ 0 ].getSize( 2 ) ) + 1;
					System.out.println( "  Opening " + Paths.get( forOpening.get( i ).getFilePath() + ( repeat == 0 ? "" : " - iter " + repeat ) )  + ( slice != null ? " at slice " + sliceToOpen : "" ) );
					ImagePlus imp;
					try {
						imp = slice != null ? TiffSliceReader.readSlice( forOpening.get( i ).getFilePath(), sliceToOpen ) : IJ.openImage( forOpening.get( i ).getFilePath() );
					} catch (final IOException e) {
						throw new RuntimeException( e );
					}
					imp.close();
					elapsed = System.nanoTime() - elapsed;
					System.out.println( ( ( forOpening.get( i ).equals( forOpening.get( 0 ) ) && i != 0 ) ? "****" : "" ) + "took " + elapsed/1e9 + "s, " + counter.decrementAndGet() + " images left" );
				}
				return 0;
			},
			forOpening.size() );

		elapsedTotal = System.nanoTime() - elapsedTotal;
		return elapsedTotal;
	}
//	private static long benchmark( final TileInfo[] tiles, final MultithreadedExecutor threadPool, final Integer slice ) throws InterruptedException, ExecutionException, IOException
//	{
//		final int startImage = Math.max( rnd.nextInt( tiles.length ) - testImagesCount, 0 );
//		System.out.println( "Benchmarking " + ( slice != null ? "slice" : "full image" ) + " reader: images " + startImage + ".." + ( startImage + testImagesCount - 1 ) + ( slice != null ? " at slice " + slice : "" ) );
//
//		final List< TileInfo > forOpening = new ArrayList<>();
//		for ( int i = startImage; i < startImage + testImagesCount; ++i )
//			forOpening.add( tiles[ i ] );
//
//		TileInfoJSONProvider.saveTilesConfiguration( forOpening.toArray( new TileInfo[ 0 ] ), slice != null ? "test1.txt" : "test2.txt" );
//		return 0;
//	}
}
