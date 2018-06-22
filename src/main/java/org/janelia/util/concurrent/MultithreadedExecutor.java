package org.janelia.util.concurrent;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;

public class MultithreadedExecutor implements AutoCloseable
{
	private final ExecutorService threadPool;
	private final int numThreads;

	public MultithreadedExecutor()
	{
		// reserve one thread for the OS
		this( Runtime.getRuntime().availableProcessors() - 1 );
	}

	public MultithreadedExecutor( final int numThreads )
	{
		this( Executors.newFixedThreadPool( numThreads ), numThreads );
	}

	public MultithreadedExecutor( final ExecutorService threadPool, final int numThreads )
	{
		this.threadPool = threadPool;
		this.numThreads = numThreads;
	}

	@Override
	public void close()
	{
		threadPool.shutdown();
	}

	public ExecutorService getThreadPool()
	{
		return threadPool;
	}

	public int getNumThreads()
	{
		return numThreads;
	}


	public void run( final IntConsumer func, final int totalSize ) throws InterruptedException, ExecutionException
	{
		run( ( thread, i ) -> { func.accept( i ); return i; }, totalSize );
	}


	public double sumReal( final IntToDoubleFunction func, final int totalSize ) throws InterruptedException, ExecutionException
	{
		final double[] partialSums = new double[ numThreads ];

		run( ( thread, i ) -> { partialSums[ thread ] += func.applyAsDouble( i ); return 0; }, totalSize );

		double sum = 0;
		for ( final double partialSum : partialSums )
			sum += partialSum;
		return sum;
	}

	public long sum( final IntToLongFunction func, final int totalSize ) throws InterruptedException, ExecutionException
	{
		final long[] partialSums = new long[ numThreads ];

		run( ( thread, i ) -> { partialSums[ thread ] += func.applyAsLong( i ); return 0; }, totalSize );

		long sum = 0;
		for ( final long partialSum : partialSums )
			sum += partialSum;
		return sum;
	}


	public double minReal( final IntToDoubleFunction func, final int totalSize ) throws InterruptedException, ExecutionException
	{
		final double[] partialMins = new double[ numThreads ];
		Arrays.fill( partialMins, Double.POSITIVE_INFINITY );

		run( ( thread, i ) -> { partialMins[ thread ] = Math.min( func.applyAsDouble( i ), partialMins[ thread ] ); return 0; }, totalSize );

		double min = Double.POSITIVE_INFINITY;
		for ( final double partialMin : partialMins )
			min = Math.min( partialMin, min );
		return min;
	}

	public long min( final IntToLongFunction func, final int totalSize ) throws InterruptedException, ExecutionException
	{
		final long[] partialMins = new long[ numThreads ];
		Arrays.fill( partialMins, Long.MAX_VALUE );

		run( ( thread, i ) -> { partialMins[ thread ] = Math.min( func.applyAsLong( i ), partialMins[ thread ] ); return 0; }, totalSize );

		long min = Long.MAX_VALUE;
		for ( final long partialMin : partialMins )
			min = Math.min( partialMin, min );
		return min;
	}

	public double maxReal( final IntToDoubleFunction func, final int totalSize ) throws InterruptedException, ExecutionException
	{
		final double[] partialMaxs = new double[ numThreads ];
		Arrays.fill( partialMaxs, Double.NEGATIVE_INFINITY );

		run( ( thread, i ) -> { partialMaxs[ thread ] = Math.max( func.applyAsDouble( i ), partialMaxs[ thread ] ); return 0; }, totalSize );

		double max = Double.NEGATIVE_INFINITY;
		for ( final double partialMax : partialMaxs )
			max = Math.max( partialMax, max );
		return max;
	}

	public long max( final IntToLongFunction func, final int totalSize ) throws InterruptedException, ExecutionException
	{
		final long[] partialMaxs = new long[ numThreads ];
		Arrays.fill( partialMaxs, Long.MIN_VALUE );

		run( ( thread, i ) -> { partialMaxs[ thread ] = Math.max( func.applyAsLong( i ), partialMaxs[ thread ] ); return 0; }, totalSize );

		long max = Long.MIN_VALUE;
		for ( final long partialMax : partialMaxs )
			max = Math.max( partialMax, max );
		return max;
	}


	public void run( final IntBinaryOperator func, final int totalSize ) throws InterruptedException, ExecutionException
	{
		final AtomicInteger ai = new AtomicInteger();
		final Future< ? >[] futures = new Future[ numThreads ];

		for ( int ithread = 0; ithread < numThreads; ++ithread )
			futures[ ithread ] = threadPool.submit( () ->
			{
				final int myNumber = ai.getAndIncrement();
				for ( int i = myNumber; i < totalSize; i += numThreads )
					func.applyAsInt( myNumber, i );
			});

		for ( final Future< ? > future : futures )
			future.get();
	}
}
