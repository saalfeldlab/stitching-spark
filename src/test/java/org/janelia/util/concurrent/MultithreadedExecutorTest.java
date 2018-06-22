package org.janelia.util.concurrent;

import java.util.Random;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MultithreadedExecutorTest
{
	private Random rnd;
	private int numThreads;
	private MultithreadedExecutor multithreadedExecutor;

	@Before
	public void setUp()
	{
		rnd = new Random();
		numThreads = rnd.nextInt( Runtime.getRuntime().availableProcessors() * 2 ) + 1;
		multithreadedExecutor = new MultithreadedExecutor( Executors.newFixedThreadPool( numThreads ), numThreads );
	}

	@Test
	public void testSum() throws Exception
	{
		for ( int t = 0; t < 10; t++ )
		{
			final int totalSteps = rnd.nextInt( 100000 );
			final int totalCount = (int) multithreadedExecutor.sum( i -> 1, totalSteps );
			Assert.assertEquals( totalSteps, totalCount );

			final int totalCountReal = (int) multithreadedExecutor.sumReal( i -> 1, totalSteps );
			Assert.assertEquals( totalSteps, totalCountReal );
		}
	}

	@Test
	public void testRun() throws Exception
	{
		for ( int t = 0; t < 10; t++ )
		{
			final int totalSteps = rnd.nextInt( 100000 ) + 1;
			final int[] arr = new int[ totalSteps ];

			multithreadedExecutor.run( i -> arr[ i ]++, totalSteps );

			for ( int i = 0; i < totalSteps; i++ )
				Assert.assertEquals( 1, arr[ i ] );
		}
	}

	@After
	public void tearDown() throws Exception
	{
		multithreadedExecutor.close();
	}
}
