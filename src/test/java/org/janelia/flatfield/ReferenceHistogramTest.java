package org.janelia.flatfield;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

public class ReferenceHistogramTest
{
	private static final double REFERENCE_HISTOGRAM_POINTS_PERCENT = 0.5;

	private JavaSparkContext sparkContext;

	@Before
	public void setUp()
	{
		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local" )
				.setAppName( "TestReferenceHistogram" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ) );
	}

	@After
	public void tearDown()
	{
		sparkContext.close();
	}


	@Test
	public void testEvenEven()
	{
		final HistogramSettings histogramSettings = new HistogramSettings( 0, 99, 5 );
		final List< long[] > histograms = new ArrayList<>();
		histograms.add( new long[] { 1, 4, 2, 0, 1 } );
		histograms.add( new long[] { 2, 2, 2, 2, 0 } );
		histograms.add( new long[] { 3, 1, 0, 1, 3 } );
		histograms.add( new long[] { 2, 5, 1, 0, 0 } );

		final long[] accumulatedHistogram = accumulateHistograms( histograms, histogramSettings );
		Assert.assertArrayEquals( new long[] { 3, 6, 4, 2, 1 }, accumulatedHistogram );
	}

	@Test
	public void testEvenOdd()
	{
		final HistogramSettings histogramSettings = new HistogramSettings( 0, 99, 5 );
		final List< long[] > histograms = new ArrayList<>();
		histograms.add( new long[] { 1, 4, 2, 0, 1 } );
		histograms.add( new long[] { 2, 2, 2, 2, 0 } );
		histograms.add( new long[] { 3, 1, 0, 1, 3 } );
		histograms.add( new long[] { 2, 5, 1, 0, 0 } );
		histograms.add( new long[] { 1, 3, 3, 1, 0 } );
		histograms.add( new long[] { 0, 1, 6, 1, 0 } );

		final long[] accumulatedHistogram = accumulateHistograms( histograms, histogramSettings );
		Assert.assertArrayEquals( new long[] { 4, 9, 7, 3, 1 }, accumulatedHistogram );
	}

	@Test
	public void testOddEven()
	{
		final HistogramSettings histogramSettings = new HistogramSettings( 0, 99, 5 );
		final List< long[] > histograms = new ArrayList<>();
		histograms.add( new long[] { 1, 4, 2, 0, 1 } );
		histograms.add( new long[] { 2, 2, 2, 2, 0 } );
		histograms.add( new long[] { 3, 1, 0, 1, 3 } );
		histograms.add( new long[] { 2, 5, 1, 0, 0 } );
		histograms.add( new long[] { 1, 3, 3, 1, 0 } );
		histograms.add( new long[] { 0, 1, 6, 1, 0 } );
		histograms.add( new long[] { 1, 2, 1, 2, 2 } );

		final long[] accumulatedHistogram = accumulateHistograms( histograms, histogramSettings );
		Assert.assertArrayEquals( new long[] { 6, 7, 11, 5, 3 }, accumulatedHistogram );
	}

	@Test
	public void testOddOdd()
	{
		final HistogramSettings histogramSettings = new HistogramSettings( 0, 99, 5 );
		final List< long[] > histograms = new ArrayList<>();
		histograms.add( new long[] { 1, 4, 2, 0, 1 } );
		histograms.add( new long[] { 2, 2, 2, 2, 0 } );
		histograms.add( new long[] { 3, 1, 0, 1, 3 } );
		histograms.add( new long[] { 2, 5, 1, 0, 0 } );
		histograms.add( new long[] { 0, 1, 6, 1, 0 } );

		final long[] accumulatedHistogram = accumulateHistograms( histograms, histogramSettings );
		Assert.assertArrayEquals( new long[] { 6, 7, 4, 3, 4 }, accumulatedHistogram );
	}


	private long[] accumulateHistograms( final List< long[] > histograms, final HistogramSettings histogramSettings )
	{
		final JavaPairRDD< Long, long[] > rddHistograms = sparkContext.parallelize( histograms ).zipWithIndex().mapToPair( tuple -> tuple.swap() ).cache();
		final Tuple2< long[], Long > ret = HistogramsProvider.accumulateHistograms( rddHistograms, histogramSettings, REFERENCE_HISTOGRAM_POINTS_PERCENT );
		rddHistograms.unpersist();
		return ret._1();
	}
}
