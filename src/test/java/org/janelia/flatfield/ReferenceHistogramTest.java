package org.janelia.flatfield;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.histogram.Histogram;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReferenceHistogramTest
{
	private static final double REFERENCE_HISTOGRAM_POINTS_PERCENT = 0.5;
	private static final double EPSILON = 1e-10;

	private transient JavaSparkContext sparkContext;

	private final int histMinValue = 0, histMaxValue = 100;

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
		final List< Histogram > histograms = new ArrayList<>();
		histograms.add( createHistogram( new int[] { 1, 4, 2, 0, 1 } ) );
		histograms.add( createHistogram( new int[] { 2, 2, 2, 2, 0 } ) );
		histograms.add( createHistogram( new int[] { 3, 1, 0, 1, 3 } ) );
		histograms.add( createHistogram( new int[] { 2, 5, 1, 0, 0 } ) );

		final Histogram accumulatedHistogram = accumulateHistograms( histograms );
		Assert.assertArrayEquals( new double[] { 1.5, 3, 2, 1, 0.5 }, getHistogramArray( accumulatedHistogram ), EPSILON );
	}

	@Test
	public void testEvenOdd()
	{
		final List< Histogram > histograms = new ArrayList<>();
		histograms.add( createHistogram( new int[] { 1, 4, 2, 0, 1 } ) );
		histograms.add( createHistogram( new int[] { 2, 2, 2, 2, 0 } ) );
		histograms.add( createHistogram( new int[] { 3, 1, 0, 1, 3 } ) );
		histograms.add( createHistogram( new int[] { 2, 5, 1, 0, 0 } ) );
		histograms.add( createHistogram( new int[] { 1, 3, 3, 1, 0 } ) );
		histograms.add( createHistogram( new int[] { 0, 1, 6, 1, 0 } ) );

		final Histogram accumulatedHistogram = accumulateHistograms( histograms );
		Assert.assertArrayEquals( new double[] { 1.3333333333, 3, 2.3333333333, 1, 0.3333333333 }, getHistogramArray( accumulatedHistogram ), EPSILON );
	}

	@Test
	public void testOddEven()
	{
		final List< Histogram > histograms = new ArrayList<>();
		histograms.add( createHistogram( new int[] { 1, 4, 2, 0, 1 } ) );
		histograms.add( createHistogram( new int[] { 2, 2, 2, 2, 0 } ) );
		histograms.add( createHistogram( new int[] { 3, 1, 0, 1, 3 } ) );
		histograms.add( createHistogram( new int[] { 2, 5, 1, 0, 0 } ) );
		histograms.add( createHistogram( new int[] { 1, 3, 3, 1, 0 } ) );
		histograms.add( createHistogram( new int[] { 0, 1, 6, 1, 0 } ) );
		histograms.add( createHistogram( new int[] { 1, 2, 1, 2, 2 } ) );

		final Histogram accumulatedHistogram = accumulateHistograms( histograms );
		Assert.assertArrayEquals( new double[] { 1.5, 1.75, 2.75, 1.25, 0.75 }, getHistogramArray( accumulatedHistogram ), EPSILON );
	}

	@Test
	public void testOddOdd()
	{
		final List< Histogram > histograms = new ArrayList<>();
		histograms.add( createHistogram( new int[] { 1, 4, 2, 0, 1 } ) );
		histograms.add( createHistogram( new int[] { 2, 2, 2, 2, 0 } ) );
		histograms.add( createHistogram( new int[] { 3, 1, 0, 1, 3 } ) );
		histograms.add( createHistogram( new int[] { 2, 5, 1, 0, 0 } ) );
		histograms.add( createHistogram( new int[] { 0, 1, 6, 1, 0 } ) );

		final Histogram accumulatedHistogram = accumulateHistograms( histograms );
		Assert.assertArrayEquals( new double[] { 2, 2.3333333333, 1.3333333333, 1, 1.3333333333 }, getHistogramArray( accumulatedHistogram ), EPSILON );
	}

	private Histogram createHistogram( final int... binElements )
	{
		final Histogram histogram = new Histogram( histMinValue, histMaxValue, binElements.length );
		for ( int i = 0; i < binElements.length; ++i )
			histogram.set( i, binElements[ i ] );
		return histogram;
	}

	private Histogram accumulateHistograms( final List< Histogram > histograms )
	{
		final JavaPairRDD< Long, Histogram > rddHistograms = sparkContext.parallelize( histograms ).zipWithIndex().mapToPair( tuple -> tuple.swap() ).cache();
		final Histogram ret = HistogramsProvider.estimateReferenceHistogram( rddHistograms, REFERENCE_HISTOGRAM_POINTS_PERCENT );
		rddHistograms.unpersist();
		return ret;
	}

	private double[] getHistogramArray( final Histogram histogram )
	{
		final double[] array = new double[ histogram.getNumBins() ];
		for ( int i = 0; i < array.length; ++i )
			array[ i ] = histogram.get( i );
		return array;
	}
}
