package org.janelia.stitching;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.flatfield.ShiftedDownsampling;
import org.janelia.histogram.Histogram;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.imglib2.Interval;
import net.imglib2.SerializableFinalInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.IntervalIndexer;
import scala.Tuple2;

public class DownsampleHistogramsTest
{
	private static final double EPSILON = 1e-10;

	private transient JavaSparkContext sparkContext;

	private final int histMinValue = 0, histMaxValue = 100;

	@Before
	public void setUp()
	{
		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local" )
				.setAppName( "TestDownsampleHistograms" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ) );
	}

	@After
	public void tearDown()
	{
		sparkContext.close();
	}

	@Test
	public void test()
	{
		final AffineTransform3D downsamplingTransform = new AffineTransform3D();
		downsamplingTransform.set(
				0.5, 0, 0, -0.5,
				0, 0.5, 0, -0.5,
				0, 0, 0.5, -0.5
			);

		final long[] dimensions = new long[] { 4, 3, 2 };
		final Interval interval = new SerializableFinalInterval( dimensions );

		final JavaPairRDD< Long, Histogram > rddHistograms = createHistograms( dimensions );

		final ShiftedDownsampling< AffineTransform3D > shiftedDownsampling = new ShiftedDownsampling<>( sparkContext, interval, downsamplingTransform );

		final int scale = 1;
		try ( ShiftedDownsampling< AffineTransform3D >.PixelsMapping pixelsMapping = shiftedDownsampling.new PixelsMapping( scale ) )
		{
			final long[] downsampledDimensions = new long[] { 3, 2, 2 };
			Assert.assertArrayEquals( downsampledDimensions, pixelsMapping.getDimensions() );

			final Map< Long, Histogram > downsampledHistograms = shiftedDownsampling.downsampleHistograms( rddHistograms, pixelsMapping ).collectAsMap();
			Assert.assertEquals( 12, downsampledHistograms.size() );

			Assert.assertArrayEquals( new double[] { 5, 0, 1 },       getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 0, 0, 0 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 2.5, 3, 0.5 },   getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 1, 0, 0 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 5, 1, 0 },       getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 2, 0, 0 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 1, 2.5, 2.5 },   getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 0, 1, 0 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 1.25, 2.75, 2 }, getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 1, 1, 0 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 2, 1, 3 },       getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 2, 1, 0 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 0, 2, 4 },       getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 0, 0, 1 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 2, 0.5, 3.5 },   getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 1, 0, 1 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 0, 4, 2 },       getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 2, 0, 1 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 0.5, 3.5, 2 },   getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 0, 1, 1 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 3, 2.5, 0.5 },   getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 1, 1, 1 }, downsampledDimensions ) ) ), EPSILON );
			Assert.assertArrayEquals( new double[] { 3.5, 1.5, 1 },   getHistogramArray( downsampledHistograms.get( IntervalIndexer.positionToIndex( new long[] { 2, 1, 1 }, downsampledDimensions ) ) ), EPSILON );
		}
	}

	private JavaPairRDD< Long, Histogram > createHistograms( final long[] dimensions  )
	{
		final List< Tuple2< long[], Histogram > > histograms = new ArrayList<>();
		histograms.add( new Tuple2<>( new long[] { 0, 0, 0 }, createHistogram( 5, 0, 1 ) ) );
		histograms.add( new Tuple2<>( new long[] { 1, 0, 0 }, createHistogram( 3, 2, 1 ) ) );
		histograms.add( new Tuple2<>( new long[] { 2, 0, 0 }, createHistogram( 2, 4, 0 ) ) );
		histograms.add( new Tuple2<>( new long[] { 3, 0, 0 }, createHistogram( 5, 1, 0 ) ) );
		histograms.add( new Tuple2<>( new long[] { 0, 1, 0 }, createHistogram( 2, 2, 2 ) ) );
		histograms.add( new Tuple2<>( new long[] { 1, 1, 0 }, createHistogram( 3, 1, 2 ) ) );
		histograms.add( new Tuple2<>( new long[] { 2, 1, 0 }, createHistogram( 1, 4, 1 ) ) );
		histograms.add( new Tuple2<>( new long[] { 3, 1, 0 }, createHistogram( 1, 1, 4 ) ) );
		histograms.add( new Tuple2<>( new long[] { 0, 2, 0 }, createHistogram( 0, 3, 3 ) ) );
		histograms.add( new Tuple2<>( new long[] { 1, 2, 0 }, createHistogram( 1, 0, 5 ) ) );
		histograms.add( new Tuple2<>( new long[] { 2, 2, 0 }, createHistogram( 0, 6, 0 ) ) );
		histograms.add( new Tuple2<>( new long[] { 3, 2, 0 }, createHistogram( 3, 1, 2 ) ) );
		histograms.add( new Tuple2<>( new long[] { 0, 0, 1 }, createHistogram( 0, 2, 4 ) ) );
		histograms.add( new Tuple2<>( new long[] { 1, 0, 1 }, createHistogram( 4, 1, 1 ) ) );
		histograms.add( new Tuple2<>( new long[] { 2, 0, 1 }, createHistogram( 0, 0, 6 ) ) );
		histograms.add( new Tuple2<>( new long[] { 3, 0, 1 }, createHistogram( 0, 4, 2 ) ) );
		histograms.add( new Tuple2<>( new long[] { 0, 1, 1 }, createHistogram( 1, 2, 3 ) ) );
		histograms.add( new Tuple2<>( new long[] { 1, 1, 1 }, createHistogram( 3, 1, 2 ) ) );
		histograms.add( new Tuple2<>( new long[] { 2, 1, 1 }, createHistogram( 4, 2, 0 ) ) );
		histograms.add( new Tuple2<>( new long[] { 3, 1, 1 }, createHistogram( 1, 3, 2 ) ) );
		histograms.add( new Tuple2<>( new long[] { 0, 2, 1 }, createHistogram( 0, 5, 1 ) ) );
		histograms.add( new Tuple2<>( new long[] { 1, 2, 1 }, createHistogram( 1, 5, 0 ) ) );
		histograms.add( new Tuple2<>( new long[] { 2, 2, 1 }, createHistogram( 4, 2, 0 ) ) );
		histograms.add( new Tuple2<>( new long[] { 3, 2, 1 }, createHistogram( 6, 0, 0 ) ) );
		return sparkContext.parallelizePairs( histograms ).mapToPair( tuple -> new Tuple2<>( IntervalIndexer.positionToIndex( tuple._1(), dimensions ), tuple._2() ) );
	}

	private Histogram createHistogram( final int... binElements )
	{
		final Histogram histogram = new Histogram( histMinValue, histMaxValue, binElements.length );
		for ( int i = 0; i < binElements.length; ++i )
			histogram.put( histogram.getBinValue( i ), binElements[ i ] );
		return histogram;
	}

	private double[] getHistogramArray( final Histogram histogram )
	{
		final double[] array = new double[ histogram.getNumBins() ];
		for ( int i = 0; i < array.length; ++i )
			array[ i ] = histogram.get( i );
		return array;
	}
}
