package org.janelia.flatfield;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.real.DoubleType;

public class ReferenceHistogramTest
{
	static private final String histogramsN5BasePath = System.getProperty("user.home") + "/tmp/n5-reference-histogram-test";
	static private final String histogramsDataset = "/test/group/dataset";

	private static final double REFERENCE_HISTOGRAM_POINTS_PERCENT = 0.5;
	private static final double EPSILON = 1e-10;

	private transient JavaSparkContext sparkContext;

	private final int histMinValue = 0, histMaxValue = 100;

	@Before
	public void setUp()
	{
		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( "ReferenceHistogramTest" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ) );
	}

	@After
	public void tearDown() throws IOException
	{
		sparkContext.close();

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		n5.remove();
	}


	/**
	 * Test reference histogram where
	 * the number of histograms is even, and
	 * the number of filtered histograms is even.
	 * @throws IOException
	 */
	@Test
	public void testEvenEven() throws IOException
	{
		final List< double[] > histograms = new ArrayList<>();
		histograms.add( addTailBins( 1, 4, 2, 0, 1 ) );
		histograms.add( addTailBins( 2, 2, 2, 2, 0 ) );
		histograms.add( addTailBins( 3, 1, 0, 1, 3 ) );
		histograms.add( addTailBins( 2, 5, 1, 0, 0 ) );

		final double[] accumulatedHistogram = getReferenceHistogram( histograms );
		Assert.assertArrayEquals( addTailBins( 1.5, 3, 2, 1, 0.5 ), accumulatedHistogram, EPSILON );
	}

	/**
	 * Test reference histogram where
	 * the number of histograms is even, and
	 * the number of filtered histograms is odd.
	 * @throws IOException
	 */
	@Test
	public void testEvenOdd() throws IOException
	{
		final List< double[] > histograms = new ArrayList<>();
		histograms.add( addTailBins( 1, 4, 2, 0, 1 ) );
		histograms.add( addTailBins( 2, 2, 2, 2, 0 ) );
		histograms.add( addTailBins( 3, 1, 0, 1, 3 ) );
		histograms.add( addTailBins( 2, 5, 1, 0, 0 ) );
		histograms.add( addTailBins( 1, 3, 3, 1, 0 ) );
		histograms.add( addTailBins( 0, 1, 6, 1, 0 ) );

		final double[] accumulatedHistogram = getReferenceHistogram( histograms );
		Assert.assertArrayEquals( addTailBins( 1.3333333333, 3, 2.3333333333, 1, 0.3333333333 ), accumulatedHistogram, EPSILON );
	}

	/**
	 * Test reference histogram where
	 * the number of histograms is odd, and
	 * the number of filtered histograms is even.
	 * @throws IOException
	 */
	@Test
	public void testOddEven() throws IOException
	{
		final List< double[] > histograms = new ArrayList<>();
		histograms.add( addTailBins( 1, 4, 2, 0, 1 ) );
		histograms.add( addTailBins( 2, 2, 2, 2, 0 ) );
		histograms.add( addTailBins( 3, 1, 0, 1, 3 ) );
		histograms.add( addTailBins( 2, 5, 1, 0, 0 ) );
		histograms.add( addTailBins( 1, 3, 3, 1, 0 ) );
		histograms.add( addTailBins( 0, 1, 6, 1, 0 ) );
		histograms.add( addTailBins( 1, 2, 1, 2, 2 ) );

		final double[] accumulatedHistogram = getReferenceHistogram( histograms );
		Assert.assertArrayEquals( addTailBins( 1.5, 1.75, 2.75, 1.25, 0.75 ), accumulatedHistogram, EPSILON );
	}

	/**
	 * Test reference histogram where
	 * the number of histograms is odd, and
	 * the number of filtered histograms is odd.
	 * @throws IOException
	 */
	@Test
	public void testOddOdd() throws IOException
	{
		final List< double[] > histograms = new ArrayList<>();
		histograms.add( addTailBins( 1, 4, 2, 0, 1 ) );
		histograms.add( addTailBins( 2, 2, 2, 2, 0 ) );
		histograms.add( addTailBins( 3, 1, 0, 1, 3 ) );
		histograms.add( addTailBins( 2, 5, 1, 0, 0 ) );
		histograms.add( addTailBins( 0, 1, 6, 1, 0 ) );

		final double[] accumulatedHistogram = getReferenceHistogram( histograms );
		Assert.assertArrayEquals( addTailBins( 2, 2.3333333333, 1.3333333333, 1, 1.3333333333 ), accumulatedHistogram, EPSILON );
	}

	/**
	 * Test reference histogram with tail bins having non-zero values.
	 * @throws IOException
	 */
	@Test
	public void testNonZeroTailBins() throws IOException
	{
		final List< double[] > histograms = new ArrayList<>();
		histograms.add( new double[] { 6, 1, 4, 2, 0, 1, 4 } );
		histograms.add( new double[] { 3, 2, 2, 2, 2, 0, 7 } );
		histograms.add( new double[] { 1, 3, 1, 0, 1, 3, 9 } );
		histograms.add( new double[] { 8, 2, 5, 1, 0, 0, 2 } );

		final double[] accumulatedHistogram = getReferenceHistogram( histograms );
		Assert.assertArrayEquals( new double[] { 4.5, 1.5, 3, 2, 1, 0.5, 5.5 }, accumulatedHistogram, EPSILON );
	}

	private double[] getReferenceHistogram( final List< double[] > histograms ) throws IOException
	{
		final int bins = histograms.get( 0 ).length;
		for ( final double[] histogram : histograms )
			assert histogram.length == bins;

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final DataAccessType dataAccessType = DataAccessType.FILESYSTEM;

		final long[] dimensions = new long[] { 1, 1, histograms.size() };
		final int[] blockSize = new int[] { 1, 1, histograms.size() / 2 };

		final long[] extendedDimensions = new long[ dimensions.length + 1 ];
		System.arraycopy( dimensions, 0, extendedDimensions, 0, dimensions.length );
		extendedDimensions[ dimensions.length ] = bins;

		final int[] extendedBlockSize = new int[ blockSize.length + 1 ];
		System.arraycopy( blockSize, 0, extendedBlockSize, 0, blockSize.length );
		extendedBlockSize[ blockSize.length ] = bins;

		// save histograms first
		final double[] histogramsHelperArray = new double[ histograms.size() * bins ];
		int helperArrayIndex = 0;
		for ( int bin = 0; bin < bins; ++bin )
			for ( int i = 0; i < histograms.size(); ++i )
				histogramsHelperArray[ helperArrayIndex++ ] = histograms.get( i )[ bin ];

		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		final RandomAccessibleInterval< DoubleType > source = ArrayImgs.doubles( histogramsHelperArray, extendedDimensions );
		N5Utils.save( source, n5, histogramsDataset, extendedBlockSize, new GzipCompression() );

		final double[] referenceHistogram = HistogramsProvider.estimateReferenceHistogram(
				sparkContext,
				dataProvider, dataAccessType,
				histogramsN5BasePath, histogramsDataset,
				dimensions, blockSize,
				REFERENCE_HISTOGRAM_POINTS_PERCENT,
				histMinValue, histMaxValue, bins
			);

		Assert.assertTrue( n5.remove() );
		return referenceHistogram;
	}

	private double[] addTailBins( final double... histogram )
	{
		final double[] histogramWithTailBins = new double[ histogram.length + 2 ];
		System.arraycopy( histogram, 0, histogramWithTailBins, 1, histogram.length );
		return histogramWithTailBins;
	}
}
