package org.janelia.flatfield;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.histogram.Histogram;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.imglib2.img.list.ListLocalizingCursor;
import net.imglib2.img.list.WrappedListImg;

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
				.setMaster( "local" )
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


	@Test
	public void testEvenEven() throws IOException
	{
		final List< Histogram > histograms = new ArrayList<>();
		histograms.add( createHistogram( new int[] { 1, 4, 2, 0, 1 } ) );
		histograms.add( createHistogram( new int[] { 2, 2, 2, 2, 0 } ) );
		histograms.add( createHistogram( new int[] { 3, 1, 0, 1, 3 } ) );
		histograms.add( createHistogram( new int[] { 2, 5, 1, 0, 0 } ) );

		final Histogram accumulatedHistogram = getReferenceHistogram( histograms );
		Assert.assertArrayEquals( new double[] { 1.5, 3, 2, 1, 0.5 }, getHistogramArray( accumulatedHistogram ), EPSILON );
	}

	@Test
	public void testEvenOdd() throws IOException
	{
		final List< Histogram > histograms = new ArrayList<>();
		histograms.add( createHistogram( new int[] { 1, 4, 2, 0, 1 } ) );
		histograms.add( createHistogram( new int[] { 2, 2, 2, 2, 0 } ) );
		histograms.add( createHistogram( new int[] { 3, 1, 0, 1, 3 } ) );
		histograms.add( createHistogram( new int[] { 2, 5, 1, 0, 0 } ) );
		histograms.add( createHistogram( new int[] { 1, 3, 3, 1, 0 } ) );
		histograms.add( createHistogram( new int[] { 0, 1, 6, 1, 0 } ) );

		final Histogram accumulatedHistogram = getReferenceHistogram( histograms );
		Assert.assertArrayEquals( new double[] { 1.3333333333, 3, 2.3333333333, 1, 0.3333333333 }, getHistogramArray( accumulatedHistogram ), EPSILON );
	}

	@Test
	public void testOddEven() throws IOException
	{
		final List< Histogram > histograms = new ArrayList<>();
		histograms.add( createHistogram( new int[] { 1, 4, 2, 0, 1 } ) );
		histograms.add( createHistogram( new int[] { 2, 2, 2, 2, 0 } ) );
		histograms.add( createHistogram( new int[] { 3, 1, 0, 1, 3 } ) );
		histograms.add( createHistogram( new int[] { 2, 5, 1, 0, 0 } ) );
		histograms.add( createHistogram( new int[] { 1, 3, 3, 1, 0 } ) );
		histograms.add( createHistogram( new int[] { 0, 1, 6, 1, 0 } ) );
		histograms.add( createHistogram( new int[] { 1, 2, 1, 2, 2 } ) );

		final Histogram accumulatedHistogram = getReferenceHistogram( histograms );
		Assert.assertArrayEquals( new double[] { 1.5, 1.75, 2.75, 1.25, 0.75 }, getHistogramArray( accumulatedHistogram ), EPSILON );
	}

	@Test
	public void testOddOdd() throws IOException
	{
		final List< Histogram > histograms = new ArrayList<>();
		histograms.add( createHistogram( new int[] { 1, 4, 2, 0, 1 } ) );
		histograms.add( createHistogram( new int[] { 2, 2, 2, 2, 0 } ) );
		histograms.add( createHistogram( new int[] { 3, 1, 0, 1, 3 } ) );
		histograms.add( createHistogram( new int[] { 2, 5, 1, 0, 0 } ) );
		histograms.add( createHistogram( new int[] { 0, 1, 6, 1, 0 } ) );

		final Histogram accumulatedHistogram = getReferenceHistogram( histograms );
		Assert.assertArrayEquals( new double[] { 2, 2.3333333333, 1.3333333333, 1, 1.3333333333 }, getHistogramArray( accumulatedHistogram ), EPSILON );
	}

	private Histogram createHistogram( final int... binElements )
	{
		final Histogram histogram = new Histogram( histMinValue, histMaxValue, binElements.length );
		for ( int i = 0; i < binElements.length; ++i )
			histogram.put( histogram.getBinValue( i ), binElements[ i ] );
		return histogram;
	}

	private Histogram getReferenceHistogram( final List< Histogram > histograms ) throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final DataAccessType dataAccessType = DataAccessType.FILESYSTEM;

		final long[] dimensions = new long[] { 1, 1, histograms.size() };
		final int[] blockSize = new int[] { 1, 1, histograms.size() / 2 };

		// save histograms first
		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		n5.createDataset(
				histogramsDataset,
				dimensions,
				blockSize,
				DataType.SERIALIZABLE,
				new GzipCompression()
			);
		for ( int i = 0; i < dimensions[ dimensions.length - 1 ]; i += blockSize[ blockSize.length - 1 ] )
		{
			final long[] blockPosition = new long[ blockSize.length ];
			blockPosition[ blockPosition.length - 1 ] = i / blockSize[ blockSize.length - 1 ];
			final WrappedSerializableDataBlockWriter< Histogram > histogramsBlock = new WrappedSerializableDataBlockWriter<>( n5, histogramsDataset, blockPosition );
			Assert.assertFalse( histogramsBlock.wasLoadedSuccessfully() );

			final WrappedListImg< Histogram > histogramsBlockImg = histogramsBlock.wrap();
			final ListLocalizingCursor< Histogram > histogramsBlockImgCursor = histogramsBlockImg.localizingCursor();
			while ( histogramsBlockImgCursor.hasNext() )
			{
				histogramsBlockImgCursor.fwd();
				final int pos = histogramsBlockImgCursor.getIntPosition( histogramsBlockImg.numDimensions() - 1 );
				histogramsBlockImgCursor.set( histograms.get( i + pos ) );
			}
			histogramsBlock.save();
		}

		final Histogram referenceHistogram = HistogramsProvider.estimateReferenceHistogram(
				sparkContext,
				dataProvider, dataAccessType,
				histogramsN5BasePath, histogramsDataset,
				dimensions, blockSize,
				REFERENCE_HISTOGRAM_POINTS_PERCENT,
				histMinValue, histMaxValue, histograms.get( 0 ).getNumBins()
			);

		Assert.assertTrue( n5.remove() );
		return referenceHistogram;
	}

	private double[] getHistogramArray( final Histogram histogram )
	{
		final double[] array = new double[ histogram.getNumBins() ];
		for ( int i = 0; i < array.length; ++i )
			array[ i ] = histogram.get( i );
		return array;
	}
}
