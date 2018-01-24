package org.janelia.flatfield;

import java.io.IOException;
import java.net.URI;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.histogram.Histogram;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.SerializableFinalInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.list.ListCursor;
import net.imglib2.img.list.WrappedListImg;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class DownsampleHistogramsTest
{
	private static final String histogramsN5BasePath = System.getProperty("user.home") + "/tmp/n5-reference-histogram-test";
	private static final String histogramsDataset = "/test/group/dataset";
	private static final double EPSILON = 1e-10;

	private transient JavaSparkContext sparkContext;

	private final int histMinValue = 0, histMaxValue = 100, bins = 3;

	private final long[] dimensions = new long[] { 4, 3, 2 };
	private final int[] blockSize = new int[] { 2, 2, 1 };

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
	public void test() throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final Histogram[] histograms = new Histogram[ ( int ) Intervals.numElements( dimensions ) ];
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 0 }, dimensions ) ] = createHistogram( 5, 0, 1 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 0, 0 }, dimensions ) ] = createHistogram( 3, 2, 1 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 0, 0 }, dimensions ) ] = createHistogram( 2, 4, 0 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 3, 0, 0 }, dimensions ) ] = createHistogram( 5, 1, 0 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 1, 0 }, dimensions ) ] = createHistogram( 2, 2, 2 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 1, 0 }, dimensions ) ] = createHistogram( 3, 1, 2 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 1, 0 }, dimensions ) ] = createHistogram( 1, 4, 1 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 3, 1, 0 }, dimensions ) ] = createHistogram( 1, 1, 4 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 2, 0 }, dimensions ) ] = createHistogram( 0, 3, 3 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 2, 0 }, dimensions ) ] = createHistogram( 1, 0, 5 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 2, 0 }, dimensions ) ] = createHistogram( 0, 6, 0 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 3, 2, 0 }, dimensions ) ] = createHistogram( 3, 1, 2 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 1 }, dimensions ) ] = createHistogram( 0, 2, 4 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 0, 1 }, dimensions ) ] = createHistogram( 4, 1, 1 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 0, 1 }, dimensions ) ] = createHistogram( 0, 0, 6 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 3, 0, 1 }, dimensions ) ] = createHistogram( 0, 4, 2 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 1, 1 }, dimensions ) ] = createHistogram( 1, 2, 3 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 1, 1 }, dimensions ) ] = createHistogram( 3, 1, 2 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 1, 1 }, dimensions ) ] = createHistogram( 4, 2, 0 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 3, 1, 1 }, dimensions ) ] = createHistogram( 1, 3, 2 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 2, 1 }, dimensions ) ] = createHistogram( 0, 5, 1 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 2, 1 }, dimensions ) ] = createHistogram( 1, 5, 0 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 2, 1 }, dimensions ) ] = createHistogram( 4, 2, 0 );
		histograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 3, 2, 1 }, dimensions ) ] = createHistogram( 6, 0, 0 );

		// save histograms first
		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		n5.createDataset(
				histogramsDataset,
				dimensions,
				blockSize,
				DataType.SERIALIZABLE,
				new GzipCompression()
			);
		final CellGrid cellGrid = new CellGrid( dimensions, blockSize );
		final long[] cellGridDimensions = cellGrid.getGridDimensions();
		final long numCells = Intervals.numElements( cellGridDimensions );
		final long[] cellMin = new long[ cellGrid.numDimensions() ], cellMax = new long[ cellGrid.numDimensions() ];
		final int[] cellDimensions = new int[ cellGrid.numDimensions() ];
		final long[] cellGridPosition = new long[ cellGrid.numDimensions() ], position = new long[ cellGrid.numDimensions() ];
		for ( long cellIndex = 0; cellIndex < numCells; ++cellIndex )
		{
			cellGrid.getCellGridPositionFlat( cellIndex, cellGridPosition );
			cellGrid.getCellDimensions( cellIndex, cellMin, cellDimensions );
			for ( int d = 0; d < cellGrid.numDimensions(); ++d )
				cellMax[ d ] = cellMin[ d ] + cellDimensions[ d ] - 1;
			final Interval cellInterval = new FinalInterval( cellMin, cellMax );

			final WrappedSerializableDataBlockWriter< Histogram > histogramsBlock = new WrappedSerializableDataBlockWriter<>(
					n5,
					histogramsDataset,
					cellGridPosition
				);
			final WrappedListImg< Histogram > histogramsBlockImg = histogramsBlock.wrap();

			// initialize all histograms
			final ListCursor< Histogram > histogramsBlockImgCursor = histogramsBlockImg.cursor();
			while ( histogramsBlockImgCursor.hasNext() )
			{
				histogramsBlockImgCursor.fwd();
				histogramsBlockImgCursor.set( new Histogram( histMinValue, histMaxValue, bins ) );
			}

			final IntervalView< Histogram > translatedHistogramsBlockImg = Views.translate( histogramsBlockImg, cellMin );
			final RandomAccess< Histogram > translatedHistogramsBlockImgRandomAccess = translatedHistogramsBlockImg.randomAccess();

			final IntervalIterator cellIntervalIterator = new IntervalIterator( cellInterval );
			while ( cellIntervalIterator.hasNext() )
			{
				cellIntervalIterator.fwd();
				cellIntervalIterator.localize( position );
				translatedHistogramsBlockImgRandomAccess.setPosition( position );
				final long pixelIndex = IntervalIndexer.positionToIndex( position, dimensions );
				translatedHistogramsBlockImgRandomAccess.get().add( histograms[ ( int ) pixelIndex ] );
			}

			histogramsBlock.save();
		}

		final Interval interval = new SerializableFinalInterval( dimensions );
		final AffineTransform3D downsamplingTransform = new AffineTransform3D();
		downsamplingTransform.set(
				0.5, 0, 0, -0.5,
				0, 0.5, 0, -0.5,
				0, 0, 0.5, -0.5
			);
		final ShiftedDownsampling< AffineTransform3D > shiftedDownsampling = new ShiftedDownsampling<>( sparkContext, interval, downsamplingTransform );

		final int scale = 1;
		try ( ShiftedDownsampling< AffineTransform3D >.PixelsMapping pixelsMapping = shiftedDownsampling.new PixelsMapping( scale ) )
		{
			final long[] downsampledDimensions = new long[] { 3, 2, 2 };
			Assert.assertArrayEquals( downsampledDimensions, pixelsMapping.getDimensions() );

			final String downsampledHistogramsDataset = shiftedDownsampling.downsampleHistogramsN5(
					pixelsMapping,
					dataProvider,
					histogramsN5BasePath, histogramsDataset,
					histMinValue, histMaxValue, bins
				);

			Assert.assertTrue( n5.datasetExists( downsampledHistogramsDataset ) );

			final DatasetAttributes downsampledHistogramsDatasetAttributes = n5.getDatasetAttributes( downsampledHistogramsDataset );
			final int downsampledHistogramsCount = ( int ) Intervals.numElements( downsampledHistogramsDatasetAttributes.getDimensions() );
			Assert.assertEquals( 12, downsampledHistogramsCount );

			// read downsampled histograms
			final Histogram[] downsampledHistograms = new Histogram[ downsampledHistogramsCount ];
			final CellGrid downsampledCellGrid = new CellGrid( downsampledDimensions, blockSize );
			final long[] downsampledCellGridDimensions = downsampledCellGrid.getGridDimensions();
			final long downsampledNumCells = Intervals.numElements( downsampledCellGridDimensions );
			final long[] downsampledCellMin = new long[ downsampledCellGrid.numDimensions() ], downsampledCellMax = new long[ downsampledCellGrid.numDimensions() ];
			final int[] downsampledCellDimensions = new int[ downsampledCellGrid.numDimensions() ];
			final long[] downsampledCellGridPosition = new long[ downsampledCellGrid.numDimensions() ], downsampledPosition = new long[ downsampledCellGrid.numDimensions() ];
			for ( long downsampledCellIndex = 0; downsampledCellIndex < downsampledNumCells; ++downsampledCellIndex )
			{
				downsampledCellGrid.getCellGridPositionFlat( downsampledCellIndex, downsampledCellGridPosition );
				downsampledCellGrid.getCellDimensions( downsampledCellIndex, downsampledCellMin, downsampledCellDimensions );
				for ( int d = 0; d < downsampledCellGrid.numDimensions(); ++d )
					downsampledCellMax[ d ] = downsampledCellMin[ d ] + downsampledCellDimensions[ d ] - 1;
				final Interval downsampledCellInterval = new FinalInterval( downsampledCellMin, downsampledCellMax );

				final WrappedSerializableDataBlockReader< Histogram > downsampledHistogramsBlock = new WrappedSerializableDataBlockReader<>(
						n5,
						downsampledHistogramsDataset,
						downsampledCellGridPosition
					);
				final WrappedListImg< Histogram > downsampledHistogramsBlockImg = downsampledHistogramsBlock.wrap();

				final IntervalView< Histogram > translatedDownsampledHistogramsBlockImg = Views.translate( downsampledHistogramsBlockImg, downsampledCellMin );
				final RandomAccess< Histogram > translatedDownsampledHistogramsBlockImgRandomAccess = translatedDownsampledHistogramsBlockImg.randomAccess();

				final IntervalIterator downsampledCellIntervalIterator = new IntervalIterator( downsampledCellInterval );
				while ( downsampledCellIntervalIterator.hasNext() )
				{
					downsampledCellIntervalIterator.fwd();
					downsampledCellIntervalIterator.localize( downsampledPosition );
					translatedDownsampledHistogramsBlockImgRandomAccess.setPosition( downsampledPosition );
					final long downsampledPixelIndex = IntervalIndexer.positionToIndex( downsampledPosition, downsampledDimensions );
					downsampledHistograms[ ( int ) downsampledPixelIndex ] = translatedDownsampledHistogramsBlockImgRandomAccess.get();
				}
			}

			Assert.assertArrayEquals( new double[] { 5, 0, 1 },       getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 0 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 2.5, 3, 0.5 },   getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 0, 0 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 5, 1, 0 },       getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 0, 0 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 1, 2.5, 2.5 },   getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 1, 0 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 1.25, 2.75, 2 }, getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 1, 0 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 2, 1, 3 },       getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 1, 0 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 0, 2, 4 },       getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 1 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 2, 0.5, 3.5 },   getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 0, 1 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 0, 4, 2 },       getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 0, 1 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 0.5, 3.5, 2 },   getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 1, 1 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 3, 2.5, 0.5 },   getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 1, 1 }, downsampledDimensions ) ] ), EPSILON );
			Assert.assertArrayEquals( new double[] { 3.5, 1.5, 1 },   getHistogramArray( downsampledHistograms[ ( int ) IntervalIndexer.positionToIndex( new long[] { 2, 1, 1 }, downsampledDimensions ) ] ), EPSILON );
		}
	}

	private Histogram createHistogram( final int... binElements )
	{
		Assert.assertEquals( bins, binElements.length );
		final Histogram histogram = new Histogram( histMinValue, histMaxValue, bins );
		for ( int i = 0; i < bins; ++i )
			histogram.put( histogram.getBinValue( i ), binElements[ i ] );
		return histogram;
	}

	private double[] getHistogramArray( final Histogram histogram )
	{
		Assert.assertEquals( bins, histogram.getNumBins() );
		final double[] array = new double[ bins ];
		for ( int i = 0; i < bins; ++i )
			array[ i ] = histogram.get( i );
		return array;
	}
}
