package org.janelia.flatfield;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.stitching.ImageType;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.Utils;
import org.janelia.util.Conversions;
import org.janelia.util.ImageImporter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.histogram.Real1dBinMapper;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.CompositeView;
import net.imglib2.view.composite.RealComposite;

public class ShiftedDownsamplingTest
{
	private static final String histogramsN5BasePath = System.getProperty( "user.home" ) + "/tmp/n5-downsample-histogram-test";
	private static final String histogramsDataset = "/test/group/dataset";
	private static final double EPSILON = 1e-10;

	private transient JavaSparkContext sparkContext;

	private static final double histMinValue = 0, histMaxValue = 100;
	private static final int bins = 5;

	private static final long[] dimensions = new long[] { 4, 3, 2 };
	private static final int[] blockSize = new int[] { 2, 2, 1 };

	@Before
	public void setUp()
	{
		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( "TestDownsampleHistograms" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ) );
	}

	@After
	public void tearDown() throws IOException
	{
		sparkContext.close();
		cleanup();
	}

	private static void cleanup() throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		Assert.assertTrue( n5.remove() );
	}

	@Test
	public void test() throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final double[][] histograms = new double[ ( int ) Intervals.numElements( dimensions ) ][];
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

		final long[] extendedDimensions = new long[ dimensions.length + 1 ];
		System.arraycopy( dimensions, 0, extendedDimensions, 0, dimensions.length );
		extendedDimensions[ dimensions.length ] = bins;

		final int[] extendedBlockSize = new int[ blockSize.length + 1 ];
		System.arraycopy( blockSize, 0, extendedBlockSize, 0, blockSize.length );
		extendedBlockSize[ blockSize.length ] = bins;

		// save histograms first
		final double[] histogramsHelperArray = new double[ histograms.length * bins ];
		int helperArrayIndex = 0;
		for ( int bin = 0; bin < bins; ++bin )
			for ( int i = 0; i < histograms.length; ++i )
				histogramsHelperArray[ helperArrayIndex++ ] = histograms[ i ][ bin ];
		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		final RandomAccessibleInterval< DoubleType > source = ArrayImgs.doubles( histogramsHelperArray, extendedDimensions );
		N5Utils.save( source, n5, histogramsDataset, extendedBlockSize, new GzipCompression() );

		// test downsampling with offset
		final ShiftedDownsampling< AffineTransform3D > shiftedDownsampling = new ShiftedDownsampling<>(
				sparkContext,
				dataProvider.getType(),
				histogramsN5BasePath,
				histogramsDataset,
				new FinalInterval( dimensions )
			);

		Assert.assertEquals( 2, shiftedDownsampling.getNumScales() );
		final String downsampledHistogramsDataset = shiftedDownsampling.getDatasetAtScale( 1 );
		Assert.assertTrue( n5.datasetExists( downsampledHistogramsDataset ) );
		Assert.assertArrayEquals( new long[] { 2, 2, 1 }, shiftedDownsampling.getDimensionsAtScale( 1 ) );

		// read downsampled histograms
		final RandomAccessibleInterval< DoubleType > downsampledHistogramsStorageImg = N5Utils.open( n5, downsampledHistogramsDataset );
		final CompositeIntervalView< DoubleType, RealComposite< DoubleType > > downsampledHistogramsImg = Views.collapseReal( downsampledHistogramsStorageImg );
		Assert.assertArrayEquals( new long[] { 2, 2, 1 }, Intervals.dimensionsAsLongArray( downsampledHistogramsImg ) );

		Assert.assertArrayEquals( new double[] { 0, 5, 0, 1, 0 },       getHistogramArray( downsampledHistogramsImg, new long[] { 0, 0, 0 } ), EPSILON );
		Assert.assertArrayEquals( new double[] { 0, 2.5, 3, 0.5, 0 },   getHistogramArray( downsampledHistogramsImg, new long[] { 1, 0, 0 } ), EPSILON );
		Assert.assertArrayEquals( new double[] { 0, 1, 2.5, 2.5, 0 },   getHistogramArray( downsampledHistogramsImg, new long[] { 0, 1, 0 } ), EPSILON );
		Assert.assertArrayEquals( new double[] { 0, 1.25, 2.75, 2, 0 }, getHistogramArray( downsampledHistogramsImg, new long[] { 1, 1, 0 } ), EPSILON );

		// test downsampling image
		final RandomAccessibleInterval< DoubleType > img = ArrayImgs.doubles(
				Conversions.toDoubleArray( LongStream.rangeClosed( 1, Intervals.numElements( dimensions ) ).toArray() ),
				dimensions
			);
		final RandomAccessibleInterval< DoubleType > downsampledImg = shiftedDownsampling.downsampleImage( img, 1 );
		Assert.assertArrayEquals( new long[] { 2, 2, 1 }, Intervals.dimensionsAsLongArray( downsampledImg ) );
		Assert.assertArrayEquals( new long[ dimensions.length ], Intervals.minAsLongArray( downsampledImg ) );
		Assert.assertArrayEquals( new double[] { 1, ( 2 + 3 ) / 2., ( 5 + 9 ) / 2., ( 6 + 7 + 10 + 11 ) / 4. }, getArrayFromRandomAccessibleInterval( downsampledImg ), EPSILON );

		// TODO: test upsampling image

		// cleanup
		shiftedDownsampling.cleanupDownsampledHistograms();
		Assert.assertFalse( n5.datasetExists( downsampledHistogramsDataset ) );
	}

	private double[] getHistogramArray( final CompositeIntervalView< DoubleType, RealComposite< DoubleType > > downsampledHistogramsImg, final long[] position )
	{
		final double[] histogram = new double[ bins ];
		final CompositeView< DoubleType, RealComposite< DoubleType > >.CompositeRandomAccess randomAccess = downsampledHistogramsImg.randomAccess();
		randomAccess.setPosition( position );
		final RealComposite< DoubleType > composite = randomAccess.get();
		for ( int bin = 0; bin < bins; ++bin )
			histogram[ bin ] = composite.get( bin ).get();
		return histogram;
	}

	private double[] createHistogram( final int... binElements )
	{
		Assert.assertEquals( bins - 2, binElements.length );
		final double[] histogram = new double[ bins ];
		final Real1dBinMapper< DoubleType > binMapper = new Real1dBinMapper<>( histMinValue, histMaxValue, bins, true );
		final double[] binValues = HistogramMatching.getBinValues( histMinValue, histMaxValue, bins );
		for ( int bin = 1; bin < bins - 1; ++bin )
			histogram[ ( int ) binMapper.map( new DoubleType( binValues[ bin ] ) ) ] = binElements[ bin - 1 ];
		return histogram;
	}

	private double[] getArrayFromRandomAccessibleInterval( final RandomAccessibleInterval< DoubleType > rai )
	{
		final double[] arr = new double[ ( int ) Intervals.numElements( rai ) ];
		final Cursor< DoubleType > cursor = Views.flatIterable( rai ).cursor();
		int i = 0;
		while ( cursor.hasNext() )
			arr[ i++ ] = cursor.next().get();
		return arr;
	}

	@SuppressWarnings( "unchecked" )
	public static < T extends NativeType< T > & RealType< T > > void main( final String[] args ) throws IOException, URISyntaxException
	{
		cleanup();
		final String filepath = args[ 0 ];

		final ImagePlus imp = ImageImporter.openImage( filepath ), restoredStackImp;
		final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( "TestDownsampleHistograms" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ) ) )
		{
			final TileInfo tile = new TileInfo( img.numDimensions() );
			tile.setFilePath( filepath );
			tile.setIndex( 0 );
			tile.setSize( Intervals.dimensionsAsLongArray( img ) );
			tile.setType( ImageType.valueOf( imp.getType() ) );

			final HistogramsProvider histogramsProvider = new HistogramsProvider(
					sparkContext,
					DataProviderFactory.createFSDataProvider(),
					img,
					histogramsN5BasePath,
					new TileInfo[] { tile },
					tile.getSize().clone(),
					histMinValue, histMaxValue, bins
				);

			final ShiftedDownsampling< ? > downsampler = new ShiftedDownsampling<>( sparkContext, histogramsProvider );

			final int numScales = downsampler.getNumScales();
			final List< long[] > downsampledDimensions = new ArrayList<>( numScales );
			for ( int scale = 0; scale < numScales; ++scale )
				downsampledDimensions.add( downsampler.getDimensionsAtScale( scale ) );

			final int restoredScale = 0;
			final List< RandomAccessibleInterval< T > > restoredImgs = new ArrayList<>();

			for ( int lowScale = 0; lowScale < downsampler.getNumScales(); ++lowScale )
			{
				System.out.println( "Processing scale " + lowScale );
				final RandomAccessibleInterval< T > downsampledImg = downsampler.downsampleImage( img, lowScale );
				final RandomAccessible< T > upsampledImg = downsampler.upsampleImage( downsampledImg, restoredScale );
				final RandomAccessibleInterval< T > restoredImg = Views.interval( upsampledImg, new FinalInterval( downsampler.getDimensionsAtScale( restoredScale ) ) );
				restoredImgs.add( restoredImg );
			}

			final RandomAccessibleInterval< T > restoredImgsStack = Views.stack( restoredImgs );
			restoredStackImp = ImageJFunctions.wrap( restoredImgsStack, "" );
			Utils.workaroundImagePlusNSlices( restoredStackImp );

			cleanup();
		}

		new ImageJ();
		restoredStackImp.show();
	}
}
