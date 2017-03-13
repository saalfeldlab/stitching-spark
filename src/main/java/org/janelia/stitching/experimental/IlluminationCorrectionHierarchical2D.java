package org.janelia.stitching.experimental;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.stitching.TiffSliceLoader;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.janelia.util.DirectAccessListImg;
import org.janelia.util.concurrent.MultithreadedExecutor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import bdv.export.Downsample;
import ij.IJ;
import ij.ImagePlus;
import mpicbg.models.Affine1D;
import mpicbg.models.AffineModel1D;
import mpicbg.models.ConstantAffineModel1D;
import mpicbg.models.FixedScalingAffineModel1D;
import mpicbg.models.FixedTranslationAffineModel1D;
import mpicbg.models.IdentityModel;
import mpicbg.models.InterpolatedAffineModel1D;
import mpicbg.models.InvertibleBoundable;
import mpicbg.models.Model;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Scale;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.ComparablePair;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.RealSum;
import net.imglib2.util.ValuePair;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.RandomAccessibleOnRealRandomAccessible;
import net.imglib2.view.Views;


public class IlluminationCorrectionHierarchical2D implements Serializable
{
	private static final long serialVersionUID = -8987192045944606043L;

	private static final double WINDOW_POINTS_PERCENT = 0.25;
	private static final double INTERPOLATION_LAMBDA = 0.9;

	private enum ModelType
	{
		AffineModel,
		FixedScalingAffineModel,
		FixedTranslationAffineModel
	}
	private enum RegularizerModelType
	{
		AffineModel,
		IdentityModel
	}

	private final String subfolder = "hierarchical";

	private final String inputFilepath, histogramsPath, solutionPath;
	private transient JavaSparkContext sparkContext;
	private transient TileInfo[] tiles;
	private transient MultithreadedExecutor multithreadedExecutor;
	private long[] fullSize;

	public static void main( final String[] args ) throws Exception
	{
		final IlluminationCorrectionHierarchical2D driver = new IlluminationCorrectionHierarchical2D( args[ 0 ] );
		driver.run();
		driver.shutdown();
		System.out.println("Done");
	}


	public IlluminationCorrectionHierarchical2D( final String inputFilepath )
	{
		this.inputFilepath = inputFilepath;

		final String outputPath = Paths.get( inputFilepath ).getParent().toString() + "/" + subfolder;
		histogramsPath = outputPath + "/" + "histograms";
		solutionPath   = outputPath + "/" + "solution";
	}

	public void shutdown()
	{
		if ( sparkContext != null )
			sparkContext.close();
		if ( multithreadedExecutor != null )
			multithreadedExecutor.close();
	}


	public void run() throws Exception
	{
		try {
			tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilepath );
		} catch (final IOException e) {
			e.printStackTrace();
			return;
		}

		fullSize = getMinSize(tiles);

		// check if all tiles have the same size
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				if ( tile.getSize(d) != fullSize[ d ] )
				{
					System.out.println("Assumption failed: not all the tiles are of the same size");
					System.exit(1);
				}


		final Map< Integer, Map< String, double[] > > solutions = new TreeMap<>();
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
		{
			final Map< String, double[] > s = loadSolution( slice );
			if ( s != null )
				solutions.put(slice, s);
		}
		if ( !solutions.isEmpty() )
		{
			System.out.println( "Successfully loaded a solution for "+solutions.size()+" slices" );
			for ( final Entry<Integer, Map<String, double[]>> entry : solutions.entrySet() )
			{
				System.out.println( "Correcting slice " + entry.getKey() );
				correctImages(entry.getKey(), entry.getValue().get("v"), entry.getValue().get("z"));
			}
			return;
		}


		/*if ( !allHistogramsReady() )
		{
			populateHistograms();
			System.out.println( "Computed all histograms" );
			return;
		}*/


		// TODO: consider replacing TreeMaps with histograms having fixed number of bins
		final int N = tiles.length;
		System.out.println( "Working with stack of size " + N );

		System.out.println( "Output directory: " + solutionPath );

		multithreadedExecutor = new MultithreadedExecutor();

		//approximateAdditiveNoise( 0, readSliceHistogramsFromDisk( 0, getNumSlices() ) );




		if ( tiles.length > 0 )
		{
			outputMeanStdBlockwiseTest();
			return;
		}



		long elapsed = System.nanoTime();


//		final double[] referenceVector = estimateReferenceVector( readSliceHistogramsFromDisk( 0, getNumSlices() ) );
//
//		// Generate solutions iteratively from the smallest scale to the full size
//		final Pair< ArrayImg< DoubleType, DoubleArray >, ArrayImg< DoubleType, DoubleArray > >[] solution = new Pair[ getNumSlices() ];
//
//		for ( int scale = getNumScales() - 1; scale >= 0; scale-- )
//		{
//			for ( int slice = 1; slice <= getNumSlices(); slice++ )
//			{
//				final TreeMap< Short, Integer >[] histograms = readSliceHistogramsFromDisk( scale, slice );
//				solution[ slice - 1 ] = leastSquaresInterpolationFit( scale, histograms, referenceVector, solution[ slice - 1 ] );
//				saveSolution( solution[ slice - 1 ], scale, slice );
//			}
//		}


		final List< DirectAccessListImg< TreeMap< Short, Integer > > > scaleHistograms = new ArrayList<>();
		scaleHistograms.add( readSliceHistogramsFromDisk( 0, getNumSlices() ) );
		while ( Views.dropSingletonDimensions( scaleHistograms.get( scaleHistograms.size() - 1 ) ).numDimensions() == scaleHistograms.get( scaleHistograms.size() - 1 ).numDimensions() )
		{
			final int scale = scaleHistograms.size();

			scaleHistograms.add(
					downscaleHistograms(
							scaleHistograms.get( 0 ),
							scale ) );
		}

		final double[] referenceVector = estimateReferenceVector( scaleHistograms.get( 0 ) );

		// Generate solutions iteratively from the smallest scale to the full size
		final Pair< ArrayImg< DoubleType, DoubleArray >, ArrayImg< DoubleType, DoubleArray > >[] solution = new Pair[ getNumSlices() ];

		for ( int scale = scaleHistograms.size() - 1; scale >= 0; scale-- )
		{
			for ( int slice = 1; slice <= getNumSlices(); slice++ )
			{
				solution[ slice - 1 ] = leastSquaresInterpolationFit( scale, scaleHistograms.get( scale ), referenceVector, solution[ slice - 1 ] );
				saveSolution( solution[ slice - 1 ], scale, slice );
			}
		}

		final RandomAccessibleInterval< DoubleType > correctedStack = generateCorrectionOutput( solution[ 0 ] );

		outputMeanStdBlockwise( correctedStack, null );

		elapsed = System.nanoTime() - elapsed;
		System.out.println( "----------" );
		System.out.println( String.format( "Took %f mins", elapsed / 1e9 / 60 ) );
	}



	private < T extends NativeType< T > & RealType< T > > void outputMeanStdBlockwiseTest() throws Exception
	{
		final Img< T >[] sliceImgs = new Img[ tiles.length ];
		System.out.println( "Loading slices.." );
		multithreadedExecutor.run( i -> sliceImgs[ i ] = ImagePlusImgs.from( IJ.openImage( tiles[ i ].getFilePath() ) ), tiles.length );
		outputMeanStdBlockwise( Views.stack( sliceImgs ), "-original" );


		System.out.println( "Correcting stack........" );
		final Img< FloatType > vImg = ImagePlusImgs.from( IJ.openImage( "/groups/saalfeld/home/pisarevi/data/illumination-corr/hierarchical/latest/shifted-v.tif" ) );
		final Img< FloatType > zImg = ImagePlusImgs.from( IJ.openImage( "/groups/saalfeld/home/pisarevi/data/illumination-corr/hierarchical/latest/shifted-z.tif" ) );

		final Img< DoubleType >[] sliceImgsCorrected = new Img[ tiles.length ];
		multithreadedExecutor.run( i ->
		{
			final Cursor< T > imgCursor = Views.flatIterable( sliceImgs[ i ] ).cursor();

			final Cursor< FloatType > vCursor = Views.flatIterable( vImg ).cursor();
			final Cursor< FloatType > zCursor = Views.flatIterable( zImg ).cursor();

			sliceImgsCorrected[ i ] = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( sliceImgs[ i ] ) );
			final Cursor< DoubleType > correctedImgCursor = Views.flatIterable( sliceImgsCorrected[ i ] ).cursor();

			while ( correctedImgCursor.hasNext() || imgCursor.hasNext() || vCursor.hasNext() || zCursor.hasNext() )
				correctedImgCursor.next().setReal( imgCursor.next().getRealDouble() * vCursor.next().getRealDouble() + zCursor.next().getRealDouble() );
		},
		tiles.length );

		outputMeanStdBlockwise( Views.stack( sliceImgsCorrected ), "-corrected" );
	}

	private < T extends NativeType< T > & RealType< T > > void outputMeanStdBlockwise( final RandomAccessibleInterval< T > stack, final String str ) throws Exception
	{
		//final long[] blockCount = new long[] { 2, 2, 84 };
		final long[] blockCount = new long[] { fullSize[0], fullSize[1], 1 };
		final long[] blockSize = new long[ blockCount.length ];
		int blockCountTotal = 1;
		for ( int d = 0; d < blockCount.length; d++ )
		{
			blockSize[ d ] = stack.dimension( d ) / blockCount[ d ];
			blockCountTotal *= blockCount[ d ];
		}
		System.out.println( "Block size is " + Arrays.toString(blockSize));

		final Pair< Double, Double >[] blocks = new Pair[ blockCountTotal ];
		System.out.println( "Processing blocks.." );
		multithreadedExecutor.run( blockIndex ->
			{
				final long[] blockPosition = new long[ blockSize.length ];
				IntervalIndexer.indexToPosition( blockIndex, blockCount, blockPosition );
				for ( int d = 0; d < blockPosition.length; d++ )
					blockPosition[ d ] *= blockSize[ d ];

				final Interval blockInterval = Intervals.createMinSize( Utils.concatArrays( blockPosition, blockSize ) );
				final RandomAccessibleInterval< T > blockImg = Views.interval( stack, blockInterval );
				final IterableInterval< T > blockIterable = Views.iterable( blockImg );
				final Cursor< T > blockCursor = blockIterable.cursor();

				final RealSum sum = new RealSum(), sum2 = new RealSum();
				while ( blockCursor.hasNext() )
				{
					final double val = blockCursor.next().getRealDouble();
					sum.add( val );
					sum2.add( val * val );
				}

				final double mean = sum.getSum() / blockIterable.size();
				final double std = Math.sqrt( sum2.getSum() / blockIterable.size() - mean * mean );
				blocks[ blockIndex ] = new ValuePair<>( mean, std );
			},
			blocks.length );

		System.out.println( "Outputting stats.." );
		try ( final PrintWriter writer = new PrintWriter( solutionPath + "/" + "blocks_pixel"+str+".txt" ) )
		{
			for ( final Pair< Double, Double > block : blocks )
				writer.println( block.getA() + " " + block.getB() );
		}
	}






	private < T extends NativeType< T > & RealType< T > > boolean produceCompressedOutput() throws Exception
	{
		System.out.println( "Allocating huge input space" );
		final RandomAccessibleInterval< UnsignedShortType > stack = new CellImgFactory< UnsignedShortType >().create( new long[] { fullSize[ 0 ], fullSize[ 1 ], tiles.length }, new UnsignedShortType() );

		System.out.println( "Loading slices.." );
		final AtomicInteger remaining = new AtomicInteger( tiles.length );
		multithreadedExecutor.run( slice ->
			{
				final ImagePlus imp = IJ.openImage( tiles[ slice ].getFilePath() );
				Utils.workaroundImagePlusNSlices( imp );
				final Img< T > img = ImagePlusImgs.from( imp );
				final RandomAccessibleInterval< T > imgTranslated = Views.translate( img, new long[] { 0, 0, slice } );
				final Cursor< T > source = Views.flatIterable( imgTranslated ).cursor();

				final RandomAccessibleInterval< UnsignedShortType > stackImgInterval = Views.interval( stack, imgTranslated );
				final Cursor< UnsignedShortType > target = Views.flatIterable( stackImgInterval ).cursor();

				while ( target.hasNext() || source.hasNext() )
					target.next().setReal( source.next().getRealDouble() );

				System.out.println( remaining.decrementAndGet() + " slices remaining" );
			},
			tiles.length );

		final RandomAccessibleInterval< UnsignedShortType > stackRotated = Views.rotate( stack, 1, 2 );

		System.out.println( "Allocating output space" );
		final RandomAccessibleInterval< UnsignedShortType > compressedOutput = ArrayImgs.unsignedShorts( new long[] { stackRotated.dimension( 0 ), stackRotated.dimension( 0 ), 1 } );

		final int[] factors = getDownsampleFactors( stackRotated, compressedOutput );
		System.out.println( String.format( "Downsampling from %s to %s with factors=%",
				Arrays.toString( Intervals.dimensionsAsIntArray( stackRotated ) ),
				Arrays.toString( Intervals.dimensionsAsIntArray( compressedOutput ) ),
				Arrays.toString( factors ) ) );

		Downsample.downsample( stackRotated, compressedOutput, factors );

		final ImagePlus impOut = ImageJFunctions.wrap( compressedOutput, null );
    	Utils.workaroundImagePlusNSlices( impOut );

    	IJ.saveAsTiff( impOut, solutionPath + "/" + "example_output.tif" );

		return true;
	}



	private int[] getDownsampleFactors( final RandomAccessibleInterval< ? > src, final RandomAccessibleInterval< ? > dst )
	{
		if ( src.numDimensions() != dst.numDimensions() )
			return null;

		final int[] factors = new int[ src.numDimensions() ];
		for ( int d = 0; d < factors.length; d++ )
			factors[ d ] = ( int ) ( src.dimension( d ) / dst.dimension( d ) );

		return factors;
	}



	private < A extends ArrayImg< DoubleType, DoubleArray >, T extends NativeType< T > & RealType< T > > RandomAccessibleInterval< DoubleType > generateCorrectionOutput( final Pair< A, A > solution ) throws Exception
	{
		final Img< DoubleType >[] correctedTiles = new Img[ tiles.length ];
		final AtomicInteger remaining = new AtomicInteger( tiles.length );
		System.out.println( "--- Correcting images ---" );
		multithreadedExecutor.run( i ->
			{
				final ImagePlus imp = IJ.openImage( tiles[ i ].getFilePath() );
				Utils.workaroundImagePlusNSlices( imp );
				final Img< T > img = ImagePlusImgs.from( imp );
				final Cursor< T > imgCursor = Views.flatIterable( img ).cursor();

				final Cursor< DoubleType > vCursor = Views.flatIterable( solution.getA() ).cursor();
				final Cursor< DoubleType > zCursor = Views.flatIterable( solution.getB() ).cursor();

				correctedTiles[ i ] = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( img ) );
				final Cursor< DoubleType > correctedImgCursor = Views.flatIterable( correctedTiles[ i ] ).cursor();

				while ( correctedImgCursor.hasNext() || imgCursor.hasNext() || vCursor.hasNext() || zCursor.hasNext() )
					correctedImgCursor.next().setReal( imgCursor.next().getRealDouble() * vCursor.next().get() + zCursor.next().get() );

				imp.close();

				final int remainingVal = remaining.decrementAndGet();
				if ( remainingVal % 1000 == 0 )
				System.out.println( remainingVal + " remaining.." );
			},
			tiles.length );

		return Views.stack( correctedTiles );
	}






	private < T extends TreeMap< Short, Integer > > DirectAccessListImg< T > downscaleHistograms( final DirectAccessListImg< T > originalHistograms, final int scale ) throws Exception
	{
		return downscaleHistograms( originalHistograms, scale, tiles.length, multithreadedExecutor );
	}
	public static < T extends TreeMap< Short, Integer > > DirectAccessListImg< T > downscaleHistograms(
			final DirectAccessListImg< T > originalHistograms,
			final int scale,
			final int entriesCount,
			final MultithreadedExecutor multithreadedExecutor ) throws Exception
	{
		final long[]
				pixelSize 		= new long[ originalHistograms.numDimensions() ],
				offset 			= new long[ originalHistograms.numDimensions() ],
				downscaledSize 	= new long[ originalHistograms.numDimensions() ];
		for ( int d = 0; d < downscaledSize.length; d++ )
		{
			pixelSize[ d ] = 1 << scale;
			offset[ d ] = Math.min( pixelSize[ d ] >> 1, originalHistograms.dimension( d ) );
			final long remaining = originalHistograms.dimension( d ) - offset[ d ];
			downscaledSize[ d ] = 1 + remaining / pixelSize[ d ] + ( remaining % pixelSize[ d ] == 0 ? 0 : 1 );
		}

		final DirectAccessListImg< T > downscaledHistograms = new DirectAccessListImg<>( downscaledSize, null );

		System.out.println( "Downsampling histograms from " + Arrays.toString( Intervals.dimensionsAsLongArray( originalHistograms ) ) + " to " + Arrays.toString( Intervals.dimensionsAsLongArray( downscaledHistograms ) ) );

		multithreadedExecutor.run( downscaledPixel ->
		//for ( int downscaledPixel = 0; downscaledPixel < downscaledHistograms.size(); downscaledPixel++ )
			{
				final int[] downscaledPosition = new int[ downscaledHistograms.numDimensions() ];
				IntervalIndexer.indexToPosition( downscaledPixel, downscaledSize, downscaledPosition );

				final long[] mins = new long[ originalHistograms.numDimensions() ], maxs = new long[ originalHistograms.numDimensions() ];
				for ( int d = 0; d < mins.length; d++ )
				{
					if ( downscaledPosition[ d ] == 0 )
					{
						mins[ d ] = 0;
						maxs[ d ] = offset[ d ] - 1;
					}
					else
					{
						mins[ d ] = offset[ d ] + pixelSize[ d ] * ( downscaledPosition[ d ] - 1 );
						maxs[ d ] = Math.min( mins[ d ] + pixelSize[ d ] - 1, originalHistograms.max( d ) );
					}
				}
				final IntervalView< T > neighborhoodInterval = Views.interval( originalHistograms, new FinalInterval( mins, maxs ) );
				final Cursor< T > neighborhoodCursor = neighborhoodInterval.cursor();

				int neighbors = 0;
				final long[] histogramsSum = new long[ entriesCount ];

				while ( neighborhoodCursor.hasNext() )
				{
					final T histogram = neighborhoodCursor.next();
					int counter = 0;
					for ( final Entry< Short, Integer > entry : histogram.entrySet() )
						for ( int j = 0; j < entry.getValue(); j++ )
							histogramsSum[ counter++ ] += entry.getKey();

					neighbors++;
				}

				final T downscaledHistogram = ( T ) new TreeMap< Short, Integer >();
				for ( int i = 0; i < histogramsSum.length; i++ )
				{
					final short key = ( short ) Math.round( ( double ) histogramsSum[ i ] / neighbors );
					downscaledHistogram.put( key, downscaledHistogram.getOrDefault( key, 0 ) + 1 );
				}
				downscaledHistograms.set( downscaledPixel, downscaledHistogram );
			},
			( int ) downscaledHistograms.size() );

		return downscaledHistograms;
	}


	private void approximateAdditiveNoise( final int scale, final TreeMap< Short, Integer >[] histograms ) throws Exception
	{
		final short threshold = ( short ) multithreadedExecutor.max( pixel -> histograms[ pixel ].firstKey(), histograms.length );
		System.out.println( "Threshold = " + threshold );

		final long[] workingSize = getSizeAtScale( scale );

		final double[] zApproximated = new double[ histograms.length ];
		multithreadedExecutor.run( pixel ->
			{
				for ( final Entry< Short, Integer > entry : histograms[ pixel ].headMap( threshold, true ).entrySet() )
					zApproximated[ pixel ] += entry.getKey() * entry.getValue();
				zApproximated[ pixel ] /= tiles.length;
			},
			histograms.length );

		final ArrayImg< DoubleType, DoubleArray > zApproximatedImg = ArrayImgs.doubles( zApproximated, workingSize );
		final ImagePlus imp = ImageJFunctions.wrap( zApproximatedImg, "z-approx" );
		Utils.workaroundImagePlusNSlices( imp );

		final String outPath = solutionPath + "/" + imp.getTitle() + ".tif";
		IJ.saveAsTiff( imp, outPath );
		System.out.println( "Z approximation saved to " + outPath );

		removeAdditiveNoise( zApproximatedImg );
	}

	private void removeAdditiveNoise( final Img< DoubleType > additiveNoiseImg ) throws Exception
	{
		final String outPath = solutionPath + "/subtract_noise";
		new File( outPath ).mkdirs();

		multithreadedExecutor.run( i ->
			{
				final ImagePlus imp = IJ.openImage( tiles[ i ].getFilePath() );
				Utils.workaroundImagePlusNSlices( imp );
				final Img< ? extends RealType > img = ImagePlusImgs.from( imp );
				final Cursor< ? extends RealType > imgCursor = Views.flatIterable( img ).cursor();

				final Cursor< DoubleType > additiveNoiseCursor = Views.flatIterable( additiveNoiseImg ).cursor();

				final ArrayImg< DoubleType, DoubleArray > correctedImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( img ) );
				final Cursor< DoubleType > correctedImgCursor = Views.flatIterable( correctedImg ).cursor();

				while ( correctedImgCursor.hasNext() || imgCursor.hasNext() )
					correctedImgCursor.next().setReal( imgCursor.next().getRealDouble() - additiveNoiseCursor.next().getRealDouble() );

				final ImagePlus correctedImp = ImageJFunctions.wrap( correctedImg, "" );
				Utils.workaroundImagePlusNSlices( correctedImp );
				IJ.saveAsTiff( correctedImp, outPath + "/"+ Utils.addFilenameSuffix( Paths.get(tiles[i].getFilePath()).getFileName().toString(), "_noise-subtracted" ) );

				imp.close();
				correctedImp.close();
			}, tiles.length );
	}


	private void saveSolution( final Pair< ArrayImg< DoubleType, DoubleArray >, ArrayImg< DoubleType, DoubleArray > > solution, final int scale, final int slice )
	{
		final Map< String, Img< DoubleType > > solutionComponents = new HashMap<>();
		solutionComponents.put( "v", solution.getA() );
		solutionComponents.put( "z", solution.getB() );

		for ( final Entry< String, Img< DoubleType > > solutionComponent : solutionComponents.entrySet() )
		{
			final String path = solutionPath + "/" + scale + "/" + solutionComponent.getKey().toLowerCase() + "/" + slice + ".tif";

			new File( Paths.get( path ).getParent().toString() ).mkdirs();

			final ImagePlus imp = ImageJFunctions.wrap( solutionComponent.getValue(), solutionComponent.getKey() + "_scale" + scale );
			Utils.workaroundImagePlusNSlices( imp );
			IJ.saveAsTiff( imp, path );
		}
	}

	private double[] estimateReferenceVector( final DirectAccessListImg< TreeMap< Short, Integer > > histograms ) throws Exception
	{
		final List< ComparablePair< Double, Integer > > sortedMeans = new ArrayList<>( ( int ) histograms.size() );

		multithreadedExecutor.run( pixel ->
			{
				double pixelMean = 0;
				int count = 0;
				for ( final Entry< Short, Integer > entry : histograms.get( pixel ).entrySet() )
				{
					pixelMean += entry.getKey() * entry.getValue();
					count += entry.getValue();
				}
				pixelMean /= count;

				synchronized ( sortedMeans )
				{
					sortedMeans.add( new ComparablePair<>( pixelMean, pixel ) );
				}
			},
			( int ) histograms.size() );

		Collections.sort( sortedMeans );

		final int numWindowPoints = ( int ) Math.round( histograms.size() * WINDOW_POINTS_PERCENT );
		final int mStart = ( int ) ( Math.round( histograms.size() / 2.0 ) - Math.round( numWindowPoints / 2.0 ) );
		final int mEnd = mStart + numWindowPoints;
		System.out.println("Estimating reference vector using mStart="+mStart+", mEnd="+mEnd+" (points="+numWindowPoints+", histograms="+histograms.size()+")");

		final double[] referenceVector = new double[ tiles.length ];
		for ( int mIndex = mStart; mIndex < mEnd; mIndex++ )
		{
			final int pixel = sortedMeans.get( mIndex ).b;
			int counter = 0;
			for ( final Entry< Short, Integer > entry : histograms.get( pixel ).entrySet() )
				for ( int j = 0; j < entry.getValue(); j++ )
					referenceVector[ counter++ ] += entry.getKey();
		}
		for ( int i = 0; i < referenceVector.length; i++ )
			referenceVector[ i ] /= numWindowPoints;

		return referenceVector;
	}



	/*private <
		M extends Model< M > & Affine1D< M >,
		R extends Model< R > & Affine1D< R > & InvertibleBoundable >
	Pair< ArrayImg< DoubleType, DoubleArray >, ArrayImg< DoubleType, DoubleArray > > leastSquaresInterpolationFit(
			final int scale,
			final TreeMap< Short, Integer >[] histograms,
			final double[] referenceVector,
			final Pair< ArrayImg< DoubleType, DoubleArray >, ArrayImg< DoubleType, DoubleArray > > downsampledSolution ) throws Exception
	{
		final double[] v = new double[ histograms.length ], z = new double[ histograms.length ];

		final double[]
				vDownsampled = ( downsampledSolution != null ? downsampledSolution.getA().update( null ).getCurrentStorageArray() : null ),
				zDownsampled = ( downsampledSolution != null ? downsampledSolution.getB().update( null ).getCurrentStorageArray() : null );

		final long[] workingSize = new long[] {
				fullSize[ 0 ] / ( 1 << scale ),
				fullSize[ 1 ] / ( 1 << scale )
		};

		final long[] downsampledSize = ( downsampledSolution != null ? workingSize.clone() : null);
		if ( downsampledSize != null )
			for ( int d = 0; d < downsampledSize.length; d++ )
				downsampledSize[ d ] >>= 1;

		System.out.println( "Working size: " + Arrays.toString(workingSize) + ( downsampledSize != null ? ", downsampled size: " + Arrays.toString(downsampledSize) : "" ) );

		multithreadedExecutor.run( pixel ->
			{
				final double[] p = new double[ referenceVector.length ], q = new double[ referenceVector.length ], w = new double[ referenceVector.length ];
				Arrays.fill( w, 1.0 );

				int counter = 0;
				for ( final Entry< Short, Integer > entry : histograms[ pixel ].entrySet() )
				{
					for ( int j = 0; j < entry.getValue(); j++ )
					{
						final int i = counter++;
						p[ i ] = entry.getKey();
						q[ i ] = referenceVector[ i ];
					}
				}

				final M model;
				final R regularizerModel;

				if ( downsampledSolution != null )
				{
					final long[] position = new long[ workingSize.length ];
					IntervalIndexer.indexToPosition( pixel, workingSize, position );
					for ( int d = 0; d < position.length; d++ )
						position[ d ] = Math.min( position[ d ] >> 1, downsampledSize[ d ] - 1 );

					final int downsampledPixel = ( int ) IntervalIndexer.positionToIndex( position, downsampledSize );

					model = ( M ) new FixedScalingAffineModel1D( vDownsampled[ downsampledPixel ] );

					final AffineModel1D downsampledModel = new AffineModel1D();
					downsampledModel.set( vDownsampled[ downsampledPixel ], zDownsampled[ downsampledPixel ] );
					regularizerModel = ( R ) downsampledModel;
				}
				else
				{
					model = ( M ) new AffineModel1D();
					regularizerModel = ( R ) new IdentityModel();
				}

				final InterpolatedAffineModel1D< M, ConstantAffineModel1D< R > > interpolatedModel = new InterpolatedAffineModel1D<>(
						model,
						new ConstantAffineModel1D<>( regularizerModel ),
						INTERPOLATION_LAMBDA );

				try
				{
					interpolatedModel.fit( new double[][] { p }, new double[][] { q }, w );

//					final List< PointMatch > candidates = new ArrayList<>(), inliers = new ArrayList<>();
//					for ( int i = 0; i < referenceVector.length; i++ )
//						if ( p[ i ] != 0 && q[ i ] != 0 )
//							candidates.add(
//									new PointMatch(
//											new Point( new double[] { p[ i ] } ),
//											new Point( new double[] { q[ i ] } ) ) );
//
//					interpolatedModel.filter( candidates, inliers );
				}
				catch ( final Exception e )
				{
					e.printStackTrace();
				}

				final double[] m = new double[ 2 ];
				interpolatedModel.toArray( m );
				v[ pixel ] = m[ 0 ];
				z[ pixel ] = m[ 1 ];
			},
			histograms.length );

		return new ValuePair<>( ArrayImgs.doubles( v, workingSize ), ArrayImgs.doubles( z, workingSize ) );
	}*/



	private <
		A extends ArrayImg< DoubleType, DoubleArray >,
		M extends Model< M > & Affine1D< M >,
		R extends Model< R > & Affine1D< R > & InvertibleBoundable,
		T extends TreeMap< Short, Integer > >
	Pair< A, A > leastSquaresInterpolationFit(
			final int scale,
			final DirectAccessListImg< T > histograms,
			final double[] referenceVector,
			final Pair< A, A > downsampledSolution ) throws Exception
	{
		// Determine the model
		final ModelType modelType = ModelType.AffineModel;
//		if ( downsampledSolution == null )
//			modelType = ModelType.AffineModel;
//		else if ( scale >= getNumScales() / 2 )
//			modelType = ModelType.FixedTranslationAffineModel;
//		else
//			modelType = ModelType.FixedScalingAffineModel;

		final RegularizerModelType regularizerModelType = ( downsampledSolution == null ? RegularizerModelType.IdentityModel : RegularizerModelType.AffineModel );

		// Prepare dimensions
		final long[] workingSize = Intervals.dimensionsAsLongArray( histograms );
		final long[] downsampledSize = ( downsampledSolution != null ? Intervals.dimensionsAsLongArray( downsampledSolution.getA() ) : null );

		System.out.println( String.format("Working size: %s%s;  model: %s,  regularizer model: %s", Arrays.toString(workingSize), (downsampledSize != null ? String.format(", downsampled size: %s", Arrays.toString(downsampledSize)) : ""), modelType, regularizerModelType) );

		// Prepare images
		final Pair< A, A > solution = ( Pair< A, A >) new ValuePair<>( ArrayImgs.doubles( workingSize ), ArrayImgs.doubles( workingSize ) );

		final Pair< RandomAccessible< DoubleType >, RandomAccessible< DoubleType > > interpolatedDownsampledSolution;
		if ( downsampledSolution != null )
		{
			interpolatedDownsampledSolution = new ValuePair<>(
					prepareRegularizerImage( downsampledSolution.getA(), scale ),
					prepareRegularizerImage( downsampledSolution.getB(), scale ) );
		}
		else
		{
			interpolatedDownsampledSolution = null;
		}

		// Prepare image access
		final Pair< RandomAccess< DoubleType >, RandomAccess< DoubleType > > solutionRandomAccess = new ValuePair<>(
				solution.getA().randomAccess(),
				solution.getB().randomAccess() );

		final Pair< RandomAccess< DoubleType >, RandomAccess< DoubleType > > interpolatedDownsampledSolutionRandomAccess;
		if ( downsampledSolution != null )
		{
			interpolatedDownsampledSolutionRandomAccess = new ValuePair<>(
					interpolatedDownsampledSolution.getA().randomAccess(),
					interpolatedDownsampledSolution.getB().randomAccess() );
		}
		else
		{
			interpolatedDownsampledSolutionRandomAccess = null;
		}

		final AtomicLong candidatesCount = new AtomicLong(), inliersCount = new AtomicLong();

		// Perform fitting
		multithreadedExecutor.run( pixel ->
			{
				final long[] position = new long[ workingSize.length ];
				IntervalIndexer.indexToPosition( pixel, workingSize, position );

				final double[] p = new double[ referenceVector.length ], q = new double[ referenceVector.length ], w = new double[ referenceVector.length ];
				Arrays.fill( w, 1.0 );

				int counter = 0;
				for ( final Entry< Short, Integer > entry : histograms.get( pixel ).entrySet() )
				{
					for ( int j = 0; j < entry.getValue(); j++ )
					{
						final int i = counter++;
						p[ i ] = entry.getKey();
						q[ i ] = referenceVector[ i ];
					}
				}


				final double[] mPrev;
				if ( interpolatedDownsampledSolutionRandomAccess != null )
				{
					mPrev = new double[ 2 ];
					synchronized ( interpolatedDownsampledSolutionRandomAccess )
					{
						interpolatedDownsampledSolutionRandomAccess.getA().setPosition( position );
						mPrev[ 0 ] = interpolatedDownsampledSolutionRandomAccess.getA().get().get();

						interpolatedDownsampledSolutionRandomAccess.getB().setPosition( position );
						mPrev[ 1 ] = interpolatedDownsampledSolutionRandomAccess.getB().get().get();
					}
				}
				else
				{
					mPrev = null;
				}

				final M model;
				switch ( modelType )
				{
				case AffineModel:
					model = ( M ) new AffineModel1D();
					break;
				case FixedTranslationAffineModel:
					model = ( M ) new FixedTranslationAffineModel1D( mPrev[ 1 ] );
					break;
				case FixedScalingAffineModel:
					model = ( M ) new FixedScalingAffineModel1D( mPrev[ 0 ] );
					break;
				default:
					model = null;
					break;
				}

				final R regularizerModel;
				switch ( regularizerModelType )
				{
				case IdentityModel:
					regularizerModel = ( R ) new IdentityModel();
					break;
				case AffineModel:
					final AffineModel1D downsampledModel = new AffineModel1D();
					downsampledModel.set( mPrev[ 0 ], mPrev[ 1 ] );
					regularizerModel = ( R ) downsampledModel;
					break;
				default:
					regularizerModel = null;
					break;
				}


				final InterpolatedAffineModel1D< M, ConstantAffineModel1D< R > > interpolatedModel = new InterpolatedAffineModel1D<>(
						model,
						new ConstantAffineModel1D<>( regularizerModel ),
						INTERPOLATION_LAMBDA );

				try
				{
					interpolatedModel.fit( new double[][] { p }, new double[][] { q }, w );

//					final List< PointMatch > candidates = new ArrayList<>(), inliers = new ArrayList<>();
//					for ( int i = 0; i < referenceVector.length; i++ )
//						candidates.add(
//								new PointMatch(
//										new Point( new double[] { p[ i ] } ),
//										new Point( new double[] { q[ i ] } ) ) );
//
//					interpolatedModel.filter( candidates, inliers );
//
//					candidatesCount.addAndGet( candidates.size() );
//					inliersCount.addAndGet( inliers.size() );
				}
				catch ( final Exception e )
				{
					e.printStackTrace();
				}

				final double[] mCurr = new double[ 2 ];
				interpolatedModel.toArray( mCurr );

				synchronized ( solutionRandomAccess )
				{
					solutionRandomAccess.getA().setPosition( position );
					solutionRandomAccess.getA().get().set( mCurr[ 0 ] );

					solutionRandomAccess.getB().setPosition( position );
					solutionRandomAccess.getB().get().set( mCurr[ 1 ] );
				}
			},
			( int ) histograms.size() );

		System.out.println( String.format( "Got solution for scale=%d", scale ) + ( candidatesCount.get() > 0 ? String.format( ", inliers rate=%f", ( double ) inliersCount.get() / candidatesCount.get() ) : "" ) );

		return solution;
	}



	// TODO: old version -- delete?
	/*private
	< A extends Model< A > & Affine1D< A > & InvertibleBoundable >
	Pair< ArrayImg< DoubleType, DoubleArray >, ArrayImg< DoubleType, DoubleArray > > leastSquaresInterpolationFit(
			final int scale,
			final TreeMap< Short, Integer >[] histograms,
			final double[] referenceVector,
			final Pair< ArrayImg< DoubleType, DoubleArray >, ArrayImg< DoubleType, DoubleArray > > downsampledSolution ) throws Exception
	{
		final double[] v = new double[ histograms.length ], z = new double[ histograms.length ];

		final double[]
				vDownsampled = ( downsampledSolution != null ? downsampledSolution.getA().update( null ).getCurrentStorageArray() : null ),
				zDownsampled = ( downsampledSolution != null ? downsampledSolution.getB().update( null ).getCurrentStorageArray() : null );

		final long[] workingSize = new long[] {
				fullSize[ 0 ] / ( 1 << scale ),
				fullSize[ 1 ] / ( 1 << scale )
		};

		final long[] downsampledSize = ( downsampledSolution != null ? workingSize.clone() : null);
		if ( downsampledSize != null )
			for ( int d = 0; d < downsampledSize.length; d++ )
				downsampledSize[ d ] >>= 1;

		System.out.println( "Working size: " + Arrays.toString(workingSize) + ( downsampledSize != null ? ", downsampled size: " + Arrays.toString(downsampledSize) : "" ) );

		multithreadedExecutor.run( pixel ->
			{
				final double[] p = new double[ referenceVector.length ], q = new double[ referenceVector.length ], w = new double[ referenceVector.length ];
				Arrays.fill( w, 1.0 );

				int counter = 0;
				for ( final Entry< Short, Integer > entry : histograms[ pixel ].entrySet() )
				{
					for ( int j = 0; j < entry.getValue(); j++ )
					{
						final int i = counter++;
						p[ i ] = entry.getKey();
						q[ i ] = referenceVector[ i ];
					}
				}

				final ConstantAffineModel1D< A > regularizer;

				if ( downsampledSolution != null )
				{
					final long[] position = new long[ workingSize.length ];
					IntervalIndexer.indexToPosition( pixel, workingSize, position );
					for ( int d = 0; d < position.length; d++ )
						position[ d ] = Math.min( position[ d ] >> 1, downsampledSize[ d ] - 1 );
					final int downsampledPixel = ( int ) IntervalIndexer.positionToIndex( position, downsampledSize );

					final AffineModel1D downsampledModel = new AffineModel1D();
					downsampledModel.set( vDownsampled[ downsampledPixel ], zDownsampled[ downsampledPixel ] );
					regularizer = new ConstantAffineModel1D<>( ( A ) downsampledModel );
				}
//				if ( downsampledSolution != null )
//				{
//					final long[] offset = new long[ workingSize.length ];
//					for ( int d = 0; d < offset.length; d++ )
//						offset[ d ] = ( ( (getNumScales() - 1) - scale ) % (long)Math.pow(d, workingSize.length) ) - d;
//
//					final long[] position = new long[ workingSize.length ];
//					IntervalIndexer.indexToPosition( pixel, workingSize, position );
//					for ( int d = 0; d < position.length; d++ )
//						position[ d ] = Math.min( position[ d ] >> 1, downsampledSize[ d ] - 1 );
//
//					final int downsampledPixel = ( int ) IntervalIndexer.positionToIndex( position, downsampledSize );
//
//					model = ( M ) new FixedScalingAffineModel1D( vDownsampled[ downsampledPixel ] );
//					//model = ( M ) new AffineModel1D();
//
//					final AffineModel1D downsampledModel = new AffineModel1D();
//					downsampledModel.set( vDownsampled[ downsampledPixel ], zDownsampled[ downsampledPixel ] );
//					regularizerModel = ( R ) downsampledModel;
//				}
				else
				{
					regularizer = new ConstantAffineModel1D<>( ( A ) new IdentityModel() );
				}

				final InterpolatedAffineModel1D< AffineModel1D, ConstantAffineModel1D< A > > interpolatedModel = new InterpolatedAffineModel1D<>(
						new AffineModel1D(),
						regularizer,
						INTERPOLATION_LAMBDA );

				try
				{
					//interpolatedModel.fit( new double[][] { p }, new double[][] { q }, w );

					final List< PointMatch > candidates = new ArrayList<>(), inliers = new ArrayList<>();
					for ( int i = 0; i < referenceVector.length; i++ )
						if ( p[ i ] != 0 && q[ i ] != 0 )
							candidates.add(
									new PointMatch(
											new Point( new double[] { p[ i ] } ),
											new Point( new double[] { q[ i ] } ) ) );

					interpolatedModel.filter( candidates, inliers, 3 );
				}
				catch ( final NotEnoughDataPointsException e )
				{
					e.printStackTrace();
				}

				final double[] m = new double[ 2 ];
				interpolatedModel.toArray( m );
				v[ pixel ] = m[ 0 ];
				z[ pixel ] = m[ 1 ];
			},
			histograms.length );

		return new ValuePair<>( ArrayImgs.doubles( v, workingSize ), ArrayImgs.doubles( z, workingSize ) );
	}*/


	private < T extends NativeType< T > & RealType< T > > void populateHistograms() throws Exception
	{
		System.out.println( String.format( "Precomputing histograms for %d scales", getNumScales() ) );

		final JavaRDD< TileInfo > rddTiles = sparkContext.parallelize( Arrays.asList( tiles ) ).cache();

		for ( int scale = 0; scale < getNumScales(); scale++ )
		{
			final int currentScale = scale;
			System.out.println( "Processing scale " + currentScale );

			// Check for existing histograms
			final Set< Integer > remainingSlices = new HashSet<>();
			for ( int slice = 1; slice <= getNumSlices(); slice++ )
				if ( !Files.exists( Paths.get( generateSliceHistogramsPath( currentScale, slice ) ) ) )
					remainingSlices.add( slice );

			for ( final int currentSlice : remainingSlices )
			{
				System.out.println( "  Processing slice " + currentSlice );

				final TreeMap<Short,Integer>[] histograms = rddTiles.treeAggregate(
					null, // zero value

					// generator
					( intermediateHist, tile ) ->
					{
						final ImagePlus imp = TiffSliceLoader.loadSlice( tile, currentSlice );
						Utils.workaroundImagePlusNSlices( imp );

						final Img< T > img = ImagePlusImgs.from( imp );
						final Img< T > scaledImg;

						if ( currentScale == 0 )
						{
							scaledImg = img;
						}
						else
						{
							final int[] dimFactors = new int[ img.numDimensions() ];
							Arrays.fill( dimFactors, 1 << currentScale );

							final int[] scaledImgDimensions = Intervals.dimensionsAsIntArray( img );
							for ( int d = 0; d < scaledImgDimensions.length; d++ )
								scaledImgDimensions[ d ] /= dimFactors[ d ];

							scaledImg = new ImagePlusImgFactory< T >().create( scaledImgDimensions, ( T ) tile.getType().getType().createVariable() );
							Downsample.downsample( img, scaledImg, dimFactors );
						}

						final Cursor< T > cursor = Views.iterable( scaledImg ).localizingCursor();
						final int[] dimensions = Intervals.dimensionsAsIntArray( scaledImg );
						final int[] position = new int[ dimensions.length ];

						final TreeMap<Short,Integer>[] ret;
						if ( intermediateHist != null )
						{
							ret = intermediateHist;
						}
						else
						{
							ret = new TreeMap[ (int) scaledImg.size() ];
							for ( int i = 0; i < ret.length; i++ )
								ret[ i ] = new TreeMap<>();
						}

						while ( cursor.hasNext() )
						{
							cursor.fwd();
							cursor.localize( position );
							final int pixel = IntervalIndexer.positionToIndex( position, dimensions );
							final short key = ( short ) cursor.get().getRealDouble();
							ret[ pixel ].put( key, ret[ pixel ].getOrDefault( key, 0 ) + 1 );
						}

						imp.close();
						return ret;
					},

					// reducer
					( a, b ) ->
					{
						if ( a == null )
							return b;
						else if ( b == null )
							return a;

						for ( int pixel = 0; pixel < b.length; pixel++ )
							for ( final Entry< Short, Integer > entry : b[ pixel ].entrySet() )
								a[ pixel ].put( entry.getKey(), a[ pixel ].getOrDefault( entry.getKey(), 0 ) + entry.getValue() );
						return a;
					},

					getAggregationTreeDepth() );

				System.out.println( "Obtained result for slice " + currentSlice + ", saving..");

				saveSliceHistogramsToDisk( currentScale, currentSlice, histograms );
			}
		}
	}


	private Map< String, double[] > loadSolution( final int slice )
	{
		final Map< String, double[] > imagesFlattened = new HashMap<>();
		imagesFlattened.put( "v", null );
		imagesFlattened.put( "z", null );

		for ( final Entry< String, double[] > entry : imagesFlattened.entrySet() )
		{
			final String path = solutionPath + "/" + entry.getKey() + "/" + slice + ".tif";
			if ( !Files.exists( Paths.get( path ) ) )
				return null;

			final ImagePlus imp = IJ.openImage( path );
			Utils.workaroundImagePlusNSlices( imp );

			final Img< ? extends RealType > img = ImagePlusImgs.from( imp );
			final Cursor< ? extends RealType > imgCursor = Views.flatIterable( img ).cursor();

			final ArrayImg< DoubleType, DoubleArray > arrayImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( img ) );
			final Cursor< DoubleType > arrayImgCursor = Views.flatIterable( arrayImg ).cursor();

			while ( arrayImgCursor.hasNext() || imgCursor.hasNext() )
				arrayImgCursor.next().setReal( imgCursor.next().getRealDouble() );

			imp.close();
			entry.setValue( arrayImg.update( null ).getCurrentStorageArray() );
		}

		return imagesFlattened;
	}

	private void correctImages( final int slice, final double[] v, final double[] z )
	{
		final String outPath = solutionPath + "/corrected/" + slice;
		new File( outPath ).mkdirs();

		// Prepare broadcast variables for V and Z
		final Broadcast< double[] > vBroadcasted = sparkContext.broadcast( v ), zBroadcasted = sparkContext.broadcast( z );

		//final double v_mean = mean( vFinal );
		//final double z_mean = mean( zFinal );

		final Accumulator< Integer > remaining = sparkContext.accumulator( tiles.length, "Tiles remaining" );

		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
		rdd.foreach( tile ->
				{
					final ImagePlus imp = TiffSliceLoader.loadSlice(tile, slice);
					Utils.workaroundImagePlusNSlices( imp );
					final Img< ? extends RealType > img = ImagePlusImgs.from( imp );
					final Cursor< ? extends RealType > imgCursor = Views.flatIterable( img ).cursor();

					final ArrayImg< DoubleType, DoubleArray > vImg = ArrayImgs.doubles( vBroadcasted.value(), Intervals.dimensionsAsLongArray( img ) );
					final ArrayImg< DoubleType, DoubleArray > zImg = ArrayImgs.doubles( zBroadcasted.value(), Intervals.dimensionsAsLongArray( img ) );
					final Cursor< DoubleType > vCursor = Views.flatIterable( vImg ).cursor();
					final Cursor< DoubleType > zCursor = Views.flatIterable( zImg ).cursor();

					final ArrayImg< DoubleType, DoubleArray > correctedImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( img ) );
					final Cursor< DoubleType > correctedImgCursor = Views.flatIterable( correctedImg ).cursor();

					while ( correctedImgCursor.hasNext() || imgCursor.hasNext() )
						correctedImgCursor.next().setReal( (imgCursor.next().getRealDouble() - zCursor.next().get()) / vCursor.next().get() );   // * v_mean + z_mean


//						final ArrayImg< UnsignedShortType, ShortArray > correctedImgShort = ArrayImgs.unsignedShorts( originalSize );
//						final Cursor< UnsignedShortType > correctedImgShortCursor = Views.flatIterable( correctedImgShort ).cursor();
//						correctedImgCursor.reset();
//						while ( correctedImgShortCursor.hasNext() || correctedImgCursor.hasNext() )
//							correctedImgShortCursor.next().setReal( correctedImgCursor.next().get() );

					final ImagePlus correctedImp = ImageJFunctions.wrap( correctedImg, "" );
					Utils.workaroundImagePlusNSlices( correctedImp );
					IJ.saveAsTiff( correctedImp, outPath + "/"+ Utils.addFilenameSuffix( Paths.get(tile.getFilePath()).getFileName().toString(), "_corrected" ) );

					imp.close();
					correctedImp.close();

					remaining.add( -1 );
				}
			);
	}





	private void saveSliceHistogramsToDisk( final int scale, final int slice, final TreeMap<Short,Integer>[] hist ) throws Exception
	{
		final String path = generateSliceHistogramsPath( scale, slice );

		new File( Paths.get( path ).getParent().toString() ).mkdirs();

		final OutputStream os = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream( path )
						)
				);

		//final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();
		final MapSerializer serializer = new MapSerializer();
		serializer.setKeysCanBeNull( false );
		serializer.setKeyClass( Short.class, kryo.getSerializer( Short.class ) );
		serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
		kryo.register( TreeMap.class, serializer );
		kryo.register( TreeMap[].class );

		//try ( final Output output = kryoSerializer.newKryoOutput() )
		//{
		//	output.setOutputStream( os );
		try ( final Output output = new Output( os ) )
		{
			kryo.writeClassAndObject( output, hist );
		}
	}

	private DirectAccessListImg< TreeMap< Short, Integer > > readSliceHistogramsFromDisk( final int scale, final int slice ) throws Exception
	{
		System.out.println( "Loading slice " + slice );
		final String path = generateSliceHistogramsPath( scale, slice );

		if ( !Files.exists(Paths.get(path)) )
			return null;

		final InputStream is = new DataInputStream(
				new BufferedInputStream(
						new FileInputStream( path )
						)
				);

		//final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();
		final MapSerializer serializer = new MapSerializer();
		serializer.setKeysCanBeNull( false );
		serializer.setKeyClass( Short.class, kryo.getSerializer( Short.class ) );
		serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
		kryo.register( TreeMap.class, serializer );
		kryo.register( TreeMap[].class );

		try ( final Input input = new Input( is ) )
		{
			final TreeMap< Short, Integer >[] histograms = ( TreeMap< Short, Integer >[] ) kryo.readClassAndObject( input );
			return new DirectAccessListImg<>( Arrays.asList( histograms ), getSizeAtScale( scale ) );
		}
	}


	private void saveReferenceVectorToDisk( final double[] vec ) throws Exception
	{
		final String path = inputFilepath + "_Q.ser";

		final OutputStream os = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream( path )
						)
				);

		//final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();
		kryo.register( double[].class );

		//try ( final Output output = kryoSerializer.newKryoOutput() )
		//{
		//	output.setOutputStream( os );
		try ( final Output output = new Output( os ) )
		{
			kryo.writeClassAndObject( output, vec );
		}
	}

	private double[] readReferenceVectorFromDisk() throws Exception
	{
		final String path = inputFilepath + "_Q.ser";

		if ( !Files.exists(Paths.get(path)) )
			return null;

		final InputStream is = new DataInputStream(
				new BufferedInputStream(
						new FileInputStream( path )
						)
				);

		//final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();
		kryo.register( double[].class );

		try ( final Input input = new Input( is ) )
		{
			return ( double[] ) kryo.readClassAndObject( input );
		}
	}


	private boolean allHistogramsReady() throws Exception
	{
		for ( int scale = 0; scale < getNumScales(); scale++ )
			for ( int slice = 1; slice <= getNumSlices(); slice++ )
				if ( !Files.exists( Paths.get( generateSliceHistogramsPath( scale, slice ) ) ) )
					return false;
		return true;
	}

	private String generateSliceHistogramsPath( final int scale, final int slice )
	{
		return histogramsPath + "/" + scale + "/" + slice + ".hist";
	}

	private int getNumSlices()
	{
		return ( int ) ( fullSize.length == 3 ? fullSize[ 2 ] : 1 );
	}

	private int getNumScales()
	{
		return 10;
		//return 1 + ( int ) ( Math.log( Math.min( fullSize[ 0 ], fullSize[ 1 ] ) ) / Math.log( 2 ) );
	}

	private int getAggregationTreeDepth()
	{
		return (int) Math.ceil( Math.log( sparkContext.defaultParallelism() ) / Math.log( 2 ) );
	}

	private long[] getMinSize( final TileInfo[] tiles )
	{
		final long[] minSize = tiles[ 0 ].getSize().clone();
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < minSize.length; d++ )
				if (minSize[ d ] > tile.getSize( d ))
					minSize[ d ] = tile.getSize( d );
		return minSize;
	}

	private long[] getSizeAtScale( final int scale )
	{
		final long[] size = new long[ fullSize.length ];
		for ( int d = 0; d < size.length; d++ )
			size[ d ] = fullSize[ d ] >> scale;
		return size;
	}


	private < T extends RealType< T > & NativeType< T > > void rescale( final Img< T > srcImg, final Img< T > dstImg )
	{
		final RandomAccessibleOnRealRandomAccessible< T > srcImgTransformedRastered = prepareRescaledImage( srcImg, dstImg );
		final IntervalView< T > srcImgScaledView = Views.interval( srcImgTransformedRastered, dstImg );
		final IterableInterval< T > srcImgScaledIterable = Views.flatIterable( srcImgScaledView );
		final Cursor< T > srcImgScaledCursor = srcImgScaledIterable.cursor();

		// Prepare destination image
		final IterableInterval< T > dstImgIterable = Views.flatIterable( dstImg );
		final Cursor< T > dstImgCursor = dstImgIterable.cursor();

		// Write data
		while ( dstImgCursor.hasNext() && srcImgScaledCursor.hasNext() )
			dstImgCursor.next().set( srcImgScaledCursor.next() );
	}

	private < T extends RealType< T > & NativeType< T > > RandomAccessibleOnRealRandomAccessible< T > prepareRescaledImage( final Img< T > srcImg, final Dimensions dstImgDimensions )
	{
		// Define scaling transform
		final double[] scalingCoeffs = new double[ srcImg.numDimensions() ];
		for ( int d = 0; d < scalingCoeffs.length; d++ )
			scalingCoeffs[ d ] = (double) dstImgDimensions.dimension( d ) / srcImg.dimension( d );
		final Scale scalingTransform = new Scale( scalingCoeffs );

		// Set up the transform
		final ExtendedRandomAccessibleInterval< T, Img< T > > srcImgExtended = Views.extendBorder( srcImg );
		final RealRandomAccessible< T > srcImgInterpolated = Views.interpolate( srcImgExtended, new NLinearInterpolatorFactory<>() );
		final RealTransformRandomAccessible< T, InverseRealTransform > srcImgTransformed = RealViews.transform( srcImgInterpolated, scalingTransform );
		final RandomAccessibleOnRealRandomAccessible< T > srcImgTransformedRastered = Views.raster( srcImgTransformed );

		return srcImgTransformedRastered;
	}

	private < T extends RealType< T > & NativeType< T > > RandomAccessible< T > prepareRegularizerImage( final Img< T > srcImg, final int level )
	{
		// Define the transform
		final double[] scale = new double[ srcImg.numDimensions() ], translation = new double[ srcImg.numDimensions() ];
		Arrays.fill( scale, 2 );
		Arrays.fill( translation, ( level > 0 ? -0.5 : -1 ) );
		final ScaleAndTranslation transform = new ScaleAndTranslation( scale, translation );

		return RealViews.transform(
				Views.interpolate( Views.extendBorder( srcImg ), new NLinearInterpolatorFactory<>() ),
				transform );

	}

	/*private final double[] rescaleHelper( final double[] srcArr, final String title )
	{
		final ArrayImg< DoubleType, DoubleArray > srcImg = ArrayImgs.doubles( srcArr, originalSize );
		final ArrayImg< DoubleType, DoubleArray > dstImg = ArrayImgs.doubles( originalSize );
		rescale( srcImg, dstImg );

		final ImagePlus imp = ImageJFunctions.wrap( dstImg, title );
		Utils.workaroundImagePlusNSlices( imp );
		IJ.saveAsTiff( imp, inputFilepath + "_" + title + ".tif" );

		return dstImg.update( null ).getCurrentStorageArray();
	}

	private void correctImages()
	{
		final String subfolder = "corrected";
		new File( Paths.get( inputFilepath ).getParent().toString() + "/" + subfolder ).mkdirs();

		// Prepare broadcast variables for V and Z
		final double[] vFinalData = vFinal, zFinalData = zFinal;
		final Broadcast< double[] > vFinalBroadcasted = sparkContext.broadcast( vFinalData ), zFinalBroadcasted = sparkContext.broadcast( zFinalData );

		//final double v_mean = mean( vFinal );
		//final double z_mean = mean( zFinal );

		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
		final JavaRDD< TileInfo > task = rdd.map(
				new Function< TileInfo, TileInfo >()
				{
					private static final long serialVersionUID = 4991255417353136684L;

					@Override
					public TileInfo call( final TileInfo tile ) throws Exception
					{
						final ImagePlus imp = IJ.openImage( tile.getFilePath() );
						Utils.workaroundImagePlusNSlices( imp );
						final Img< ? extends RealType > img = ImagePlusImgs.from( imp );
						final Cursor< ? extends RealType > imgCursor = Views.flatIterable( img ).cursor();

						final ArrayImg< DoubleType, DoubleArray > vFinalImg = ArrayImgs.doubles( vFinalBroadcasted.value(), originalSize );
						final ArrayImg< DoubleType, DoubleArray > zFinalImg = ArrayImgs.doubles( zFinalBroadcasted.value(), originalSize );
						final Cursor< DoubleType > vCursor = Views.flatIterable( vFinalImg ).cursor();
						final Cursor< DoubleType > zCursor = Views.flatIterable( zFinalImg ).cursor();

						final ArrayImg< DoubleType, DoubleArray > correctedImg = ArrayImgs.doubles( originalSize );
						final Cursor< DoubleType > correctedImgCursor = Views.flatIterable( correctedImg ).cursor();

						while ( correctedImgCursor.hasNext() || imgCursor.hasNext() )
							correctedImgCursor.next().setReal( (imgCursor.next().getRealDouble() - zCursor.next().get()) / vCursor.next().get() );   // * v_mean + z_mean


//						final ArrayImg< UnsignedShortType, ShortArray > correctedImgShort = ArrayImgs.unsignedShorts( originalSize );
//						final Cursor< UnsignedShortType > correctedImgShortCursor = Views.flatIterable( correctedImgShort ).cursor();
//						correctedImgCursor.reset();
//						while ( correctedImgShortCursor.hasNext() || correctedImgCursor.hasNext() )
//							correctedImgShortCursor.next().setReal( correctedImgCursor.next().get() );

						final ImagePlus correctedImp = ImageJFunctions.wrap( correctedImg, "" );
						Utils.workaroundImagePlusNSlices( correctedImp );
						tile.setType( ImageType.valueOf( correctedImp.getType() ) );
						tile.setFilePath( Paths.get( inputFilepath ).getParent().toString() + "/" + subfolder +"/" + Utils.addFilenameSuffix( Paths.get( tile.getFilePath() ).getFileName().toString(), "_corrected" ) );
						IJ.saveAsTiff( correctedImp, tile.getFilePath() );

						imp.close();
						correctedImp.close();
						return tile;
					}
				});

		correctedTiles = task.collect().toArray( new TileInfo[0] );
		try {
			TileInfoJSONProvider.saveTilesConfiguration( correctedTiles, Utils.addFilenameSuffix( inputFilepath, "_corrected" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}


	private void estimateQ()
	{
		int numWindowPoints = (int) Math.round( numPixels * WINDOW_POINTS_PERCENT );
		final int mStart = (int)( Math.round( numPixels / 2.0 ) - Math.round( numWindowPoints / 2.0 ) ) - 1;
		final int mEnd   = (int)( Math.round( numPixels / 2.0 ) + Math.round( numWindowPoints / 2.0 ) );
		numWindowPoints = mEnd - mStart;

		System.out.println("Estimating Q using mStart="+mStart+", mEnd="+mEnd+" (points="+numWindowPoints+")");

		final int treeDepth = (int) Math.ceil( Math.log( sparkContext.defaultParallelism() ) / Math.log( 2 ) );
		System.out.println( "default parallelism = " + sparkContext.defaultParallelism() + ",  tree depth = " + treeDepth );



		Q = rddHistogramsShrinked.mapToPair( tuple ->
		{
			double pixelMean = 0;
			int count = 0;
			for ( final Entry< Float, Short > entry : tuple._2().entrySet() )
			{
				pixelMean += entry.getKey() * entry.getValue();
				count += entry.getValue();
			}
			pixelMean /= count;
			return new Tuple2<>( pixelMean, tuple._1() );
		}
	)
	.sortByKey()
	.zipWithIndex()
	.mapToPair( pair -> pair.swap() )
	.filter( tuple -> tuple._1() >= mStart && tuple._1() < mEnd )
	.mapToPair( tuple -> tuple._2().swap() )
	.join( rddHistogramsShrinked )
	.mapToPair( item -> item._2() )
	.treeAggregate(
		new double[ N ],
		( ret, tuple ) ->
		{
			int counter = 0;
			for ( final Entry< Float, Short > entry : tuple._2().entrySet() )
				for ( int j = 0; j < entry.getValue(); j++ )
					ret[ counter++ ] += entry.getKey();
			return ret;
		},
		( ret, other ) ->
		{
			for ( int i = 0; i < N; i++ )
				ret[ i ] += other[ i ];
			return ret;
		},
		treeDepth
	);



		for ( int i = 0; i < N; i++ )
			Q[ i ] /= numWindowPoints;


	}
	*/
}
