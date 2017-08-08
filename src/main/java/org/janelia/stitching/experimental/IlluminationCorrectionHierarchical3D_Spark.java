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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.janelia.util.TiffSliceReader;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

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
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.img.list.ListImg;
import net.imglib2.img.list.ListRandomAccess;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.RandomAccessiblePair;
import net.imglib2.view.Views;
import scala.Tuple2;


public class IlluminationCorrectionHierarchical3D_Spark implements Serializable
{
	private static final long serialVersionUID = -8987192045944606043L;

	private static final double WINDOW_POINTS_PERCENT = 0.25;
	private static final double INTERPOLATION_LAMBDA = 0.1;

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
	private long[] fullSize;

	//private JavaPairRDD< Long, List< Long > > downsampledPixelToFullPixels;

	private Broadcast< int[] > broadcastedFullPixelToDownsampledPixel;
	private Broadcast< int[] > broadcastedDownsampledPixelToFullPixelsCount;


	public static void main( final String[] args ) throws Exception
	{
		final IlluminationCorrectionHierarchical3D_Spark driver = new IlluminationCorrectionHierarchical3D_Spark( args[ 0 ] );
		driver.run();
		driver.shutdown();
		System.out.println("Done");
	}


	public IlluminationCorrectionHierarchical3D_Spark( final String inputFilepath )
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
	}


	public <
		A extends ArrayImg< DoubleType, DoubleArray >,
		V extends TreeMap< Short, Integer > >
	void run() throws Exception
	{
		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "IlluminationCorrection3D" )
				//.set( "spark.driver.maxResultSize", "8g" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				//.set( "spark.kryoserializer.buffer.max", "1g" )
				.registerKryoClasses( new Class[] { Short.class, Integer.class, Long.class, Double.class, TreeMap.class, TreeMap[].class, double[].class, List.class, Tuple2.class, Interval.class, FinalInterval.class, ArrayImg.class, DoubleType.class, DoubleArray.class } )
				.set( "spark.rdd.compress", "true" )
				//.set( "spark.executor.heartbeatInterval", "10000000" )
				//.set( "spark.network.timeout", "10000000" )
			);

		try {
			tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilepath );
		} catch (final IOException e) {
			e.printStackTrace();
			return;
		}

		fullSize = getMinSize( tiles );

		// check if all tiles have the same size
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				if ( tile.getSize(d) != fullSize[ d ] )
				{
					System.out.println("Assumption failed: not all the tiles are of the same size");
					System.exit(1);
				}


		/*final Map< Integer, Map< String, double[] > > solutions = new TreeMap<>();
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
		}*/


		if ( !allHistogramsReady() )
		{
			populateHistograms();
			System.out.println( "Computed all histograms" );
			return;
		}


		final int N = tiles.length;
		System.out.println( "Working with stack of size " + N );
		System.out.println( "Output directory: " + solutionPath );

		long elapsed = System.nanoTime();

		System.out.println( "Loading histograms.." );
		final JavaPairRDD< Integer, V > rddFullHistograms = loadHistograms();

		final double[] referenceVector = estimateReferenceVector( rddFullHistograms );
		System.out.println( "Obtained reference vector of size " + referenceVector.length );
		saveReferenceVectorToDisk( referenceVector );

		// Define the transform and calculate the image size on each scale level
		final AffineTransform3D downsamplingTransform = new AffineTransform3D();
		downsamplingTransform.set(
				0.5, 0, 0, -1,
				0, 0.5, 0, -1,
				0, 0, 0.5, -1
			);

		final List< Integer > scales = new ArrayList<>();
		final TreeMap< Integer, long[] > scaleLevelToPixelSize = new TreeMap<>();
		final TreeMap< Integer, long[] > scaleLevelToOffset = new TreeMap<>();
		final TreeMap< Integer, long[] > scaleLevelToDimensions = new TreeMap<>();
		long smallestDimension;
		do
		{
			final int scale = scaleLevelToDimensions.size();
			final long[]
					pixelSize 		= new long[ fullSize.length ],
					offset 			= new long[ fullSize.length ],
					downsampledSize = new long[ fullSize.length ];
			for ( int d = 0; d < downsampledSize.length; d++ )
			{
				// how many original pixels form a single pixel on this scale level
				pixelSize[ d ] = ( long ) ( scale == 0 ? 1 : scaleLevelToPixelSize.get( scale - 1 )[ d ] / downsamplingTransform.get( d, d ) );

				// how many original pixels form the leftmost pixel on this scale level (which could be different from pixelSize[d] because of the translation component)
				offset[ d ] = ( long ) ( scale == 0 ? 0 : Math.min( scaleLevelToPixelSize.get( scale - 1 )[ d ] * downsamplingTransform.get( d, 4 ) + pixelSize[ d ], fullSize[ d ] ) );

				// how many original pixels form the rightmost pixel on this scale level
				final long remaining = fullSize[ d ] - offset[ d ];

				// how many downsampled pixels are on this scale level
				downsampledSize[ d ] = ( offset[ d ] == 0 ? 0 : 1 ) + remaining / pixelSize[ d ] + ( remaining % pixelSize[ d ] == 0 ? 0 : 1 );
			}

			scales.add( scale );
			scaleLevelToPixelSize.put( scale, pixelSize );
			scaleLevelToOffset.put( scale, offset );
			scaleLevelToDimensions.put( scale, downsampledSize );

			smallestDimension = Long.MAX_VALUE;
			for ( int d = 0; d < fullSize.length; d++ )
				smallestDimension = Math.min( downsampledSize[ d ], smallestDimension );
		}
		while ( smallestDimension > 1 );

		System.out.println( "Scale level to dimensions:" );
		for ( final Entry< Integer, long[] > entry : scaleLevelToDimensions.entrySet() )
			System.out.println( String.format( " %d: %s", entry.getKey(), Arrays.toString( entry.getValue() ) ) );

		// Generate solutions iteratively from the smallest scale to the full size
		Collections.reverse( scales );
		Pair< A, A > lastSolution = null;
		for ( final int scale : scales )
		{
			final ModelType modelType;
			if ( lastSolution == null )
				modelType = ModelType.AffineModel;
			else if ( scale >= scales.size() / 2 )
				modelType = ModelType.FixedTranslationAffineModel;
			else
				modelType = ModelType.FixedScalingAffineModel;

			final RegularizerModelType regularizerModelType;
			if ( lastSolution == null )
				regularizerModelType = RegularizerModelType.IdentityModel;
			else
				regularizerModelType = RegularizerModelType.AffineModel;

			lastSolution = leastSquaresInterpolationFit(
					downsampleHistograms(
							rddFullHistograms,
							scaleLevelToPixelSize.get( scale ),
							scaleLevelToOffset.get( scale ),
							scaleLevelToDimensions.get( scale ) ),
					scale,
					scaleLevelToDimensions.get( scale ),
					referenceVector,
					lastSolution,
					downsamplingTransform,
					modelType,
					regularizerModelType );

//			if ( downsampledPixelToFullPixels != null )
//				downsampledPixelToFullPixels.unpersist();

			if ( broadcastedFullPixelToDownsampledPixel != null )
			{
				broadcastedFullPixelToDownsampledPixel.destroy();
				broadcastedFullPixelToDownsampledPixel = null;
			}
			if ( broadcastedDownsampledPixelToFullPixelsCount != null )
			{
				broadcastedDownsampledPixelToFullPixelsCount.destroy();
				broadcastedDownsampledPixelToFullPixelsCount = null;
			}

			saveSolution( lastSolution, scale );
		}

		//final RandomAccessibleInterval< DoubleType > correctedStack = generateCorrectionOutput( solution[ 0 ] );
		//outputMeanStdBlockwise( correctedStack, null );

		elapsed = System.nanoTime() - elapsed;
		System.out.println( "----------" );
		System.out.println( String.format( "Took %f mins", elapsed / 1e9 / 60 ) );
	}


	private <
		A extends ArrayImg< DoubleType, DoubleArray >,
		M extends Model< M > & Affine1D< M >,
		R extends Model< R > & Affine1D< R > & InvertibleBoundable,
		V extends TreeMap< Short, Integer > >
	Pair< A, A > leastSquaresInterpolationFit(
			final JavaPairRDD< Integer, V > rddHistograms,
			final int scale,
			final long[] size,
			final double[] referenceVector,
			final Pair< A, A > downsampledSolution,
			final AffineTransform3D downsamplingTransform,
			final ModelType modelType,
			final RegularizerModelType regularizerModelType ) throws Exception
	{
		final long[] downsampledSize = ( downsampledSolution != null ? Intervals.dimensionsAsLongArray( downsampledSolution.getA() ) : null );
		System.out.println( String.format("Working size: %s%s;  model: %s,  regularizer model: %s", Arrays.toString(size), (downsampledSize != null ? String.format(", downsampled size: %s", Arrays.toString(downsampledSize)) : ""), modelType, regularizerModelType) );

		final Broadcast< double[] > referenceVectorBroadcast = sparkContext.broadcast( referenceVector );
		final Broadcast< Pair< A, A > > downsampledSolutionBroadcast = sparkContext.broadcast( downsampledSolution );

		final JavaPairRDD< Integer, Pair< Double, Double > > rddSolutionPixels = rddHistograms.mapToPair( tuple ->
			{
				final double[] referenceVectorLocal = referenceVectorBroadcast.value();
				final Pair< A, A > downsampledSolutionLocal = downsampledSolutionBroadcast.value();

				final long[] position = new long[ size.length ];
				IntervalIndexer.indexToPosition( tuple._1(), size, position );

				final double[]
						p = new double[ referenceVectorLocal.length ],
						q = new double[ referenceVectorLocal.length ],
						w = new double[ referenceVectorLocal.length ];
				Arrays.fill( w, 1.0 );

				int counter = 0;
				for ( final Entry< Short, Integer > entry : tuple._2().entrySet() )
				{
					for ( int j = 0; j < entry.getValue(); j++ )
					{
						final int i = counter++;
						p[ i ] = entry.getKey();
						q[ i ] = referenceVectorLocal[ i ];
					}
				}

				final double[] mPrev;
				if ( downsampledSolutionLocal != null )
				{
					final RandomAccessiblePair< DoubleType, DoubleType > interpolatedDownsampledSolution = new RandomAccessiblePair<>(
							prepareRegularizerImage( downsampledSolutionLocal.getA(), scale ),
							prepareRegularizerImage( downsampledSolutionLocal.getB(), scale ) );

					final RandomAccessiblePair< DoubleType, DoubleType >.RandomAccess interpolatedDownsampledSolutionRandomAccess = interpolatedDownsampledSolution.randomAccess();
					interpolatedDownsampledSolutionRandomAccess.setPosition( position );

					mPrev = new double[]
						{
							interpolatedDownsampledSolutionRandomAccess.getA().get(),
							interpolatedDownsampledSolutionRandomAccess.getB().get()
						};
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
				}
				catch ( final Exception e )
				{
					e.printStackTrace();
				}

				final double[] mCurr = new double[ 2 ];
				interpolatedModel.toArray( mCurr );

				return new Tuple2<>( tuple._1(), new ValuePair<>( mCurr[ 0 ], mCurr[ 1 ] ) );
			} );

		final List< Tuple2< Integer, Pair< Double, Double > > > solutionPixels  = rddSolutionPixels.collect();

		final Pair< A, A > solution = ( Pair< A, A >) new ValuePair<>( ArrayImgs.doubles( size ), ArrayImgs.doubles( size ) );
		final RandomAccessiblePair< DoubleType, DoubleType >.RandomAccess solutionRandomAccess = new RandomAccessiblePair<>( solution.getA(), solution.getB() ).randomAccess();
		final long[] position = new long[ size.length ];
		for ( final Tuple2< Integer, Pair< Double, Double > > tuple : solutionPixels )
		{
			IntervalIndexer.indexToPosition( tuple._1(), size, position );
			solutionRandomAccess.setPosition( position );

			solutionRandomAccess.getA().set( tuple._2().getA() );
			solutionRandomAccess.getB().set( tuple._2().getB() );
		}

		System.out.println( "Got solution for scale=" + scale );
		return solution;
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
	/*private < T extends RealType< T > & NativeType< T > > RandomAccessible< T > prepareRegularizerImage( final Img< T > srcImg, final AffineTransform3D downsamplingTransform )
	{
		return RealViews.transform(
				Views.interpolate( Views.extendBorder( srcImg ), new NLinearInterpolatorFactory<>() ),
				downsamplingTransform.inverse() );
	}*/


	private < V extends TreeMap< Short, Integer > > JavaPairRDD< Integer, V > loadHistograms() throws Exception
	{
		final List< Integer > slices = new ArrayList<>();
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
			slices.add( slice );

		final JavaRDD< Integer > rddSlices = sparkContext.parallelize( slices );
		return rddSlices
				.flatMapToPair( slice ->
					{
						final RandomAccessibleInterval< V > sliceHistograms = Views.stack( readSliceHistogramsFromDisk( slice ) );
						final ListImg< Tuple2< Integer, V > > ret = new ListImg<>( Intervals.dimensionsAsLongArray( sliceHistograms ), null );

						final Cursor< V > srcCursor = Views.iterable( sliceHistograms ).localizingCursor();
						final ListRandomAccess< Tuple2< Integer, V > > dstRandomAccess = ret.randomAccess();
						while ( srcCursor.hasNext() )
						{
							srcCursor.fwd();
							dstRandomAccess.setPosition( srcCursor );
							dstRandomAccess.set( new Tuple2<>(
									( int ) IntervalIndexer.positionToIndex(
											new long[] { srcCursor.getLongPosition( 0 ), srcCursor.getLongPosition( 1 ), slice - 1 },
											fullSize ),
									srcCursor.get() ) );
						}
						return ret.iterator();
					} )
				//.partitionBy( new HashPartitioner( sparkContext.defaultParallelism() ) )
				.persist( StorageLevel.MEMORY_ONLY_SER() );
	}


	private < V extends TreeMap< Short, Integer > > JavaPairRDD< Integer, V > downsampleHistograms(
			final JavaPairRDD< Integer, V > rddFullHistograms,
			final long[] pixelSize,
			final long[] offset,
			final long[] downsampledSize )
	{
		// Check if targetScaleLevel==fullScaleLevel
		boolean fullScale = true;
		for ( int d = 0; d < fullSize.length; d++ )
			fullScale &= ( fullSize[ d ] == downsampledSize[ d ] );
		if ( fullScale )
			return rddFullHistograms;

		System.out.println( "Downsampling histograms from " + Arrays.toString( fullSize ) + " to " + Arrays.toString( downsampledSize ) );

		long numDownsampledPixels = 1;
		for ( final long side : downsampledSize )
			numDownsampledPixels *= side;
		final List< Tuple2< Integer, Interval > > downsampledPixelToCorrespondingFullInterval = new ArrayList<>( ( int ) numDownsampledPixels );
		final int[] downsampledPosition = new int[ downsampledSize.length ];
		for ( int downsampledPixel = 0; downsampledPixel < numDownsampledPixels; downsampledPixel++ )
		{
			IntervalIndexer.indexToPosition( downsampledPixel, downsampledSize, downsampledPosition );
			final long[] mins = new long[ fullSize.length ], maxs = new long[ fullSize.length ];
			for ( int d = 0; d < mins.length; d++ )
			{
				if ( downsampledPosition[ d ] == 0 )
				{
					mins[ d ] = 0;
					maxs[ d ] = offset[ d ] - 1;
				}
				else
				{
					mins[ d ] = offset[ d ] + pixelSize[ d ] * ( downsampledPosition[ d ] - 1 );
					maxs[ d ] = Math.min( mins[ d ] + pixelSize[ d ], fullSize[ d ] ) - 1;
				}
			}
			downsampledPixelToCorrespondingFullInterval.add( new Tuple2<>( downsampledPixel, new FinalInterval( mins, maxs ) ) );
		}

		final int N = tiles.length;

		// create a mapping of: downsampled pixel -> list of original pixels
		long numFullPixels = 1;
		for ( final long side : fullSize )
			numFullPixels *= side;

		long elapsed = System.nanoTime();
		final int[] fullPixelToDownsampledPixel = new int[ ( int ) numFullPixels ];
		final int[] downsampledPixelToFullPixelsCount = new int[ ( int ) numDownsampledPixels ];
		for ( final Tuple2< Integer, Interval > tuple : downsampledPixelToCorrespondingFullInterval )
		{
			final int downsampledPixel = tuple._1();
			final int n = tuple._2().numDimensions();
			final int[] min = Intervals.minAsIntArray( tuple._2() );
			final int[] max = Intervals.maxAsIntArray( tuple._2() );
			final int[] dimensions = Intervals.dimensionsAsIntArray( new FinalDimensions( fullSize ) );
			final int[] position = min.clone();
			for ( int d = 0; d < n; )
			{
				fullPixelToDownsampledPixel[ IntervalIndexer.positionToIndex( position, dimensions ) ] = downsampledPixel;

				for ( d = 0; d < n; ++d )
				{
					position[ d ]++;
					if ( position[ d ] <= max[ d ] )
						break;
					else
						position[ d ] = min[ d ];
				}
			}

			downsampledPixelToFullPixelsCount[ downsampledPixel ] = ( int ) Intervals.numElements( tuple._2() );
		}
		elapsed = System.nanoTime() - elapsed;
		System.out.println( "Populating matching arrays took " + elapsed / 1e9 + "s" );

		broadcastedFullPixelToDownsampledPixel = sparkContext.broadcast( fullPixelToDownsampledPixel );
		broadcastedDownsampledPixelToFullPixelsCount = sparkContext.broadcast( downsampledPixelToFullPixelsCount );

		return rddFullHistograms
				.mapToPair( tuple -> new Tuple2<>( broadcastedFullPixelToDownsampledPixel.value()[ tuple._1() ], tuple._2() ) )
				.aggregateByKey(
					new long[ N ],
					( ret, histogram ) ->
					{
						int counter = 0;
						for ( final Entry< Short, Integer > entry : histogram.entrySet() )
							for ( int j = 0; j < entry.getValue(); j++ )
								ret[ counter++ ] += entry.getKey();
						return ret;
					},
					( ret, other ) ->
					{
						for ( int i = 0; i < N; i++ )
							ret[ i ] += other[ i ];
						return ret;
					} )
				.mapToPair( tuple ->
					{
						final long[] histogramsSum = tuple._2();
						final int fullPixelsCount = broadcastedDownsampledPixelToFullPixelsCount.value()[ tuple._1() ];

						final V downsampledHistogram = ( V ) new TreeMap< Short, Integer >();
						for ( int i = 0; i < histogramsSum.length; i++ )
						{
							final short key = ( short ) Math.round( ( double ) histogramsSum[ i ] / fullPixelsCount );
							downsampledHistogram.put( key, downsampledHistogram.getOrDefault( key, 0 ) + 1 );
						}
						return new Tuple2<>( tuple._1(), downsampledHistogram );
					}
				);

		// create a mapping of: downsampled pixel -> list of original pixels
		/*downsampledPixelToFullPixels = sparkContext.parallelizePairs( downsampledPixelToCorrespondingFullInterval )
				.mapValues( interval ->
					{
						final List< Long > fullPixels = new ArrayList<>();
						TileOperations.forEachPixel( interval, new FinalDimensions( fullSize ), i -> fullPixels.add( i ) );
						return fullPixels;
					} ).cache();

		final JavaPairRDD< Long, Long > originalPixelToDownsampledPixel = downsampledPixelToFullPixels
				.flatMapValues( val -> val )
				.mapToPair( pair -> pair.swap() );

		return rddFullHistograms.join( originalPixelToDownsampledPixel )
				.mapToPair( tuple -> tuple._2().swap() )
				.aggregateByKey(
					new long[ N ],
					( ret, histogram ) ->
					{
						int counter = 0;
						for ( final Entry< Short, Integer > entry : histogram.entrySet() )
							for ( int j = 0; j < entry.getValue(); j++ )
								ret[ counter++ ] += entry.getKey();
						return ret;
					},
					( ret, other ) ->
					{
						for ( int i = 0; i < N; i++ )
							ret[ i ] += other[ i ];
						return ret;
					} )
				.join( downsampledPixelToFullPixels )
				.mapValues( tuple -> new Tuple2<>( tuple._1(), tuple._2().size() ) )
				.mapValues( tuple ->
					{
						final long[] histogramsSum = tuple._1();
						final int neighbors = tuple._2();

						final V downsampledHistogram = ( V ) new TreeMap< Short, Integer >();
						for ( int i = 0; i < histogramsSum.length; i++ )
						{
							final short key = ( short ) Math.round( ( double ) histogramsSum[ i ] / neighbors );
							downsampledHistogram.put( key, downsampledHistogram.getOrDefault( key, 0 ) + 1 );
						}
						return downsampledHistogram;
					}
				);*/
	}

	private < A extends ArrayImg< DoubleType, DoubleArray > > void saveSolution( final Pair< A, A > solution, final int scale )
	{
		final Map< String, Img< DoubleType > > solutionComponents = new HashMap<>();
		solutionComponents.put( "v", solution.getA() );
		solutionComponents.put( "z", solution.getB() );

		for ( final Entry< String, Img< DoubleType > > solutionComponent : solutionComponents.entrySet() )
		{
			final String path = solutionPath + "/" + scale + "/" + solutionComponent.getKey().toLowerCase() + ".tif";

			new File( Paths.get( path ).getParent().toString() ).mkdirs();

			final ImagePlus imp = ImageJFunctions.wrap( solutionComponent.getValue(), solutionComponent.getKey() + "_scale" + scale );
			Utils.workaroundImagePlusNSlices( imp );
			IJ.saveAsTiff( imp, path );
		}
	}

	private < V extends TreeMap< Short, Integer > > double[] estimateReferenceVector( final JavaPairRDD< Integer, V > rddHistograms ) throws Exception
	{
		long numPixels = 1;
		for ( final long side : fullSize )
			numPixels *= side;

		final int N = tiles.length;

		int numWindowPoints = ( int ) Math.round( numPixels * WINDOW_POINTS_PERCENT );
		final int mStart = ( int ) ( Math.round( numPixels / 2.0 ) - Math.round( numWindowPoints / 2.0 ) ) - 1;
		final int mEnd   = ( int ) ( Math.round( numPixels / 2.0 ) + Math.round( numWindowPoints / 2.0 ) );
		numWindowPoints = mEnd - mStart;

		System.out.println("Estimating Q using mStart="+mStart+", mEnd="+mEnd+" (points="+numWindowPoints+")");

		final double[] referenceVector = rddHistograms
		.mapValues( histogram ->
			{
				double pixelMean = 0;
				int count = 0;
				for ( final Entry< Short, Integer > entry : histogram.entrySet() )
				{
					pixelMean += entry.getKey() * entry.getValue();
					count += entry.getValue();
				}
				pixelMean /= count;
				return pixelMean;
			}
		)
		.mapToPair( pair -> pair.swap() )
		.sortByKey()
		.zipWithIndex()
		.filter( tuple -> tuple._2() >= mStart && tuple._2() < mEnd )
		.mapToPair( tuple -> tuple._1().swap() )
		.join( rddHistograms )
		.map( item -> item._2()._2() )
		.treeAggregate(
			new double[ N ],
			( ret, histogram ) ->
			{
				int counter = 0;
				for ( final Entry< Short, Integer > entry : histogram.entrySet() )
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
			getAggregationTreeDepth()
		);

		for ( int i = 0; i < N; i++ )
			referenceVector[ i ] /= numWindowPoints;

		return referenceVector;
	}


	private <
		T extends NativeType< T > & RealType< T >,
		V extends TreeMap< Short, Integer > >
	void populateHistograms() throws Exception
	{
		final JavaRDD< TileInfo > rddTiles = sparkContext.parallelize( Arrays.asList( tiles ) );

		// Check for existing histograms
		final Set< Integer > remainingSlices = new HashSet<>();
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
			if ( !Files.exists( Paths.get( generateSliceHistogramsPath( 0, slice ) ) ) )
				remainingSlices.add( slice );

		for ( final int currentSlice : remainingSlices )
		{
			System.out.println( "  Processing slice " + currentSlice );

			final V[] histograms = rddTiles.treeAggregate(
				null, // zero value

				// generator
				( intermediateHist, tile ) ->
				{
					final ImagePlus imp = TiffSliceReader.readSlice( tile.getFilePath(), currentSlice );
					Utils.workaroundImagePlusNSlices( imp );

					final Img< T > img = ImagePlusImgs.from( imp );
					final Cursor< T > cursor = Views.iterable( img ).localizingCursor();
					final int[] dimensions = Intervals.dimensionsAsIntArray( img );
					final int[] position = new int[ dimensions.length ];

					final V[] ret;
					if ( intermediateHist != null )
					{
						ret = intermediateHist;
					}
					else
					{
						ret = ( V[] ) new TreeMap[ (int) img.size() ];
						for ( int i = 0; i < ret.length; i++ )
							ret[ i ] = ( V ) new TreeMap< Short, Integer >();
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

			saveSliceHistogramsToDisk( 0, currentSlice, histograms );
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

		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
		rdd.foreach( tile ->
				{
					final ImagePlus imp = TiffSliceReader.readSlice(tile.getFilePath(), slice);
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
				}
			);
	}


	private < V extends TreeMap< Short, Integer > >void saveSliceHistogramsToDisk( final int scale, final int slice, final V[] hist ) throws Exception
	{
		final String path = generateSliceHistogramsPath( scale, slice );

		new File( Paths.get( path ).getParent().toString() ).mkdirs();

		final OutputStream os = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream( path )
						)
				);

//		final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();
		final MapSerializer serializer = new MapSerializer();
		serializer.setKeysCanBeNull( false );
		serializer.setKeyClass( Short.class, kryo.getSerializer( Short.class ) );
		serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
		kryo.register( TreeMap.class, serializer );

		//try ( final Output output = kryoSerializer.newKryoOutput() )
		//{
		//	output.setOutputStream( os );
		try ( final Output output = new Output( os ) )
		{
			kryo.writeClassAndObject( output, hist );
		}
	}


	private < V extends TreeMap< Short, Integer > > ListImg< V > readSliceHistogramsFromDisk( final int slice )
	{
		return new ListImg<>( Arrays.asList( readSliceHistogramsArrayFromDisk( 0, slice ) ), new long[] { fullSize[0],fullSize[1] } );
	}
	private < V extends TreeMap< Short, Integer > > V[] readSliceHistogramsArrayFromDisk( final int scale, final int slice )
	{
		System.out.println( "Loading slice " + slice );
		final String path = generateSliceHistogramsPath( scale, slice );

		if ( !Files.exists(Paths.get(path)) )
			return null;

		try
		{
			final InputStream is = new DataInputStream(
					new BufferedInputStream(
							new FileInputStream( path )
							)
					);

//			final Kryo kryo = kryoSerializer.newKryo();
			final Kryo kryo = new Kryo();

			final MapSerializer serializer = new MapSerializer();
			serializer.setKeysCanBeNull( false );
			serializer.setKeyClass( Short.class, kryo.getSerializer( Short.class ) );
			serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
			kryo.register( TreeMap.class, serializer );

			try ( final Input input = new Input( is ) )
			{
				return ( V[] ) kryo.readClassAndObject( input );
			}
		}
		catch ( final IOException e )
		{
			e.printStackTrace();
			return null;
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

//		final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();

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

//		final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();

		try ( final Input input = new Input( is ) )
		{
			return ( double[] ) kryo.readClassAndObject( input );
		}
	}


	private boolean allHistogramsReady() throws Exception
	{
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
			if ( !Files.exists( Paths.get( generateSliceHistogramsPath( 0, slice ) ) ) )
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

	private int getAggregationTreeDepth()
	{
		return ( int ) Math.ceil( Math.log( sparkContext.defaultParallelism() ) / Math.log( 2 ) );
	}
	/*private int getNumPartitionsForScaleLevel( final int scale )
	{
		return sparkContext.defaultParallelism() * ( 1 << scale );
	}*/

	private long[] getMinSize( final TileInfo[] tiles )
	{
		final long[] minSize = tiles[ 0 ].getSize().clone();
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < minSize.length; d++ )
				if (minSize[ d ] > tile.getSize( d ))
					minSize[ d ] = tile.getSize( d );
		return minSize;
	}
}
