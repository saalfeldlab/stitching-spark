package org.janelia.flatfield;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.histogram.Histogram;
import org.janelia.histogram.HistogramMatching;

import mpicbg.models.Affine1D;
import mpicbg.models.AffineModel1D;
import mpicbg.models.FixedScalingAffineModel1D;
import mpicbg.models.FixedTranslationAffineModel1D;
import mpicbg.models.IdentityModel;
import mpicbg.models.IndependentlyInterpolatedAffineModel1D;
import mpicbg.models.InterpolatedAffineModel1D;
import mpicbg.models.InvertibleBoundable;
import mpicbg.models.Model;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.RandomAccessiblePair;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;
import scala.Tuple2;

public class FlatfieldCorrectionSolver implements Serializable
{
	private static final long serialVersionUID = -5761317703704915380L;

	public static enum ModelType
	{
		AffineModel,
		FixedScalingAffineModel,
		FixedTranslationAffineModel
	}
	public static enum RegularizerModelType
	{
		AffineModel,
		IdentityModel
	}

	private static final double INTERPOLATION_LAMBDA_IDENTITY = 0.0;
	private static final double INTERPOLATION_LAMBDA_PIVOT = 0.5;

	private static final double INTERPOLATION_LAMBDA_SCALING = 0.5;
	private static final double INTERPOLATION_LAMBDA_TRANSLATION = 0.5;

	private transient final JavaSparkContext sparkContext;

	public FlatfieldCorrectionSolver( final JavaSparkContext sparkContext )
	{
		this.sparkContext = sparkContext;
	}



	public static class FlatfieldSolution
	{
		public final Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > correctionFields;
		public final RandomAccessibleInterval< DoubleType > pivotValues;

		public FlatfieldSolution(
				final Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > correctionFields,
				final RandomAccessibleInterval< DoubleType > pivotValues )
		{
			this.correctionFields = correctionFields;
			this.pivotValues = pivotValues;
		}
	}
	@SuppressWarnings("unchecked")
	public < M extends Model< M > & Affine1D< M >, R extends Model< R > & Affine1D< R > & InvertibleBoundable > FlatfieldSolution leastSquaresInterpolationFit(
			final JavaPairRDD< Long, Histogram > rddHistograms,
			final Histogram referenceHistogram,
			final ShiftedDownsampling< ? >.PixelsMapping pixelsMapping,
			final RandomAccessiblePairNullable< DoubleType, DoubleType > regularizer,
			final ModelType modelType,
			final RegularizerModelType regularizerModelType,
			final double pivotValue )
	{
		System.out.println( "Solving for scale " + pixelsMapping.scale + ":  size=" + Arrays.toString( pixelsMapping.getDimensions() ) + ",  model=" + modelType.toString() + ", regularizer=" + regularizerModelType.toString() );

		final Broadcast< RandomAccessiblePairNullable< DoubleType, DoubleType > > broadcastedRegularizer = sparkContext.broadcast( regularizer );

		final long[] size = pixelsMapping.getDimensions();

//		final double referenceHistogramOffset = getMedianValue( referenceHistogram );
		final double referenceHistogramOffset = pivotValue;

		final JavaPairRDD< Long, Pair< Pair< Double, Double >, Double > > rddSolutionPixels = rddHistograms.mapToPair( tuple ->
			{
				final long[] position = new long[ size.length ];
				IntervalIndexer.indexToPosition( tuple._1(), size, position );

				final List< PointMatch > matches = HistogramMatching.generateHistogramMatches( tuple._2(), referenceHistogram );

				// apply the offsets to the pointmatch values
//				final double[] offset = new double[] { getMedianValue( tuple._2() ), referenceHistogramOffset };
				final double[] offset = new double[] { pivotValue, referenceHistogramOffset };
				for ( final PointMatch match : matches )
				{
					final Point[] points = new Point[] { match.getP1(), match.getP2() };
					for ( int i = 0; i < 2; ++i )
						for ( final double[] value : new double[][] { points[ i ].getL(), points[ i ].getW() } )
							value[ 0 ] -= offset[ i ];
				}

				final double[] regularizerValues;
				if ( broadcastedRegularizer.value() != null )
				{
					final RandomAccessiblePairNullable< DoubleType, DoubleType >.RandomAccess regularizerRandomAccess = broadcastedRegularizer.value().randomAccess();
					regularizerRandomAccess.setPosition( position );

					regularizerValues = new double[]
						{
							( regularizerRandomAccess.getA() != null ? regularizerRandomAccess.getA().get() : 1 ),
							( regularizerRandomAccess.getB() != null ? regularizerRandomAccess.getB().get() : 0 )
						};
				}
				else
				{
					regularizerValues = null;
				}

				final M model;
				switch ( modelType )
				{
				case AffineModel:
					model = ( M ) new AffineModel1D();
					break;
				case FixedTranslationAffineModel:
					model = ( M ) new FixedTranslationAffineModel1D( regularizerValues == null ? 0 : regularizerValues[ 1 ] );
					break;
				case FixedScalingAffineModel:
					model = ( M ) new FixedScalingAffineModel1D( regularizerValues == null ? 1 : regularizerValues[ 0 ] );
					break;
				default:
					model = null;
					break;
				}

				final M pivotedModel = ( M ) new InterpolatedAffineModel1D<>( model, new FixedTranslationAffineModel1D( 0 ), INTERPOLATION_LAMBDA_PIVOT );

				boolean modelFound = false;
				try
				{
//					modelFound = model.filter( matches, new ArrayList<>(), 4.0 );
					pivotedModel.fit( matches );
					modelFound = true;
				}
				catch ( final Exception e )
				{
//					modelFound = false;
//					e.printStackTrace();
					throw e;
				}


				final R regularizerModel;
				switch ( regularizerModelType )
				{
				case IdentityModel:
					regularizerModel = ( R ) new IdentityModel();
					break;
				case AffineModel:
					final AffineModel1D downsampledModel = new AffineModel1D();
					downsampledModel.set(
							regularizerValues != null ? regularizerValues[ 0 ] : 1,
							regularizerValues != null ? regularizerValues[ 1 ] : 0 );
					regularizerModel = ( R ) downsampledModel;
					break;
				default:
					regularizerModel = null;
					break;
				}

//				final R interpolatedRegularizer = ( R ) new InterpolatedAffineModel1D<>(
//						regularizerModel,
//						new IdentityModel(),
//						INTERPOLATION_LAMBDA_IDENTITY );
//
//				final M interpolatedModel = ( M ) ( modelFound ?
//						new IndependentlyInterpolatedAffineModel1D<>(
//								pivotedModel,
//								interpolatedRegularizer,
//								INTERPOLATION_LAMBDA_SCALING,
//								INTERPOLATION_LAMBDA_TRANSLATION ) :
//						interpolatedRegularizer );

				final M interpolatedModel = ( M ) ( modelFound ?
						new IndependentlyInterpolatedAffineModel1D<>(
								pivotedModel,
								regularizerModel,
								INTERPOLATION_LAMBDA_SCALING,
								INTERPOLATION_LAMBDA_TRANSLATION ) :
							regularizerModel );

				final double[] estimatedModelValues = new double[ 2 ];
				interpolatedModel.toArray( estimatedModelValues );

				return new Tuple2<>( tuple._1(), new ValuePair<>( new ValuePair<>( estimatedModelValues[ 0 ], estimatedModelValues[ 1 ] ), offset[ 0 ] ) );
			} );

		final List< Tuple2< Long, Pair< Pair< Double, Double >, Double > > > solutionPixels  = rddSolutionPixels.collect();

		broadcastedRegularizer.destroy();

		final Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > solution = new ValuePair<>( ArrayImgs.doubles( size ), ArrayImgs.doubles( size ) );
		final RandomAccessiblePair< DoubleType, DoubleType >.RandomAccess solutionRandomAccess = new RandomAccessiblePair<>( solution.getA(), solution.getB() ).randomAccess();
		final long[] position = new long[ size.length ];
		for ( final Tuple2< Long, Pair< Pair< Double, Double >, Double > > tuple : solutionPixels )
		{
			IntervalIndexer.indexToPosition( tuple._1(), size, position );
			solutionRandomAccess.setPosition( position );

			solutionRandomAccess.getA().set( tuple._2().getA().getA() );
			solutionRandomAccess.getB().set( tuple._2().getA().getB() );
		}

		final double[] offsets = new double[ solutionPixels.size() ];
		for ( final Tuple2< Long, Pair< Pair< Double, Double >, Double > > tuple : solutionPixels )
			offsets[ tuple._1().intValue() ] = tuple._2().getB();
		final RandomAccessibleInterval< DoubleType > offsetsImg = ArrayImgs.doubles( offsets, size );

		return new FlatfieldSolution( solution, offsetsImg );
	}


	public static Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > unpivotSolution( final FlatfieldSolution solution )
	{
		// account for the pivot point in the final solution
		final RandomAccessibleInterval< DoubleType > srcScaling= solution.correctionFields.getA(), srcTranslation = solution.correctionFields.getB();
		final RandomAccessibleInterval< DoubleType > dstScaling = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( srcScaling ) ), dstTranslation = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( srcTranslation ) );
		final Cursor< DoubleType > srcScalingCursor = Views.flatIterable( srcScaling ).cursor(), srcTranslationCursor = Views.flatIterable( srcTranslation ).cursor();
		final Cursor< DoubleType > dstScalingCursor = Views.flatIterable( dstScaling ).cursor(), dstTranslationCursor = Views.flatIterable( dstTranslation ).cursor();
		final Cursor< DoubleType > cursorOffsets = Views.flatIterable( solution.pivotValues ).cursor();
		final AffineModel1D model = new AffineModel1D(), offset = new AffineModel1D(), translation = new AffineModel1D();
		while ( srcScalingCursor.hasNext() || srcTranslationCursor.hasNext() || dstScalingCursor.hasNext() || dstTranslationCursor.hasNext() || cursorOffsets.hasNext() )
		{
			final double scalingVal = srcScalingCursor.next().get(), translationVal = srcTranslationCursor.next().get(), shift = cursorOffsets.next().get();

			model.set( scalingVal, translationVal );
			offset.set( 1, -shift );
			translation.set( 1, shift );

			model.concatenate( offset );
			model.preConcatenate( translation );

			final double[] m = new double[ 2 ];
			model.toArray( m );

			dstScalingCursor.next().set( m[ 0 ] );
			dstTranslationCursor.next().set( m[ 1 ] );
		}
		return new ValuePair<>( dstScaling, dstTranslation );
	}


	private double getMedianValue( final Histogram histogram )
	{
		double quantityProcessed = 0;
		for ( int i = 0; i < histogram.getNumBins(); ++i )
		{
			quantityProcessed += histogram.get( i );
			if ( quantityProcessed >= histogram.getQuantityTotal() / 2 )
				return histogram.getBinValue( i );
		}
		return 0;
	}
}
