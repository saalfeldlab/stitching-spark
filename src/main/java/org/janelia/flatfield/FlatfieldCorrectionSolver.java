package org.janelia.flatfield;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.histogram.Histogram;
import org.janelia.histogram.HistogramsMatching;

import mpicbg.models.Affine1D;
import mpicbg.models.AffineModel1D;
import mpicbg.models.ConstantAffineModel1D;
import mpicbg.models.FixedScalingAffineModel1D;
import mpicbg.models.FixedTranslationAffineModel1D;
import mpicbg.models.IdentityModel;
import mpicbg.models.IndependentlyInterpolatedAffineModel1D;
import mpicbg.models.InterpolatedAffineModel1D;
import mpicbg.models.InvertibleBoundable;
import mpicbg.models.Model;
import mpicbg.models.PointMatch;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.RandomAccessiblePair;
import net.imglib2.view.RandomAccessiblePairNullable;
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

	private static final double INTERPOLATION_LAMBDA_IDENTITY = 0.1;
	private static final double INTERPOLATION_LAMBDA_SCALING = 0.5;
	private static final double INTERPOLATION_LAMBDA_TRANSLATION = 0.0;

	private transient final JavaSparkContext sparkContext;

	public FlatfieldCorrectionSolver( final JavaSparkContext sparkContext )
	{
		this.sparkContext = sparkContext;
	}

	@SuppressWarnings("unchecked")
	public < M extends Model< M > & Affine1D< M >, R extends Model< R > & Affine1D< R > & InvertibleBoundable >
	Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > leastSquaresInterpolationFit(
			final JavaPairRDD< Long, Histogram > rddHistograms,
			final Histogram referenceHistogram,
			final ShiftedDownsampling< ? >.PixelsMapping pixelsMapping,
			final RandomAccessiblePairNullable< DoubleType, DoubleType > regularizer,
			final ModelType modelType,
			final RegularizerModelType regularizerModelType )
	{
		System.out.println( "Solving for scale " + pixelsMapping.scale + ":  size=" + Arrays.toString( pixelsMapping.getDimensions() ) + ",  model=" + modelType.toString() + ", regularizer=" + regularizerModelType.toString() );

		final Broadcast< RandomAccessiblePairNullable< DoubleType, DoubleType > > broadcastedRegularizer = sparkContext.broadcast( regularizer );

		final long[] size = pixelsMapping.getDimensions();

		final JavaPairRDD< Long, Pair< Double, Double > > rddSolutionPixels = rddHistograms.mapToPair( tuple ->
			{
				final long[] position = new long[ size.length ];
				IntervalIndexer.indexToPosition( tuple._1(), size, position );

				final List< PointMatch > matches = HistogramsMatching.generateHistogramMatches( tuple._2(), referenceHistogram );

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

				boolean modelFound = false;
				try
				{
//					modelFound = model.filter( matches, new ArrayList<>(), 4.0 );
					model.fit( matches );
					modelFound = true;
				}
				catch ( final Exception e )
				{
					modelFound = false;
					e.printStackTrace();
				}

				final R interpolatedModel = modelFound ?
						( R ) new IndependentlyInterpolatedAffineModel1D<>(
								model,
								new ConstantAffineModel1D<>( regularizerModel ),
								INTERPOLATION_LAMBDA_SCALING,
								INTERPOLATION_LAMBDA_TRANSLATION ) :
						regularizerModel;

				final Affine1D< ? > identityInterpolatedModel = new InterpolatedAffineModel1D<>(
								interpolatedModel,
								new ConstantAffineModel1D<>( new IdentityModel() ),
								INTERPOLATION_LAMBDA_IDENTITY );

				final double[] estimatedModelValues = new double[ 2 ];
				identityInterpolatedModel.toArray( estimatedModelValues );

				return new Tuple2<>( tuple._1(), new ValuePair<>( estimatedModelValues[ 0 ], estimatedModelValues[ 1 ] ) );
			} );

		final List< Tuple2< Long, Pair< Double, Double > > > solutionPixels  = rddSolutionPixels.collect();

		broadcastedRegularizer.destroy();

		final Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > solution = new ValuePair<>( ArrayImgs.doubles( size ), ArrayImgs.doubles( size ) );
		final RandomAccessiblePair< DoubleType, DoubleType >.RandomAccess solutionRandomAccess = new RandomAccessiblePair<>( solution.getA(), solution.getB() ).randomAccess();
		final long[] position = new long[ size.length ];
		for ( final Tuple2< Long, Pair< Double, Double > > tuple : solutionPixels )
		{
			IntervalIndexer.indexToPosition( tuple._1(), size, position );
			solutionRandomAccess.setPosition( position );

			solutionRandomAccess.getA().set( tuple._2().getA() );
			solutionRandomAccess.getB().set( tuple._2().getB() );
		}

		return solution;
	}
}
