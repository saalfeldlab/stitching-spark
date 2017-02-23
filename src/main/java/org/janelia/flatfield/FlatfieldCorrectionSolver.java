package org.janelia.flatfield;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import mpicbg.models.Affine1D;
import mpicbg.models.AffineModel1D;
import mpicbg.models.ConstantAffineModel1D;
import mpicbg.models.FixedScalingAffineModel1D;
import mpicbg.models.FixedTranslationAffineModel1D;
import mpicbg.models.IdentityModel;
import mpicbg.models.InterpolatedAffineModel1D;
import mpicbg.models.InvertibleBoundable;
import mpicbg.models.Model;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
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

	private static final double INTERPOLATION_LAMBDA_V = 0.5;
	private static final double INTERPOLATION_LAMBDA_Z = 0.0;

	private transient final JavaSparkContext sparkContext;

	public FlatfieldCorrectionSolver( final JavaSparkContext sparkContext )
	{
		this.sparkContext = sparkContext;
	}

	@SuppressWarnings("unchecked")
	public <
		A extends ArrayImg< DoubleType, DoubleArray >,
		M extends Model< M > & Affine1D< M >,
		R extends Model< R > & Affine1D< R > & InvertibleBoundable >
	Pair< A, A > leastSquaresInterpolationFit(
			final JavaPairRDD< Long, long[] > rddHistograms,
			final long[] referenceHistogram,
			final HistogramSettings histogramSettings,
			final ShiftedDownsampling.PixelsMapping pixelsMapping,
			final RandomAccessiblePairNullable< DoubleType, DoubleType > regularizer,
			final ModelType modelType,
			final RegularizerModelType regularizerModelType )
	{
		final Broadcast< RandomAccessiblePairNullable< DoubleType, DoubleType > > broadcastedRegularizer = sparkContext.broadcast( regularizer );
		final Broadcast< int[] > broadcastedDownsampledPixelToFullPixelsCount = pixelsMapping.broadcastedDownsampledPixelToFullPixelsCount;

		final long[] size = pixelsMapping.getDimensions();

		final JavaPairRDD< Long, Pair< Double, Double > > rddSolutionPixels = rddHistograms.mapToPair( tuple ->
			{
				final long[] histogram = tuple._2();

				final long[] position = new long[ size.length ];
				IntervalIndexer.indexToPosition( tuple._1(), size, position );

				// accumulated histograms have N*(number of aggregated full-scale pixels) items,
				// so we need to compensate for that by multuplying reference histogram values by the same amount
				final int referenceHistogramMultiplier;
				if ( broadcastedDownsampledPixelToFullPixelsCount != null )
					referenceHistogramMultiplier = broadcastedDownsampledPixelToFullPixelsCount.value()[ tuple._1().intValue() ];
				else
					referenceHistogramMultiplier = 1;

				final List< PointMatch > matches = generateHistogramMatches( histogramSettings, histogram, referenceHistogram, referenceHistogramMultiplier );

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

				final Affine1D< ? > interpolatedModel = modelFound ?
						new InterpolatedAffineModel1D<>(
								model,
								new ConstantAffineModel1D<>( regularizerModel ),
								modelType == ModelType.FixedTranslationAffineModel ? INTERPOLATION_LAMBDA_V : INTERPOLATION_LAMBDA_Z ) :
						regularizerModel;

				final double[] estimatedModelValues = new double[ 2 ];
				interpolatedModel.toArray( estimatedModelValues );

				return new Tuple2<>( tuple._1(), new ValuePair<>( estimatedModelValues[ 0 ], estimatedModelValues[ 1 ] ) );
			} );

		final List< Tuple2< Long, Pair< Double, Double > > > solutionPixels  = rddSolutionPixels.collect();

		broadcastedRegularizer.destroy();

		final Pair< A, A > solution = ( Pair< A, A > ) new ValuePair<>( ArrayImgs.doubles( size ), ArrayImgs.doubles( size ) );
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


	public static List< PointMatch > generateHistogramMatches(
			final HistogramSettings histogramSettings,
			final long[] histogram,
			final long[] referenceHistogram,
			final long referenceHistogramMultiplier )
	{
		if ( referenceHistogramMultiplier <= 0 )
			throw new IllegalArgumentException( "Incorrect value: referenceHistogramMultiplier="+referenceHistogramMultiplier );

		// number of elements should match in both histograms (accounting for the multiplier)
		long histogramElements = 0, referenceHistogramElements = 0;
		for ( int i = 0; i < histogramSettings.bins; i++ )
		{
			histogramElements += histogram[ i ];
			referenceHistogramElements += referenceHistogram[ i ] * referenceHistogramMultiplier;
		}
		if ( histogramElements != referenceHistogramElements )
			throw new IllegalArgumentException( "Number of elements doesn't match: histogramElements="+histogramElements+", referenceHistogramElements="+referenceHistogramElements);

		final List< PointMatch > matches = new ArrayList<>();
		long histogramValue = 0, referenceHistogramValue = 0;
		for ( int histogramIndex = -1, referenceHistogramIndex = -1; ; )
		{
			while ( histogramValue == 0 && histogramIndex < histogramSettings.bins - 1 ) histogramValue = histogram[ ++histogramIndex ];
			while ( referenceHistogramValue == 0 && referenceHistogramIndex < histogramSettings.bins - 1 ) referenceHistogramValue = referenceHistogram[ ++referenceHistogramIndex ] * referenceHistogramMultiplier;

			// if at least one of them remains 0, it means that we have reached the boundary
			if ( histogramValue == 0 || referenceHistogramValue == 0 )
				break;

			final long weight = Math.min( histogramValue, referenceHistogramValue );

			// ignore the first and the last bin because they presumably contain undersaturated/oversaturated values
			if ( histogramIndex > 0 && histogramIndex < histogramSettings.bins - 1 && referenceHistogramIndex > 0 && referenceHistogramIndex < histogramSettings.bins - 1 )
				matches.add(
						new PointMatch(
								new Point( new double[] { histogramSettings.getBinValue( histogramIndex ) } ),
								new Point( new double[] { histogramSettings.getBinValue( referenceHistogramIndex ) } ),
								weight )
						);

			histogramValue -= weight;
			referenceHistogramValue -= weight;
		}
		return matches;
	}
}
