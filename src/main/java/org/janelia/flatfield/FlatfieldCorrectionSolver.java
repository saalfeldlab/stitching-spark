package org.janelia.flatfield;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.histogram.Histogram;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.N5RemoveSpark;

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
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.list.WrappedListImg;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

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

	public static class FlatfieldSolutionMetadata implements Serializable
	{
		private static final long serialVersionUID = -4110180561225910592L;

		public final String scalingTermDataset;
		public final String translationTermDataset;
		public final String pivotValuesDataset;

		public FlatfieldSolutionMetadata( final int scaleLevel )
		{
			scalingTermDataset = PathResolver.get( INTERMEDIATE_EXPORTS_N5_GROUP, SCALING_TERM_GROUP, "s" + scaleLevel );
			translationTermDataset = PathResolver.get( INTERMEDIATE_EXPORTS_N5_GROUP, TRANSLATION_TERM_GROUP, "s" + scaleLevel );
			pivotValuesDataset = PathResolver.get( INTERMEDIATE_EXPORTS_N5_GROUP, PIVOT_VALUES_GROUP, "s" + scaleLevel );
		}

		public FlatfieldSolution open( final DataProvider dataProvider, final String histogramsN5BasePath ) throws IOException
		{
			final N5Reader n5 = dataProvider.createN5Reader( URI.create( histogramsN5BasePath ) );
			return new FlatfieldSolution(
					new ValuePair<>( N5Utils.open( n5, scalingTermDataset ), N5Utils.open( n5, translationTermDataset ) ),
					N5Utils.open( n5, pivotValuesDataset )
				);
		}
	}

	private static final double INTERPOLATION_LAMBDA_IDENTITY = 0.0;
	private static final double INTERPOLATION_LAMBDA_PIVOT = 0.5;

	private static final double INTERPOLATION_LAMBDA_SCALING = 0.5;
	private static final double INTERPOLATION_LAMBDA_TRANSLATION = 0.5;

	private static final String INTERMEDIATE_EXPORTS_N5_GROUP = "flatfield-intermediate-exports";
	private static final String SCALING_TERM_GROUP = "flatfield-solution-S";
	private static final String TRANSLATION_TERM_GROUP = "flatfield-solution-T";
	private static final String PIVOT_VALUES_GROUP = "flatfield-solution-pivot";

	private transient final JavaSparkContext sparkContext;

	public FlatfieldCorrectionSolver( final JavaSparkContext sparkContext )
	{
		this.sparkContext = sparkContext;
	}

	@SuppressWarnings("unchecked")
	public < M extends Model< M > & Affine1D< M >, R extends Model< R > & Affine1D< R > & InvertibleBoundable > FlatfieldSolutionMetadata leastSquaresInterpolationFit(
			final DataProvider dataProvider,
			final int currentScaleLevel,
			final String histogramsN5BasePath, final String currentScaleHistogramsDataset,
			final Histogram referenceHistogram,
			final RandomAccessiblePairNullable< DoubleType, DoubleType > regularizer,
			final ModelType modelType, final RegularizerModelType regularizerModelType,
			final double pivotValue ) throws IOException
	{
		final Broadcast< RandomAccessiblePairNullable< DoubleType, DoubleType > > broadcastedRegularizer = sparkContext.broadcast( regularizer );

//		final double referenceHistogramOffset = getMedianValue( referenceHistogram );
		final double referenceHistogramOffset = pivotValue;

		final DataAccessType dataAccessType = dataProvider.getType();
		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		final DatasetAttributes currentScaleHistogramsDatasetAttributes = n5.getDatasetAttributes( currentScaleHistogramsDataset );
		final long[] currentScaleHistogramsDimensions = currentScaleHistogramsDatasetAttributes.getDimensions();
		final int[] currentScaleHistogramsBlockSize = currentScaleHistogramsDatasetAttributes.getBlockSize();

		final FlatfieldSolutionMetadata flatfieldSolutionMetadata = new FlatfieldSolutionMetadata( currentScaleLevel );
		n5.createDataset( flatfieldSolutionMetadata.scalingTermDataset, currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize, DataType.FLOAT64, currentScaleHistogramsDatasetAttributes.getCompression() );
		n5.createDataset( flatfieldSolutionMetadata.translationTermDataset, currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize, DataType.FLOAT64, currentScaleHistogramsDatasetAttributes.getCompression() );
		n5.createDataset( flatfieldSolutionMetadata.pivotValuesDataset, currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize, DataType.FLOAT64, currentScaleHistogramsDatasetAttributes.getCompression() );

		final List< long[] > currentScaleBlockPositions = HistogramsProvider.getBlockPositions( currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize );
		sparkContext.parallelize( currentScaleBlockPositions, currentScaleBlockPositions.size() ).foreach( currentScaleBlockPosition ->
			{
				final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataAccessType );
				final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsN5BasePath ) );

				// histogram block reader
				final WrappedSerializableDataBlockReader< Histogram > histogramsBlock = new WrappedSerializableDataBlockReader<>(
						n5Local,
						currentScaleHistogramsDataset,
						currentScaleBlockPosition
					);
				final WrappedListImg< Histogram > histogramsBlockImg = histogramsBlock.wrap();

				// solution data blocks
				final RandomAccessibleInterval< DoubleType > scalingTermBlockImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( histogramsBlockImg ) );
				final RandomAccessibleInterval< DoubleType > translationTermBlockImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( histogramsBlockImg ) );
				final RandomAccessibleInterval< DoubleType > pivotValuesBlockImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( histogramsBlockImg ) );

				final CellGrid cellGrid = new CellGrid( currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize );
				final long[] cellMin = new long[ cellGrid.numDimensions() ];
				final int[] cellDimensions = new int[ cellGrid.numDimensions() ];
				cellGrid.getCellDimensions( currentScaleBlockPosition, cellMin, cellDimensions );

				final IntervalView< Histogram > translatedHistogramsBlockImg = Views.translate( histogramsBlockImg, cellMin );
				final IntervalView< DoubleType > translatedScalingTermBlockImg = Views.translate( scalingTermBlockImg, cellMin );
				final IntervalView< DoubleType > translatedTranslationTermBlockImg = Views.translate( translationTermBlockImg, cellMin );
				final IntervalView< DoubleType > translatedPivotValuesBlockImg = Views.translate( pivotValuesBlockImg, cellMin );

				final Cursor< Histogram > translatedHistogramsBlockImgCursor = Views.flatIterable( translatedHistogramsBlockImg ).localizingCursor();
				final Cursor< DoubleType > translatedScalingTermBlockImgCursor = Views.flatIterable( translatedScalingTermBlockImg ).cursor();
				final Cursor< DoubleType > translatedTranslationTermBlockImgCursor = Views.flatIterable( translatedTranslationTermBlockImg ).cursor();
				final Cursor< DoubleType > translatedPivotValuesBlockImgCursor = Views.flatIterable( translatedPivotValuesBlockImg ).cursor();

				final long[] position = new long[ cellGrid.numDimensions() ];
				while ( translatedHistogramsBlockImgCursor.hasNext() )
				{
					final Histogram histogram = translatedHistogramsBlockImgCursor.next();
					translatedHistogramsBlockImgCursor.localize( position );

					translatedScalingTermBlockImgCursor.fwd();
					translatedTranslationTermBlockImgCursor.fwd();
					translatedPivotValuesBlockImgCursor.fwd();

					final List< PointMatch > matches = HistogramMatching.generateHistogramMatches( histogram, referenceHistogram );

					// apply the offsets to the pointmatch values
//					final double[] offset = new double[] { getMedianValue( tuple._2() ), referenceHistogramOffset };
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
//						modelFound = model.filter( matches, new ArrayList<>(), 4.0 );
						pivotedModel.fit( matches );
						modelFound = true;
					}
					catch ( final Exception e )
					{
//						modelFound = false;
//						e.printStackTrace();
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

//					final R interpolatedRegularizer = ( R ) new InterpolatedAffineModel1D<>(
//							regularizerModel,
//							new IdentityModel(),
//							INTERPOLATION_LAMBDA_IDENTITY );
	//
//					final M interpolatedModel = ( M ) ( modelFound ?
//							new IndependentlyInterpolatedAffineModel1D<>(
//									pivotedModel,
//									interpolatedRegularizer,
//									INTERPOLATION_LAMBDA_SCALING,
//									INTERPOLATION_LAMBDA_TRANSLATION ) :
//							interpolatedRegularizer );

					final M interpolatedModel = ( M ) ( modelFound ?
							new IndependentlyInterpolatedAffineModel1D<>(
									pivotedModel,
									regularizerModel,
									INTERPOLATION_LAMBDA_SCALING,
									INTERPOLATION_LAMBDA_TRANSLATION ) :
								regularizerModel );

					final double[] estimatedModelValues = new double[ 2 ];
					interpolatedModel.toArray( estimatedModelValues );

					translatedScalingTermBlockImgCursor.get().set( estimatedModelValues[ 0 ] );
					translatedTranslationTermBlockImgCursor.get().set( estimatedModelValues[ 1 ] );
					translatedPivotValuesBlockImgCursor.get().set( offset[ 0 ] );
				}

				N5Utils.saveBlock( scalingTermBlockImg, n5Local, flatfieldSolutionMetadata.scalingTermDataset, currentScaleBlockPosition );
				N5Utils.saveBlock( translationTermBlockImg, n5Local, flatfieldSolutionMetadata.translationTermDataset, currentScaleBlockPosition );
				N5Utils.saveBlock( pivotValuesBlockImg, n5Local, flatfieldSolutionMetadata.pivotValuesDataset, currentScaleBlockPosition );
			} );

		broadcastedRegularizer.destroy();

		return flatfieldSolutionMetadata;
	}

	public void cleanupFlatfieldSolutionExports( final DataProvider dataProvider, final String histogramsN5BasePath ) throws IOException
	{
		final DataAccessType dataAccessType = dataProvider.getType();
		N5RemoveSpark.remove(
				sparkContext,
				() -> DataProviderFactory.createByType( dataAccessType ).createN5Writer( URI.create( histogramsN5BasePath ) ),
				INTERMEDIATE_EXPORTS_N5_GROUP
			);
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
