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
import org.janelia.flatfield.FlatfieldCorrectionSolver.FlatfieldRegularizerMetadata.RegularizerMode;
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
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.ConstantUtils;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;

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

		public final String scalingTermDataset, translationTermDataset;
//		public final String pivotValuesDataset;

		private double pivotValue;

		public FlatfieldSolutionMetadata( final int scaleLevel, final double pivotValue )
		{
			scalingTermDataset = PathResolver.get( INTERMEDIATE_EXPORTS_N5_GROUP, SCALING_TERM_GROUP, "s" + scaleLevel );
			translationTermDataset = PathResolver.get( INTERMEDIATE_EXPORTS_N5_GROUP, TRANSLATION_TERM_GROUP, "s" + scaleLevel );
//			pivotValuesDataset = PathResolver.get( INTERMEDIATE_EXPORTS_N5_GROUP, PIVOT_VALUES_GROUP, "s" + scaleLevel );
			this.pivotValue = pivotValue;
		}

		public FlatfieldSolutionMetadata( final String scalingTermDataset, final String translationTermDataset, final double pivotValue )
		{
			this.scalingTermDataset = scalingTermDataset;
			this.translationTermDataset = translationTermDataset;
			this.pivotValue = pivotValue;
		}

		public FlatfieldSolution open( final DataProvider dataProvider, final String histogramsN5BasePath ) throws IOException
		{
			final N5Reader n5 = dataProvider.createN5Reader( URI.create( histogramsN5BasePath ) );
			final Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > correctionFields = new ValuePair<>(
					N5Utils.open( n5, scalingTermDataset ),
					N5Utils.open( n5, translationTermDataset )
				);
			final RandomAccessibleInterval< DoubleType > pivotValuesImg = ConstantUtils.constantRandomAccessibleInterval(
					new DoubleType( pivotValue ),
					correctionFields.getA().numDimensions(),
					correctionFields.getA()
				);
//			N5Utils.open( n5, pivotValuesDataset )
			return new FlatfieldSolution( correctionFields, pivotValuesImg );
		}
	}

	public static class FlatfieldRegularizerMetadata implements Serializable
	{
		private static final long serialVersionUID = 3764292044450927660L;

		public static enum RegularizerMode
		{
			UPSAMPLE_CURRENT_SOLUTION,
			DOWNSAMPLE_PREVIOUS_SOLUTION
		}

		public final String scalingRegularizerDataset, translationRegularizerDataset;
		public final RegularizerMode scalingRegularizerMode, translationRegularizerMode;

		public FlatfieldRegularizerMetadata(
				final String scalingRegularizerDataset, final String translationRegularizerDataset,
				final RegularizerMode scalingRegularizerMode, final RegularizerMode translationRegularizerMode )
		{
			this.scalingRegularizerDataset = scalingRegularizerDataset;
			this.translationRegularizerDataset = translationRegularizerDataset;
			this.scalingRegularizerMode = scalingRegularizerMode;
			this.translationRegularizerMode = translationRegularizerMode;
		}
	}

	private static final double INTERPOLATION_LAMBDA_IDENTITY = 0.0;
	private static final double INTERPOLATION_LAMBDA_PIVOT = 0.5;

	private static final double INTERPOLATION_LAMBDA_SCALING = 0.5;
	private static final double INTERPOLATION_LAMBDA_TRANSLATION = 0.5;

	private static final String INTERMEDIATE_EXPORTS_N5_GROUP = "flatfield-intermediate-exports";
	private static final String SCALING_TERM_GROUP = "flatfield-solution-S";
	private static final String TRANSLATION_TERM_GROUP = "flatfield-solution-T";
//	private static final String PIVOT_VALUES_GROUP = "flatfield-solution-pivot";

	private transient final JavaSparkContext sparkContext;
	private final HistogramsProvider histogramsProvider;

	public FlatfieldCorrectionSolver( final JavaSparkContext sparkContext, final HistogramsProvider histogramsProvider )
	{
		this.sparkContext = sparkContext;
		this.histogramsProvider = histogramsProvider;
	}

	@SuppressWarnings( "unchecked" )
	public < T extends RealType< T >, M extends Model< M > & Affine1D< M >, R extends Model< R > & Affine1D< R > & InvertibleBoundable > FlatfieldSolutionMetadata leastSquaresInterpolationFit(
			final int currentScaleLevel,
			final String currentScaleHistogramsDataset,
			final FlatfieldRegularizerMetadata regularizerMetadata,
			final ShiftedDownsampling< ? > shiftedDownsampling,
			final ModelType modelType, final RegularizerModelType regularizerModelType,
			final double pivotValue ) throws IOException
	{
//		final double referenceHistogramOffset = getMedianValue( referenceHistogram );
		final double referenceHistogramOffset = pivotValue;

		final double[] binValues = HistogramMatching.getBinValues(
				histogramsProvider.getHistogramMinValue(),
				histogramsProvider.getHistogramMaxValue(),
				histogramsProvider.getHistogramBins()
			);

		// TODO: make shifted downsampling Serializable (currently there is a required non-serializable field for affine transformation)
		final Broadcast< ShiftedDownsampling< ? > > broadcastedShiftedDownsampling = sparkContext.broadcast( shiftedDownsampling );

		final N5Writer n5 = histogramsProvider.getDataProvider().createN5Writer( URI.create( histogramsProvider.getHistogramsN5BasePath() ) );
		final DatasetAttributes currentScaleHistogramsDatasetAttributes = n5.getDatasetAttributes( currentScaleHistogramsDataset );
		final long[] currentScaleHistogramsExtendedDimensions = currentScaleHistogramsDatasetAttributes.getDimensions();
		final int[] currentScaleHistogramsExtendedBlockSize = currentScaleHistogramsDatasetAttributes.getBlockSize();

		final long[] currentScaleHistogramsDimensions = new long[ currentScaleHistogramsExtendedDimensions.length - 1 ];
		System.arraycopy( currentScaleHistogramsExtendedDimensions, 0, currentScaleHistogramsDimensions, 0, currentScaleHistogramsDimensions.length );
		final int[] currentScaleHistogramsBlockSize = new int[ currentScaleHistogramsExtendedBlockSize.length - 1 ];
		System.arraycopy( currentScaleHistogramsExtendedBlockSize, 0, currentScaleHistogramsBlockSize, 0, currentScaleHistogramsBlockSize.length );

		final FlatfieldSolutionMetadata solutionMetadata = new FlatfieldSolutionMetadata( currentScaleLevel, pivotValue );
		n5.createDataset( solutionMetadata.scalingTermDataset, currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize, DataType.FLOAT64, currentScaleHistogramsDatasetAttributes.getCompression() );
		n5.createDataset( solutionMetadata.translationTermDataset, currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize, DataType.FLOAT64, currentScaleHistogramsDatasetAttributes.getCompression() );
//		n5.createDataset( flatfieldSolutionMetadata.pivotValuesDataset, currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize, DataType.FLOAT64, currentScaleHistogramsDatasetAttributes.getCompression() );

		final List< long[] > currentScaleBlockPositions = HistogramsProvider.getBlockPositions( currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize );
		sparkContext.parallelize( currentScaleBlockPositions, currentScaleBlockPositions.size() ).foreach( blockPosition ->
			{
				final CellGrid cellGrid = new CellGrid( currentScaleHistogramsDimensions, currentScaleHistogramsBlockSize );
				final long[] cellMin = new long[ cellGrid.numDimensions() ], cellMax = new long[ cellGrid.numDimensions() ];
				final int[] cellDimensions = new int[ cellGrid.numDimensions() ];
				cellGrid.getCellDimensions( blockPosition, cellMin, cellDimensions );
				for ( int d = 0; d < cellGrid.numDimensions(); ++d )
					cellMax[ d ] = cellMin[ d ] + cellDimensions[ d ] - 1;
				final Interval cellInterval = new FinalInterval( cellMin, cellMax );

				// open histograms dataset
				final DataProvider dataProviderLocal = DataProviderFactory.createByType( histogramsProvider.getDataAccessType() );
				final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsProvider.getHistogramsN5BasePath() ) );
				final RandomAccessibleInterval< T > histogramsStorageImg = ( RandomAccessibleInterval< T > ) N5Utils.open( n5Local, currentScaleHistogramsDataset );
				final CompositeIntervalView< T, RealComposite< T > > histogramsImg = Views.collapseReal( histogramsStorageImg );
				final IntervalView< RealComposite< T > > histogramsBlockImg = Views.interval( histogramsImg, cellInterval );

				// open regularizer datasets
				final RandomAccessiblePairNullable< DoubleType, DoubleType > regularizer;
				if ( regularizerMetadata != null )
				{
					final ShiftedDownsampling< ? > shiftedDownsamplingLocal = broadcastedShiftedDownsampling.value();

					final RandomAccessibleInterval< DoubleType > scalingRegularizerSource = N5Utils.open( n5Local, regularizerMetadata.scalingRegularizerDataset );
					final RandomAccessibleInterval< DoubleType > translationRegularizerSource = N5Utils.open( n5Local, regularizerMetadata.scalingRegularizerDataset );

					final RandomAccessible< DoubleType > scalingRegularizer;
					if ( regularizerMetadata.scalingRegularizerMode == RegularizerMode.UPSAMPLE_CURRENT_SOLUTION )
						scalingRegularizer = shiftedDownsamplingLocal.upsampleImage( scalingRegularizerSource, currentScaleLevel );
					else if ( regularizerMetadata.scalingRegularizerMode == RegularizerMode.DOWNSAMPLE_PREVIOUS_SOLUTION )
						scalingRegularizer = shiftedDownsamplingLocal.downsampleImage( scalingRegularizerSource, currentScaleLevel );
					else
						throw new IllegalArgumentException( "unknown regularizer mode" );

					final RandomAccessible< DoubleType > translationRegularizer;
					if ( regularizerMetadata.translationRegularizerMode == RegularizerMode.UPSAMPLE_CURRENT_SOLUTION )
						translationRegularizer = shiftedDownsamplingLocal.upsampleImage( translationRegularizerSource, currentScaleLevel );
					else if ( regularizerMetadata.scalingRegularizerMode == RegularizerMode.DOWNSAMPLE_PREVIOUS_SOLUTION )
						translationRegularizer = shiftedDownsamplingLocal.downsampleImage( translationRegularizerSource, currentScaleLevel );
					else
						throw new IllegalArgumentException( "unknown regularizer mode" );

					regularizer = new RandomAccessiblePairNullable<>( scalingRegularizer, translationRegularizer );
				}
				else
				{
					regularizer = null;
				}

				// solution data blocks
				final RandomAccessibleInterval< DoubleType > scalingTermBlockStorageImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( cellInterval ) );
				final RandomAccessibleInterval< DoubleType > translationTermBlockStorageImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( cellInterval ) );
				final RandomAccessibleInterval< DoubleType > pivotValuesBlockStorageImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( cellInterval ) );
				final IntervalView< DoubleType > scalingTermBlockImg = Views.translate( scalingTermBlockStorageImg, Intervals.minAsLongArray( cellInterval ) );
				final IntervalView< DoubleType > translationTermBlockImg = Views.translate( translationTermBlockStorageImg, Intervals.minAsLongArray( cellInterval ) );
				final IntervalView< DoubleType > pivotValuesBlockImg = Views.translate( pivotValuesBlockStorageImg, Intervals.minAsLongArray( cellInterval ) );

				final Cursor< RealComposite< T > > histogramsBlockImgCursor = Views.flatIterable( histogramsBlockImg ).localizingCursor();
				final Cursor< DoubleType > scalingTermBlockImgCursor = Views.flatIterable( scalingTermBlockImg ).cursor();
				final Cursor< DoubleType > translationTermBlockImgCursor = Views.flatIterable( translationTermBlockImg ).cursor();
				final Cursor< DoubleType > pivotValuesBlockImgCursor = Views.flatIterable( pivotValuesBlockImg ).cursor();

				final RealComposite< T > referenceHistogram = ( RealComposite< T > ) new RealComposite<>(
						ArrayImgs.doubles( histogramsProvider.getReferenceHistogram(), histogramsProvider.getHistogramBins() ).randomAccess(),
						histogramsProvider.getHistogramBins()
					);

				final long[] position = new long[ cellGrid.numDimensions() ];
				while ( histogramsBlockImgCursor.hasNext() )
				{
					final RealComposite< T > histogram = histogramsBlockImgCursor.next();
					histogramsBlockImgCursor.localize( position );

					scalingTermBlockImgCursor.fwd();
					translationTermBlockImgCursor.fwd();
					pivotValuesBlockImgCursor.fwd();

					final List< PointMatch > matches = HistogramMatching.generateHistogramMatches(
							histogram,
							referenceHistogram,
							binValues
						);

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
					if ( regularizer != null )
					{
						final RandomAccessiblePairNullable< DoubleType, DoubleType >.RandomAccess regularizerRandomAccess = regularizer.randomAccess();
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
						modelFound = false;
						e.printStackTrace();
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

					scalingTermBlockImgCursor.get().set( estimatedModelValues[ 0 ] );
					translationTermBlockImgCursor.get().set( estimatedModelValues[ 1 ] );
					pivotValuesBlockImgCursor.get().set( offset[ 0 ] );
				}

				N5Utils.saveBlock( scalingTermBlockImg, n5Local, solutionMetadata.scalingTermDataset, blockPosition );
				N5Utils.saveBlock( translationTermBlockImg, n5Local, solutionMetadata.translationTermDataset, blockPosition );
//				N5Utils.saveBlock( pivotValuesBlockImg, n5Local, flatfieldSolutionMetadata.pivotValuesDataset, blockPosition );
			} );

		broadcastedShiftedDownsampling.destroy();

		return solutionMetadata;
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
}
