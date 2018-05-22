package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.janelia.dataaccess.DataProvider;
import org.janelia.stitching.TileSearchRadiusEstimator.EstimatedTileBoxRelativeSearchRadius;
import org.janelia.stitching.TileSearchRadiusEstimator.EstimatedTileBoxWorldSearchRadius;
import org.janelia.util.concurrent.SameThreadExecutorService;

import ij.ImagePlus;
import mpicbg.imglib.custom.OffsetValidator;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.RealPoint;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.concatenate.Concatenable;
import net.imglib2.concatenate.PreConcatenable;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.exception.ImgLibException;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.imageplus.FloatImagePlus;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.realtransform.Translation;
import net.imglib2.realtransform.TranslationGet;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

public class StitchSubdividedTileBoxPair< T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > >
{
	public static class StitchingResult implements Serializable
	{
		private static final long serialVersionUID = -533794988639089455L;

		public final SerializablePairWiseStitchingResult shift;
		public final double[] searchRadiusLength;

		public StitchingResult( final SerializablePairWiseStitchingResult shift, final double[] searchRadiusLength )
		{
			this.shift = shift;
			this.searchRadiusLength = searchRadiusLength;
		}
	}

	private final StitchingJob job;
	private final TileSearchRadiusEstimator searchRadiusEstimator;
	private final List< Map< String, TileInfo > > coordsToTilesChannels;
	private final List< RandomAccessiblePairNullable< U, U > > flatfieldForChannels;

	public StitchSubdividedTileBoxPair(
			final StitchingJob job,
			final TileSearchRadiusEstimator searchRadiusEstimator,
			final List< Map< String, TileInfo > > coordsToTilesChannels,
			final List< RandomAccessiblePairNullable< U, U > > flatfieldForChannels
		)
	{
		this.job = job;
		this.searchRadiusEstimator = searchRadiusEstimator;
		this.coordsToTilesChannels = coordsToTilesChannels;
		this.flatfieldForChannels = flatfieldForChannels;
	}

	/**
	 * Estimate pairwise shift vector between a pair of tile boxes (smaller parts of input tiles).
	 * The first tile box of the given pair is considered 'fixed', and the second is 'moving',
	 * that means, the resulting shift vector will effectively be equal to (NewMovingPos - FixedPos).
	 *
	 * @param tileBoxPair
	 * @throws PipelineExecutionException
	 */
	public StitchingResult stitchTileBoxPair( final SubdividedTileBoxPair tileBoxPair ) throws PipelineExecutionException
	{
		final SubdividedTileBox[] tileBoxes = tileBoxPair.toArray();

		final InvertibleRealTransform[] estimatedAffines = new InvertibleRealTransform[ tileBoxes.length ];
		final ImagePlus[] roiImps = new ImagePlus[ tileBoxes.length ];
		final Interval[] transformedRoiIntervals = new Interval[ tileBoxes.length ];

		// Render both ROIs in the fixed space (the moving tile box is transform by forward moving transform followed by inverse fixed transform)
		final InvertibleRealTransform[] fixedBoxSpaceAffines = new InvertibleRealTransform[] {
				new InvertibleRealTransformSequence(), // dummy identity fixed transform
				getMovingTileToFixedTileTransform( estimatedAffines ) // moving to fixed
			};
		for ( int i = 0; i < tileBoxes.length; ++i )
		{
			estimatedAffines[ i ] = estimateAffineTransformation( tileBoxes[ i ].getFullTile(), searchRadiusEstimator );
			final Pair< ImagePlus, Interval > roiAndFixedSpaceBoundingBox = renderTileBox(
					tileBoxes[ i ],
					fixedBoxSpaceAffines[ i ],
					coordsToTilesChannels,
					flatfieldForChannels
				);
			roiImps[ i ] = roiAndFixedSpaceBoundingBox.getA();
			transformedRoiIntervals[ i ] = roiAndFixedSpaceBoundingBox.getB();
		}

		final EstimatedTileBoxRelativeSearchRadius combinedSearchRadiusForMovingBox;
		if ( searchRadiusEstimator != null )
		{
			combinedSearchRadiusForMovingBox = getCombinedSearchRadiusForMovingBox(
					searchRadiusEstimator,
					tileBoxes
				);
		}
		else
		{
			combinedSearchRadiusForMovingBox = null;
		}

		// TODO: use smaller ROI instead of whole subdivided box?

		final SerializablePairWiseStitchingResult pairwiseResult = stitchPairwise(
				tileBoxPair,
				roiImps,
				combinedSearchRadiusForMovingBox.combinedErrorEllipse
			);

		// compute variance within ROI for both images
		if ( pairwiseResult != null )
			pairwiseResult.setVariance( computeVariance( roiImps ) );

		for ( int i = 0; i < 2; i++ )
			roiImps[ i ].close();

		System.out.println( "Stitched tile box pair " + tileBoxPair );

		return new StitchingResult(
				pairwiseResult,
				combinedSearchRadiusForMovingBox != null ? combinedSearchRadiusForMovingBox.combinedErrorEllipse.getEllipseRadius() : null
			);
	}

	/**
	 * Estimates an expected affine transformation for a given tile based on offset statistics selected from local neighborhood.
	 * The estimated transformation performs the following mapping: local tile coordinates -> expected world coordinates.
	 *
	 * @param tile
	 * @param searchRadiusEstimator
	 * @return
	 * @throws PipelineExecutionException
	 */
	static AffineGet estimateAffineTransformation(
			final TileInfo tile,
			final TileSearchRadiusEstimator searchRadiusEstimator ) throws PipelineExecutionException
	{
		return estimateAffineTransformation(
				estimateLinearAndTranslationAffineComponents( tile, searchRadiusEstimator )
			);
	}

	/**
	 * Estimates an expected affine transformation for a given tile based on offset statistics selected from local neighborhood.
	 * The estimated transformation performs the following mapping: local tile coordinates -> expected world coordinates.
	 *
	 * @param estimatedAffineLinearComponent
	 * @param estimatedAffineTranslationComponent
	 * @return
	 * @throws PipelineExecutionException
	 */
	static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > AffineGet estimateAffineTransformation(
			final Pair< AffineGet, TranslationGet > estimatedAffineLinearAndTranslationComponents ) throws PipelineExecutionException
	{
		final int dim = estimatedAffineLinearAndTranslationComponents.getA().numDimensions();
		final A estimatedAffine = TransformUtils.createTransform( dim );
		estimatedAffine.concatenate( estimatedAffineLinearAndTranslationComponents.getA() );
		estimatedAffine.concatenate( estimatedAffineLinearAndTranslationComponents.getB() );
		return estimatedAffine;
	}

	/**
	 * Estimates linear and translation components of the expected affine transformation for a given tile
	 * based on offset statistics selected from local neighborhood.
	 *
	 * @param tile
	 * @param searchRadiusEstimator
	 * @return
	 * @throws PipelineExecutionException
	 */
	static Pair< AffineGet, TranslationGet > estimateLinearAndTranslationAffineComponents(
			final TileInfo tile,
			final TileSearchRadiusEstimator searchRadiusEstimator ) throws PipelineExecutionException
	{
		final SubdividedTileBox fullTileBox = SplitTileOperations.splitTilesIntoBoxes(
				new TileInfo[] { tile },
				IntStream.generate( () -> 1 ).limit( tile.numDimensions() ).toArray()
			).iterator().next();

		final EstimatedTileBoxWorldSearchRadius estimatedSearchRadius = searchRadiusEstimator.estimateSearchRadiusWithinWindow(
				fullTileBox,
				searchRadiusEstimator.getEstimationWindow(
						new RealPoint( SplitTileOperations.getTileBoxMiddlePointStagePosition( fullTileBox ) )
					)
			);

		final Set< TileInfo > neighboringTiles = estimatedSearchRadius.neighboringTiles;

		final double[][] expectedLinearAffineMatrix = new double[ tile.numDimensions() ][ tile.numDimensions() + 1 ];
		for ( final TileInfo neighboringTile : neighboringTiles )
		{
			final AffineGet neighboringTileTransform = TileOperations.getTileTransform( neighboringTile );
			for ( int dRow = 0; dRow < tile.numDimensions(); ++dRow )
				for ( int dCol = 0; dCol < tile.numDimensions(); ++dCol )
					expectedLinearAffineMatrix[ dRow ][ dCol ] += neighboringTileTransform.get( dRow, dCol );
		}
		for ( int dRow = 0; dRow < tile.numDimensions(); ++dRow )
			for ( int dCol = 0; dCol < tile.numDimensions(); ++dCol )
				expectedLinearAffineMatrix[ dRow ][ dCol ] /= neighboringTiles.size();

		final double[] expectedTranslationComponentVector = new double[ tile.numDimensions() ];
		for ( int d = 0; d < tile.numDimensions(); ++d )
			expectedTranslationComponentVector[ d ] = tile.getPosition( d ) + estimatedSearchRadius.errorEllipse.getOffsetsMeanValues()[ d ];

		return new ValuePair<>(
				TransformUtils.createTransform( expectedLinearAffineMatrix ),
				new Translation( expectedTranslationComponentVector )
			);
	}

	/**
	 * Renders the given tile box in the transformed space averaging and optionally flat-fielding all channels.
	 * The resulting image is wrapped as {@link ImagePlus}.
	 *
	 * @param tileBox
	 * @param originalTileTransform
	 * @param coordsToTilesChannels
	 * @param flatfieldForChannels
	 * @return pair: (rendered image; its world bounding box)
	 * @throws PipelineExecutionException
	 */
	private Pair< ImagePlus, Interval > renderTileBox(
			final SubdividedTileBox tileBox,
			final InvertibleRealTransform originalTileTransform,
			final List< Map< String, TileInfo > > coordsToTilesChannels,
			final List< RandomAccessiblePairNullable< U, U > > flatfieldForChannels ) throws PipelineExecutionException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final double[] normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( tileBox.getFullTile().getPixelResolution() );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );
		final double[] blurSigmas = new  double[ normalizedVoxelDimensions.length ];
		for ( int d = 0; d < blurSigmas.length; d++ )
			blurSigmas[ d ] = job.getArgs().blurSigma() / normalizedVoxelDimensions[ d ];

		System.out.println( "Averaging corresponding tile images for " + job.getChannels() + " channels" );
		final String coordsStr = Utils.getTileCoordinatesString( tileBox.getFullTile() );
		int channelsUsed = 0;

		FloatImagePlus< FloatType > avgChannelImg = null;
		Interval roiBoundingBox = null;

		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final TileInfo tile = coordsToTilesChannels.get( channel ).get( coordsStr );
			if ( tile == null )
				throw new PipelineExecutionException( tileBox.getFullTile().getIndex() + ": cannot find corresponding tile for channel " + channel );

			if ( tileBox.getFullTile().getIndex().intValue() != tile.getIndex().intValue() )
				throw new PipelineExecutionException( tileBox.getFullTile().getIndex() + ": different indexes for the same grid position " + Utils.getTileCoordinatesString( tile ) );

			// get ROI image
			final RandomAccessibleInterval< T > roiImg;
			try
			{
				roiImg = TransformedTileImageLoader.loadTile(
						tile,
						dataProvider,
						Optional.ofNullable( flatfieldForChannels.get( channel ) ),
						tileBox.getBoundaries(),
						originalTileTransform
					);
			}
			catch ( final IOException e )
			{
				throw new PipelineExecutionException( e );
			}

			// allocate output image if needed
			if ( avgChannelImg == null )
				avgChannelImg = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( roiImg ) );
			else if ( !Intervals.equalDimensions( avgChannelImg, roiImg ) )
				throw new PipelineExecutionException( "different ROI dimensions for the same grid position " + Utils.getTileCoordinatesString( tile ) );

			// set transformed bounding box
			if ( roiBoundingBox == null )
				roiBoundingBox = new FinalInterval( roiImg );
			else if ( !Intervals.equals( roiBoundingBox, roiImg ) )
				throw new PipelineExecutionException( "different ROI coordinates for the same grid position " + Utils.getTileCoordinatesString( tile ) );

			// accumulate data in the output image
			final RandomAccessibleInterval< FloatType > srcImg = Converters.convert( roiImg, new RealFloatConverter<>(), new FloatType() );
			final Cursor< FloatType > srcCursor = Views.flatIterable( srcImg ).cursor();
			final Cursor< FloatType > dstCursor = Views.flatIterable( avgChannelImg ).cursor();
			while ( dstCursor.hasNext() || srcCursor.hasNext() )
				dstCursor.next().add( srcCursor.next() );

			++channelsUsed;
		}

		if ( channelsUsed == 0 )
			throw new PipelineExecutionException( tileBox.getFullTile().getIndex() + ": images are missing in all channels" );

		// average output image over the number of accumulated channels
		final FloatType denom = new FloatType( channelsUsed );
		final Cursor< FloatType > dstCursor = Views.iterable( avgChannelImg ).cursor();
		while ( dstCursor.hasNext() )
			dstCursor.next().div( denom );

		// blur with requested sigma
		System.out.println( String.format( "Blurring the overlap area of size %s with sigmas=%s", Arrays.toString( Intervals.dimensionsAsLongArray( avgChannelImg ) ), Arrays.toString( blurSigmas ) ) );
		try
		{
			blur( avgChannelImg, blurSigmas );
		}
		catch ( final IncompatibleTypeException e )
		{
			throw new PipelineExecutionException( e );
		}

		final ImagePlus roiImp;
		try
		{
			roiImp = avgChannelImg.getImagePlus();
		}
		catch ( final ImgLibException e )
		{
			throw new PipelineExecutionException( e );
		}

		Utils.workaroundImagePlusNSlices( roiImp );

		return new ValuePair<>( roiImp, roiBoundingBox );
	}

	static EstimatedTileBoxRelativeSearchRadius getCombinedSearchRadiusForMovingBox(
			final TileSearchRadiusEstimator searchRadiusEstimator,
			final SubdividedTileBox[] tileBoxes ) throws PipelineExecutionException
	{
		final EstimatedTileBoxWorldSearchRadius[] searchRadiusStats = new EstimatedTileBoxWorldSearchRadius[ tileBoxes.length ];
		for ( int i = 0; i < tileBoxes.length; ++i )
			searchRadiusStats[ i ] = searchRadiusEstimator.estimateSearchRadiusWithinWindow( tileBoxes[ i ] );

		final EstimatedTileBoxRelativeSearchRadius combinedSearchRadiusForMovingBox = searchRadiusEstimator.getCombinedCovariancesSearchRadius(
				searchRadiusStats[ 0 ],
				searchRadiusStats[ 1 ]
			);

		return combinedSearchRadiusForMovingBox;
	}

	/**
	 * Returns the transformation to map the moving tile into the coordinate space of the fixed tile.
	 *
	 * @param tileTransforms
	 * @return
	 */
	static InvertibleRealTransform getMovingTileToFixedTileTransform( final InvertibleRealTransform[] tileTransforms )
	{
		final InvertibleRealTransformSequence movingTileToFixedTileTransform = new InvertibleRealTransformSequence();
		movingTileToFixedTileTransform.add( tileTransforms[ 1 ] );           // moving tile -> world
		movingTileToFixedTileTransform.add( tileTransforms[ 0 ].inverse() ); // world -> fixed tile
		return movingTileToFixedTileTransform;

	}

	/**
	 * Returns the transformation to map the moving tile into the coordinate space of the fixed box.
	 *
	 * @param tileBoxPair
	 * @param tileTransforms
	 * @return
	 */
	static InvertibleRealTransform getMovingTileToFixedBoxTransform(
			final SubdividedTileBoxPair tileBoxPair,
			final InvertibleRealTransform[] tileTransforms )
	{
		final InvertibleRealTransformSequence movingTileToFixedBoxTransform = new InvertibleRealTransformSequence();
		movingTileToFixedBoxTransform.add( getMovingTileToFixedTileTransform( tileTransforms ) ); // moving tile -> fixed tile
		movingTileToFixedBoxTransform.add( new Translation( Intervals.minAsDoubleArray( tileBoxPair.getA() ) ).inverse() ); // fixed tile -> fixed box
		return movingTileToFixedBoxTransform;
	}

	/**
	 * Returns the offset between zero-min of the transformed moving box and its bounding box.
	 *
	 * @param tileBoxPair
	 * @param tileTransforms
	 * @return
	 */
	static double[] getTransformedMovingBoxToBoundingBoxOffset(
			final SubdividedTileBoxPair tileBoxPair,
			final InvertibleRealTransform[] tileTransforms )
	{
		return getTransformedMovingBoxToBoundingBoxOffset(
				tileBoxPair,
				getMovingTileToFixedBoxTransform( tileBoxPair, tileTransforms )
			);
	}

	/**
	 * Returns the offset between zero-min of the transformed moving box and its bounding box.
	 *
	 * @param tileBoxPair
	 * @param movingTileToFixedBoxTransform
	 * @return
	 */
	static double[] getTransformedMovingBoxToBoundingBoxOffset(
			final SubdividedTileBoxPair tileBoxPair,
			final InvertibleRealTransform movingTileToFixedBoxTransform )
	{
		final double[] transformedMovingTileBoxPosition = new double[ tileBoxPair.getB().numDimensions() ];
		movingTileToFixedBoxTransform.apply( tileBoxPair.getB().getPosition(), transformedMovingTileBoxPosition );

		final RealInterval transformedMovingBoxInterval = TileOperations.getTransformedBoundingBoxReal(
				tileBoxPair.getB(),
				movingTileToFixedBoxTransform
			);

		final double[] transformedMovingTileBoxToBoundingBoxOffset = new double[ tileBoxPair.getB().numDimensions() ];
		for ( int d = 0; d < transformedMovingTileBoxToBoundingBoxOffset.length; ++d )
			transformedMovingTileBoxToBoundingBoxOffset[ d ] = transformedMovingTileBoxPosition[ d ] - transformedMovingBoxInterval.realMin( d );

		return transformedMovingTileBoxToBoundingBoxOffset;
	}

	/**
	 * Builds and sets the transformation for the error ellipse to map it to the coordinate space of the fixed box.
	 *
	 * @param tileBoxPair
	 * @param tileTransforms
	 * @param combinedErrorEllipse
	 * @return
	 */
	static InvertibleRealTransform getErrorEllipseTransform(
			final SubdividedTileBoxPair tileBoxPair,
			final InvertibleRealTransform[] tileTransforms )
	{
		final InvertibleRealTransform movingTileToFixedBoxTransform = getMovingTileToFixedBoxTransform( tileBoxPair, tileTransforms );
		final double[] transformedMovingTileBoxToBoundingBoxOffset = getTransformedMovingBoxToBoundingBoxOffset( tileBoxPair, movingTileToFixedBoxTransform );

		final InvertibleRealTransformSequence errorEllipseTransform = new InvertibleRealTransformSequence();
		errorEllipseTransform.add( movingTileToFixedBoxTransform ); // moving tile -> fixed box
		errorEllipseTransform.add( new Translation( transformedMovingTileBoxToBoundingBoxOffset ).inverse() ); // transformed box top-left -> bounding box top-left

		return errorEllipseTransform;
	}

	private < F extends NumericType< F > > void blur( final RandomAccessibleInterval< F > image, final double[] sigmas ) throws IncompatibleTypeException
	{
		final RandomAccessible< F > extendedImage = Views.extendMirrorSingle( image );
		Gauss3.gauss( sigmas, extendedImage, image, new SameThreadExecutorService() );
	}

	static double computeVariance( final ImagePlus[] roiPartImps )
	{
		double pixelSum = 0, pixelSumSquares = 0;
		long pixelCount = 0;
		for ( int i = 0; i < 2; ++i )
		{
			final ImagePlusImg< FloatType, ? > roiImg = ImagePlusImgs.from( roiPartImps[ i ] );
			final Cursor< FloatType > roiImgCursor = Views.iterable( roiImg ).cursor();
			while ( roiImgCursor.hasNext() )
			{
				final double val = roiImgCursor.next().get();
				pixelSum += val;
				pixelSumSquares += Math.pow( val, 2 );
			}
			pixelCount += roiImg.size();
		}
		final double variance = pixelSumSquares / pixelCount - Math.pow( pixelSum / pixelCount, 2 );
		return variance;
	}

	private SerializablePairWiseStitchingResult stitchPairwise(
			final SubdividedTileBoxPair tileBoxPair,
			final ImagePlus[] roiImps,
			final OffsetValidator offsetValidator )
	{
		final int timepoint = 1;
		final int numPeaks = 1;
		PairwiseStitchingPerformer.setThreads( 1 );

		final SerializablePairWiseStitchingResult[] results = PairwiseStitchingPerformer.stitchPairwise(
				roiImps[ 0 ], roiImps[ 1 ], timepoint, timepoint,
				job.getParams(), numPeaks,
				offsetValidator
			);

		final SerializablePairWiseStitchingResult result = results[ 0 ];

		if ( result == null )
		{
			// TODO: pass actions to update accumulators
//			noPeaksWithinConfidenceIntervalPairsCount.add( 1 );
			System.out.println( "no peaks found within the confidence interval" );
		}
		else
		{
			// TODO: convert offset?
			result.setTileBoxPair( tileBoxPair );
		}

		return result;
	}
}
