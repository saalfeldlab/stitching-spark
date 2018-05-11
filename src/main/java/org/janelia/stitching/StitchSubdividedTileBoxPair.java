package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.janelia.dataaccess.DataProvider;
import org.janelia.util.concurrent.SameThreadExecutorService;

import ij.ImagePlus;
import mpicbg.imglib.custom.OffsetValidator;
import mpicbg.models.Affine3D;
import mpicbg.models.IllDefinedDataPointsException;
import mpicbg.models.Model;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.exception.ImgLibException;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.imageplus.FloatImagePlus;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.RealTransformSequence;
import net.imglib2.realtransform.Translation;
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

		final InvertibleRealTransform[] estimatedAffines = new AffineTransform3D[ tileBoxes.length ];
		final ImagePlus[] roiImps = new ImagePlus[ tileBoxes.length ];
		final Interval[] transformedRoiIntervals = new Interval[ tileBoxes.length ];

		for ( int i = 0; i < tileBoxes.length; ++i )
		{
			estimatedAffines[ i ] = estimateAffineTransformation( tileBoxes[ i ].getFullTile(), searchRadiusEstimator );
			final Pair< ImagePlus, Interval > roiAndWorldBoundingBox = renderTileBox(
					tileBoxes[ i ],
					estimatedAffines[ i ],
					coordsToTilesChannels,
					flatfieldForChannels
				);
			roiImps[ i ] = roiAndWorldBoundingBox.getA();
			transformedRoiIntervals[ i ] = roiAndWorldBoundingBox.getB();
		}

		final SearchRadius combinedSearchRadiusForMovingBox;
		if ( searchRadiusEstimator != null )
		{
			combinedSearchRadiusForMovingBox = getCombinedSearchRadiusForMovingBox(
					searchRadiusEstimator,
					tileBoxes,
					estimatedAffines,
					transformedRoiIntervals
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
				combinedSearchRadiusForMovingBox
			);

		// compute variance within ROI for both images
		if ( pairwiseResult != null )
			pairwiseResult.setVariance( computeVariance( roiImps ) );

		for ( int i = 0; i < 2; i++ )
			roiImps[ i ].close();

		System.out.println( "Stitched tile box pair " + tileBoxPair );

		return new StitchingResult(
				pairwiseResult,
				combinedSearchRadiusForMovingBox != null ? combinedSearchRadiusForMovingBox.getEllipseRadius() : null
			);
	}

	/**
	 * Estimates an expected affine transformation for a given tile based on offset statistics selected from local neighborhood.
	 * The estimated transformation does the following mapping: local tile coordinates -> expected world coordinates.
	 *
	 * @param tile
	 * @param searchRadiusEstimator
	 * @return
	 * @throws PipelineExecutionException
	 */
	private InvertibleRealTransform estimateAffineTransformation(
			final TileInfo tile,
			final TileSearchRadiusEstimator searchRadiusEstimator ) throws PipelineExecutionException
	{
		if ( searchRadiusEstimator == null )
			return TileOperations.getTileTransform( tile );

		final int[] tileBoxesGridSize = new int[ tile.numDimensions() ];
		Arrays.fill( tileBoxesGridSize, 2 );
		final List< SubdividedTileBox > tileBoxes = SplitTileOperations.splitTilesIntoBoxes( new TileInfo[] { tile }, tileBoxesGridSize );
		final List< PointMatch > matches = new ArrayList<>();
		for ( final SubdividedTileBox tileBox : tileBoxes )
		{
			final SearchRadius searchRadius = searchRadiusEstimator.getSearchRadiusTreeWithinEstimationWindow( tileBox );
			final double[] stagePosition = searchRadiusEstimator.getTileBoxMiddlePointStagePosition( tileBox );
			final PointMatch match = new PointMatch( new Point( stagePosition ), new Point( searchRadius.getEllipseCenter() ) );
			matches.add( match );
		}

		final Model< ? > model = TileModelFactory.createAffineModel( tile );
		try
		{
			model.fit( matches );
		}
		catch ( final NotEnoughDataPointsException | IllDefinedDataPointsException e )
		{
			throw new PipelineExecutionException( e );
		}
		final Affine3D< ? > affineModel = ( Affine3D< ? > ) model;
		final double[][] matrix = new double[ 3 ][ 4 ];
		affineModel.toMatrix( matrix );

		final AffineTransform3D expectedTileTransform = new AffineTransform3D();
		expectedTileTransform.set( matrix );
		return expectedTileTransform;
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
	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > Pair< ImagePlus, Interval > renderTileBox(
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

	private SearchRadius getCombinedSearchRadiusForMovingBox(
			final TileSearchRadiusEstimator searchRadiusEstimator,
			final SubdividedTileBox[] tileBoxes,
			final InvertibleRealTransform[] estimatedTileTransforms,
			final Interval[] transformedBoxIntervals ) throws PipelineExecutionException
	{
		final SearchRadius[] searchRadiusStats = new SearchRadius[ tileBoxes.length ];
		for ( int i = 0; i < tileBoxes.length; ++i )
			searchRadiusStats[ i ] = searchRadiusEstimator.getSearchRadiusTreeWithinEstimationWindow( tileBoxes[ i ] );

		final RealTransform offsetTransform = buildOffsetTransform(
				tileBoxes,
				estimatedTileTransforms,
				transformedBoxIntervals,
				searchRadiusStats
			);

		final SearchRadius fixedTileBoxSearchRadius = searchRadiusStats[ 0 ], movingTileBoxSearchRadius = searchRadiusStats[ 1 ];

		final SearchRadius combinedSearchRadiusForMovingBox = searchRadiusEstimator.getCombinedCovariancesSearchRadius(
				fixedTileBoxSearchRadius,
				movingTileBoxSearchRadius
			);

		combinedSearchRadiusForMovingBox.setOffsetTransform( offsetTransform );

		return combinedSearchRadiusForMovingBox;
	}

	private RealTransform buildOffsetTransform(
			final SubdividedTileBox[] tileBoxes,
			final InvertibleRealTransform[] estimatedTileTransforms,
			final Interval[] transformedBoxIntervals,
			final SearchRadius[] searchRadiusStats )
	{
		final SubdividedTileBox fixedTileBox = tileBoxes[ 0 ], movingTileBox = tileBoxes[ 1 ];
		final InvertibleRealTransform fixedTileEstimatedTransform = estimatedTileTransforms[ 0 ], movingTileEstimatedTransform = estimatedTileTransforms[ 1 ];
		final Interval fixedTileBoxTransformedWorldInterval = transformedBoxIntervals[ 0 ], movingTileBoxTransformedWorldInterval = transformedBoxIntervals[ 1 ];
		final SearchRadius fixedTileBoxSearchRadius = searchRadiusStats[ 0 ], movingTileBoxSearchRadius = searchRadiusStats[ 1 ];

		final RealTransformSequence offsetTransform = new RealTransformSequence();

		// expected offset between ROIs (bounding boxes of transformed fixed tile box and moving tile box)
		final double[] estimatedWorldRoiOffset = new double[ fixedTileBox.numDimensions() ];
		for ( int d = 0; d < estimatedWorldRoiOffset.length; ++d )
			estimatedWorldRoiOffset[ d ] = movingTileBoxTransformedWorldInterval.realMin( d ) - fixedTileBoxTransformedWorldInterval.realMin( d );
		offsetTransform.add( new Translation( estimatedWorldRoiOffset ).inverse() ); // shift between newly estimated position and expected position

		// expected world coordinate of the middle point in the transformed moving tile box
		final double[] transformedMovingBoxMiddlePoint = SplitTileOperations.transformTileBoxMiddlePoint( movingTileBox, movingTileEstimatedTransform );
		offsetTransform.add( new Translation( transformedMovingBoxMiddlePoint ) ); // new world position of the middle point in the transformed moving tile box

		offsetTransform.add( movingTileEstimatedTransform.inverse() ); // new local coordinate of the middle point in the local moving tile box

		// initial (stage) offset between fixed tile box and moving tile box
		final double[] stageTileBoxOffset = new double[ fixedTileBox.numDimensions() ];
		for ( int d = 0; d < stageTileBoxOffset.length; ++d )
			stageTileBoxOffset[ d ] = movingTileBoxSearchRadius.getStagePosition()[ d ] - fixedTileBoxSearchRadius.getStagePosition()[ d ];
		offsetTransform.add( new Translation( stageTileBoxOffset ) ); // add relation between fixed tile box and moving tile box

		final double[] movingTileBoxMiddlePoint = SplitTileOperations.getTileBoxMiddlePoint( movingTileBox );
		offsetTransform.add( new Translation( movingTileBoxMiddlePoint ).inverse() ); // new estimated offset between fixed tile box and moving tile box

		return offsetTransform;
	}

	private < T extends NumericType< T > > void blur( final RandomAccessibleInterval< T > image, final double[] sigmas ) throws IncompatibleTypeException
	{
		final RandomAccessible< T > extendedImage = Views.extendMirrorSingle( image );
		Gauss3.gauss( sigmas, extendedImage, image, new SameThreadExecutorService() );
	}

	private double computeVariance( final ImagePlus[] roiPartImps )
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
			final OffsetValidator pointValidator )
	{
		final int timepoint = 1;
		final int numPeaks = 1;
		PairwiseStitchingPerformer.setThreads( 1 );

		final SerializablePairWiseStitchingResult[] results = PairwiseStitchingPerformer.stitchPairwise(
				roiImps[ 0 ], roiImps[ 1 ], timepoint, timepoint,
				job.getParams(), numPeaks,
				pointValidator
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
