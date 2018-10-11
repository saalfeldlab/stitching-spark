package org.janelia.stitching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.janelia.dataaccess.DataProvider;
import org.janelia.util.concurrent.SameThreadExecutorService;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.algorithm.phasecorrelation.PeakFilter;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealConverter;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.exception.ImgLibException;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;
import net.preibisch.stitcher.algorithm.PairwiseStitching;
import net.preibisch.stitcher.algorithm.PairwiseStitchingParameters;

public class StitchSubTilePair< T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > >
{
	private final StitchingJob job;
	private final OffsetUncertaintyEstimator offsetUncertaintyEstimator;
	private final List< RandomAccessiblePairNullable< U, U > > flatfieldsForChannels;
	private final List< Map< Integer, TileInfo > > tileMapsForChannels;

	public StitchSubTilePair(
			final StitchingJob job,
			final OffsetUncertaintyEstimator offsetUncertaintyEstimator,
			final List< RandomAccessiblePairNullable< U, U > > flatfieldsForChannels,
			final List< Map< Integer, TileInfo > > tileMapsForChannels
		)
	{
		this.job = job;
		this.offsetUncertaintyEstimator = offsetUncertaintyEstimator;
		this.flatfieldsForChannels = flatfieldsForChannels;
		this.tileMapsForChannels = tileMapsForChannels;
	}

	/**
	 * Estimate pairwise shift vector between a pair of subtiles.
	 * The first subtile of the given pair is considered 'fixed', and the second is 'moving',
	 * that means, the resulting shift vector will effectively be equal to (NewMovingPos - FixedPos).
	 *
	 * @param subTilePair
	 * @throws PipelineExecutionException
	 * @throws ImgLibException
	 * @throws IOException
	 */
	public SerializablePairWiseStitchingResult stitchSubTilePair( final SubTilePair subTilePair ) throws PipelineExecutionException, ImgLibException, IOException
	{
		final SubTile[] subTiles = subTilePair.toArray();

		// Get approximate transformations for the tile pair. If it is not the first iteration, they have already been estimated prior to pairwise matching.
		final AffineGet[] estimatedFullTileTransforms = new AffineGet[ subTiles.length ];
		for ( int i = 0; i < subTiles.length; ++i )
			estimatedFullTileTransforms[ i ] = TransformedTileOperations.getTileTransform( subTiles[ i ].getFullTile(), offsetUncertaintyEstimator != null );

		final ErrorEllipse movingSubTileSearchRadius;

		if ( offsetUncertaintyEstimator != null )
		{
			for ( final AffineGet estimatedFullTileTransform : estimatedFullTileTransforms )
				Objects.requireNonNull( estimatedFullTileTransform, "expected non-null affine transform for tile" );

//			try
//			{
//				// get search radius for new moving subtile position in the fixed subtile space
//				final EstimatedRelativeSearchRadius combinedSearchRadiusForMovingSubtile = PairwiseTileOperations.getCombinedSearchRadiusForMovingSubTile( subTiles, offsetUncertaintyEstimator );
//
//				movingSubTileSearchRadius = combinedSearchRadiusForMovingSubtile.combinedErrorEllipse;
//				System.out.println( "Estimated error ellipse of size " + Arrays.toString( Intervals.dimensionsAsLongArray( Intervals.smallestContainingInterval( movingSubTileSearchRadius.estimateBoundingBox() ) ) ) );
//			}
//			catch ( final NotEnoughNeighboringTilesException e )
//			{
//				System.out.println( "Could not estimate error ellipse for pair " + subTilePair );
//				final SerializablePairWiseStitchingResult invalidPairwiseResult = new SerializablePairWiseStitchingResult( subTilePair, null, 0 );
//				return invalidPairwiseResult;
//			}

			// FIXME: use uncorrelated error ellipse for now instead of estimating uncertainty
			final double[] uncorrelatedErrorEllipseRadius = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipseRadius(
					subTilePair.getA().getFullTile().getSize(),
					job.getArgs().errorEllipseRadiusAsTileSizeRatio()
				);
			movingSubTileSearchRadius = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipse( uncorrelatedErrorEllipseRadius );
			System.out.println( "Create uncorrelated error ellipse of size " + Arrays.toString( Intervals.dimensionsAsLongArray( Intervals.smallestContainingInterval( movingSubTileSearchRadius.estimateBoundingBox() ) ) ) + " (instead of estimating uncertainty for now)" );
		}
		else
		{
			if ( job.getArgs().constrainMatchingOnFirstIteration() )
			{
				final double[] uncorrelatedErrorEllipseRadius = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipseRadius(
						subTilePair.getA().getFullTile().getSize(),
						job.getArgs().errorEllipseRadiusAsTileSizeRatio()
					);
				movingSubTileSearchRadius = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipse( uncorrelatedErrorEllipseRadius );
				System.out.println( "Create uncorrelated error ellipse of size " + Arrays.toString( Intervals.dimensionsAsLongArray( Intervals.smallestContainingInterval( movingSubTileSearchRadius.estimateBoundingBox() ) ) ) + " to constrain matching" );
			}
			else
			{
				movingSubTileSearchRadius = null;
			}
		}

		if ( movingSubTileSearchRadius != null )
		{
			movingSubTileSearchRadius.setErrorEllipseTransform(
					PairwiseTileOperations.getErrorEllipseTransform( subTiles, estimatedFullTileTransforms )
				);
		}

		// Render both ROIs in the fixed space
		final InvertibleRealTransform[] affinesToFixedTileSpace = new InvertibleRealTransform[] {
				TransformUtils.createTransform( subTilePair.getA().numDimensions() ), // identity transform for fixed tile
				PairwiseTileOperations.getMovingTileToFixedTileTransform( estimatedFullTileTransforms ) // moving tile to fixed tile
			};

		// convert to fixed subtile space
		final InvertibleRealTransform[] affinesToFixedSubTileSpace = new InvertibleRealTransform[ subTiles.length ];
		for ( int i = 0; i < subTiles.length; ++i )
			affinesToFixedSubTileSpace[ i ] = PairwiseTileOperations.getFixedSubTileTransform( subTiles, affinesToFixedTileSpace[ i ] );

		// TODO: use smaller ROI instead of the full subtile?
		@SuppressWarnings( "unchecked" )
		final RandomAccessibleInterval< T >[] roiImagesInFixedSubtileSpace = new RandomAccessibleInterval[ subTiles.length ];
		for ( int i = 0; i < subTiles.length; ++i )
		{
			final List< TileInfo > channelTiles = new ArrayList<>();
			for ( int ch = 0; ch < job.getChannels(); ++ch )
			{
				final TileInfo tile = tileMapsForChannels.get( ch ).get( subTiles[ i ].getFullTile().getIndex() );
				channelTiles.add( tile );

				// validate that all corresponding tiles have the same grid coordinates
				// (or skip validation if unable to extract grid coordinates from tile filename)
				validateGridCoordinates( subTiles[ i ], tile );
			}

			roiImagesInFixedSubtileSpace[ i ] = renderSubTile(
					job.getDataProvider(),
					subTiles[ i ],
					affinesToFixedSubTileSpace[ i ],
					channelTiles,
					Optional.of( flatfieldsForChannels ),
					job.getArgs().blurSigma()
				);
		}
		// ROIs are rendered in the fixed subtile space, validate that the fixed subtile has zero-min
		if ( !Views.isZeroMin( roiImagesInFixedSubtileSpace[ 0 ] ) )
			throw new PipelineExecutionException( "fixed subtile is expected to be zero-min" );

		final SerializablePairWiseStitchingResult pairwiseResult = stitchPairwise( roiImagesInFixedSubtileSpace, job.getParams(), movingSubTileSearchRadius );

		if ( pairwiseResult == null )
		{
			System.out.println( "Could not find phase correlation peaks for pair " + subTilePair );
			final SerializablePairWiseStitchingResult invalidPairwiseResult = new SerializablePairWiseStitchingResult( subTilePair, null, 0 );
			return invalidPairwiseResult;
		}

		pairwiseResult.setVariance( computeVariance( roiImagesInFixedSubtileSpace ) );
		pairwiseResult.setSubTilePair( subTilePair );
		pairwiseResult.setEstimatedFullTileTransformPair( new AffineTransformPair( estimatedFullTileTransforms[ 0 ], estimatedFullTileTransforms[ 1 ] ) );

		System.out.println( "Stitched subtile pair " + subTilePair );
		return pairwiseResult;
	}

	/**
	 * Renders the given subtile in the transformed space averaging over all channels and optionally flat-fielding them.
	 * The resulting image is wrapped as {@link ImagePlus}.
	 *
	 * @param subTile
	 * @param fullTileTransform
	 * @param flatfieldsForChannels
	 * @return rendered subtile image in target space
	 * @throws IOException
	 * @throws IncompatibleTypeException
	 */
	public static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > RandomAccessibleInterval< T > renderSubTile(
			final DataProvider dataProvider,
			final Interval subTileInterval,
			final InvertibleRealTransform fullTileTransform,
			final List< TileInfo > channelTiles,
			final Optional< List< RandomAccessiblePairNullable< U, U > > > optionalChannelFlatfields,
			final double blurSigma ) throws IOException, IncompatibleTypeException
	{
		if ( optionalChannelFlatfields.isPresent() )
			assert channelTiles.size() == optionalChannelFlatfields.get().size();

		final double[] normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( channelTiles.iterator().next().getPixelResolution() );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );
		final double[] blurSigmas = new  double[ normalizedVoxelDimensions.length ];
		for ( int d = 0; d < blurSigmas.length; d++ )
			blurSigmas[ d ] = blurSigma / normalizedVoxelDimensions[ d ];

		System.out.println( "Averaging tile images for " + channelTiles.size() + " channels" );

		RandomAccessibleInterval< FloatType > avgChannelImg = null;
		T inputType = null;

		for ( int channel = 0; channel < channelTiles.size(); ++channel )
		{
			final TileInfo tile = channelTiles.get( channel );

			// get input image ROI
			final RandomAccessibleInterval< T > roiImg = TransformedTileImageLoader.loadTile(
					tile,
					dataProvider,
					Optional.ofNullable( optionalChannelFlatfields.isPresent() ? optionalChannelFlatfields.get().get( channel ) : null ),
					fullTileTransform,
					subTileInterval
				);

			// allocate output image if needed
			if ( avgChannelImg == null )
			{
				avgChannelImg = Views.translate(
						ArrayImgs.floats( Intervals.dimensionsAsLongArray( roiImg ) ),
						Intervals.minAsLongArray( roiImg )
					);
			}

			// store input type
			if ( inputType == null )
				inputType = Util.getTypeFromInterval( roiImg );

			// accumulate data in the output image
			final RandomAccessibleInterval< FloatType > srcImg = Converters.convert( roiImg, new RealFloatConverter<>(), new FloatType() );
			final Cursor< FloatType > srcCursor = Views.flatIterable( srcImg ).cursor();
			final Cursor< FloatType > dstCursor = Views.flatIterable( avgChannelImg ).cursor();
			while ( dstCursor.hasNext() || srcCursor.hasNext() )
				dstCursor.next().add( srcCursor.next() );
		}

		// average output image over the number of accumulated channels
		final FloatType denom = new FloatType( channelTiles.size() );
		final Cursor< FloatType > dstCursor = Views.iterable( avgChannelImg ).cursor();
		while ( dstCursor.hasNext() )
			dstCursor.next().div( denom );

		// blur with requested sigma
		System.out.println( String.format( "Blurring the overlap area of size %s with sigmas=%s", Arrays.toString( Intervals.dimensionsAsLongArray( avgChannelImg ) ), Arrays.toString( blurSigmas ) ) );
		blur( avgChannelImg, blurSigmas );

		// convert the output image to the input datatype
		final RandomAccessibleInterval< T > convertedOutputImgToInputType = Converters.convert( avgChannelImg, new RealConverter<>(), inputType );
		return convertedOutputImgToInputType;
	}

	private static < F extends NumericType< F > > void blur( final RandomAccessibleInterval< F > image, final double[] sigmas ) throws IncompatibleTypeException
	{
		final RandomAccessible< F > extendedImage = Views.extendMirrorSingle( image );
		Gauss3.gauss( sigmas, extendedImage, image, new SameThreadExecutorService() );
	}

	// TODO: compute variance only within new overlapping region (after matching)
	static < T extends NativeType< T > & RealType< T > > double computeVariance( final RandomAccessibleInterval< T >[] roiImages )
	{
		double pixelSum = 0, pixelSumSquares = 0;
		long pixelCount = 0;
		for ( int i = 0; i < 2; ++i )
		{
			final Cursor< T > roiImgCursor = Views.iterable( roiImages[ i ] ).cursor();
			while ( roiImgCursor.hasNext() )
			{
				final double val = roiImgCursor.next().getRealDouble();
				pixelSum += val;
				pixelSumSquares += Math.pow( val, 2 );
			}
			pixelCount += Intervals.numElements( roiImages[ i ] );
		}
		final double variance = pixelSumSquares / pixelCount - Math.pow( pixelSum / pixelCount, 2 );
		return variance;
	}

	public static < T extends NativeType< T > & RealType< T > > SerializablePairWiseStitchingResult stitchPairwise(
			final RandomAccessibleInterval< T >[] roiImages,
			final SerializableStitchingParameters stitchingParameters,
			final PeakFilter peakFilter )
	{
		final PairwiseStitchingParameters params = new PairwiseStitchingParameters(0, stitchingParameters.checkPeaks, true, false, false);
		final Pair< Translation, Double > shift = PairwiseStitching.getShift(
				Views.zeroMin( roiImages[ 0 ] ),
				Views.zeroMin( roiImages[ 1 ] ),
				new Translation(roiImages[ 0 ].numDimensions()),
				new Translation(roiImages[ 1 ].numDimensions()),
				params,
				new SameThreadExecutorService(),
				peakFilter
			);

		if ( shift == null || shift.getA() == null || shift.getB() == null )
			return null;

		final SerializablePairWiseStitchingResult result = new SerializablePairWiseStitchingResult(
				null,
				shift.getA().getTranslationCopy(),
				shift.getB().doubleValue()
			);
		return result;
	}

	private void validateGridCoordinates( final SubTile tileBox, final TileInfo tile )
	{
		{
			String error = null;
			try
			{
				if ( !Utils.getTileCoordinatesString( tile ).equals( Utils.getTileCoordinatesString( tileBox.getFullTile() ) ) )
					error = "tile with index " + tile.getIndex() + " has different grid positions: " +
								Utils.getTileCoordinatesString( tile ) + " vs " + Utils.getTileCoordinatesString( tileBox.getFullTile() );
			}
			catch ( final Exception e ) {}

			if ( error != null )
				throw new RuntimeException( error );
		}
	}
}
