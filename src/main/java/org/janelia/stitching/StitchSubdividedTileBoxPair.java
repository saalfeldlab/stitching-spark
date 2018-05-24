package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.janelia.dataaccess.DataProvider;
import org.janelia.stitching.TileSearchRadiusEstimator.EstimatedTileBoxRelativeSearchRadius;
import org.janelia.util.Conversions;
import org.janelia.util.concurrent.SameThreadExecutorService;

import ij.ImagePlus;
import mpicbg.imglib.custom.OffsetValidator;
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
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.InvertibleRealTransform;
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
	private final List< RandomAccessiblePairNullable< U, U > > flatfieldsForChannels;
	private final List< Map< Integer, TileInfo > > tileMapsForChannels;

	public StitchSubdividedTileBoxPair(
			final StitchingJob job,
			final TileSearchRadiusEstimator searchRadiusEstimator,
			final List< RandomAccessiblePairNullable< U, U > > flatfieldsForChannels,
			final List< Map< Integer, TileInfo > > tileMapsForChannels
		)
	{
		this.job = job;
		this.searchRadiusEstimator = searchRadiusEstimator;
		this.flatfieldsForChannels = flatfieldsForChannels;
		this.tileMapsForChannels = tileMapsForChannels;
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

		final AffineGet[] estimatedTileTransforms = new AffineGet[ tileBoxes.length ];
		for ( int i = 0; i < tileBoxes.length; ++i )
		{
			if ( searchRadiusEstimator != null )
				estimatedTileTransforms[ i ] = TransformedTileOperations.estimateAffineTransformation( tileBoxes[ i ].getFullTile(), searchRadiusEstimator );
			else
				estimatedTileTransforms[ i ] = TransformedTileOperations.getTileTransform( tileBoxes[ i ].getFullTile() );
		}

		// Render both ROIs in the fixed space
		final InvertibleRealTransform[] affinesToFixedTileSpace = new InvertibleRealTransform[] {
				TransformUtils.createTransform( tileBoxPair.getA().numDimensions() ), // identity transform for fixed tile
				PairwiseTileOperations.getMovingTileToFixedTileTransform( estimatedTileTransforms ) // moving tile to fixed tile
			};

		final InvertibleRealTransform[] affinesToFixedBoxSpace = new InvertibleRealTransform[ tileBoxes.length ];
		for ( int i = 0; i < tileBoxes.length; ++i )
		{
			// convert to fixed box space
			affinesToFixedBoxSpace[ i ] = PairwiseTileOperations.getFixedBoxTransform(
					tileBoxes,
					affinesToFixedTileSpace[ i ]
				);
		}

		final ImagePlus[] roiImps = new ImagePlus[ tileBoxes.length ];
		final Interval[] transformedRoiIntervals = new Interval[ tileBoxes.length ];
		for ( int i = 0; i < tileBoxes.length; ++i )
		{
			final Pair< ImagePlus, Interval > roiAndBoundingBox = renderTileBox(
					tileBoxes[ i ],
					affinesToFixedBoxSpace[ i ],
					flatfieldsForChannels
				);
			roiImps[ i ] = roiAndBoundingBox.getA();
			transformedRoiIntervals[ i ] = roiAndBoundingBox.getB();
		}

		// ROIs are rendered in the fixed box space, validate that the fixed box has zero-min
		if ( !Views.isZeroMin( transformedRoiIntervals[ 0 ] ) )
			throw new PipelineExecutionException( "fixed box is expected to be zero-min" );

		// get search radius for new moving box position in the fixed box space
		final EstimatedTileBoxRelativeSearchRadius combinedSearchRadiusForMovingBox;
		if ( searchRadiusEstimator != null )
		{
			combinedSearchRadiusForMovingBox = PairwiseTileOperations.getCombinedSearchRadiusForMovingBox(
					searchRadiusEstimator,
					tileBoxes
				);

			combinedSearchRadiusForMovingBox.combinedErrorEllipse.setErrorEllipseTransform(
					PairwiseTileOperations.getErrorEllipseTransform( tileBoxes, estimatedTileTransforms )
				);
		}
		else
		{
			combinedSearchRadiusForMovingBox = null;
		}

		// TODO: use smaller ROI instead of the entire subdivided box?

		final SerializablePairWiseStitchingResult pairwiseResult = stitchPairwise(
				roiImps,
				combinedSearchRadiusForMovingBox.combinedErrorEllipse
			);

		if ( pairwiseResult != null )
		{
			// Resulting offset is between the moving bounding box in the fixed box space.
			// Convert it to the offset between the moving and fixed boxes in the global translated space (where the linear affine component of each tile has been undone).
			final double[] newStitchedOffset = PairwiseTileOperations.getNewStitchedOffset(
					tileBoxes,
					estimatedTileTransforms,
					Conversions.toDoubleArray( pairwiseResult.getOffset() )
				);

			pairwiseResult.setOffset( Conversions.toFloatArray( newStitchedOffset ) );
			pairwiseResult.setVariance( computeVariance( roiImps ) );
			pairwiseResult.setTileBoxPair( tileBoxPair );
		}

		for ( int i = 0; i < 2; i++ )
			roiImps[ i ].close();

		System.out.println( "Stitched tile box pair " + tileBoxPair );

		return new StitchingResult(
				pairwiseResult,
				combinedSearchRadiusForMovingBox != null ? combinedSearchRadiusForMovingBox.combinedErrorEllipse.getEllipseRadius() : null
			);
	}

	/**
	 * Renders the given tile box in the transformed space averaging over all channels and optionally flat-fielding them.
	 * The resulting image is wrapped as {@link ImagePlus}.
	 *
	 * @param tileBox
	 * @param fullTileTransform
	 * @param flatfieldsForChannels
	 * @return pair: (rendered image; its world bounding box)
	 * @throws PipelineExecutionException
	 */
	private Pair< ImagePlus, Interval > renderTileBox(
			final SubdividedTileBox tileBox,
			final InvertibleRealTransform fullTileTransform,
			final List< RandomAccessiblePairNullable< U, U > > flatfieldsForChannels ) throws PipelineExecutionException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final double[] normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( tileBox.getFullTile().getPixelResolution() );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );
		final double[] blurSigmas = new  double[ normalizedVoxelDimensions.length ];
		for ( int d = 0; d < blurSigmas.length; d++ )
			blurSigmas[ d ] = job.getArgs().blurSigma() / normalizedVoxelDimensions[ d ];

		System.out.println( "Averaging corresponding tile images for " + job.getChannels() + " channels" );

		FloatImagePlus< FloatType > avgChannelImg = null;
		Interval roiBoundingBox = null;
		int channelsUsed = 0;

		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final TileInfo tile = tileMapsForChannels.get( channel ).get( tileBox.getFullTile().getIndex() );

			// validate that all corresponding tiles have the same grid coordinates
			if ( !Utils.getTileCoordinatesString( tile ).equals( Utils.getTileCoordinatesString( tileBox.getFullTile() ) ) )
			{
				throw new RuntimeException(
						"tile with index " + tile.getIndex() + " has different grid positions: " +
								Utils.getTileCoordinatesString( tile ) + " vs " + Utils.getTileCoordinatesString( tileBox.getFullTile() )
					);
			}

			// get ROI image
			final RandomAccessibleInterval< T > roiImg;
			try
			{
				roiImg = TransformedTileImageLoader.loadTile(
						tile,
						dataProvider,
						Optional.ofNullable( flatfieldsForChannels.get( channel ) ),
						tileBox.getBoundaries(),
						fullTileTransform
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

	private < F extends NumericType< F > > void blur( final RandomAccessibleInterval< F > image, final double[] sigmas ) throws IncompatibleTypeException
	{
		final RandomAccessible< F > extendedImage = Views.extendMirrorSingle( image );
		Gauss3.gauss( sigmas, extendedImage, image, new SameThreadExecutorService() );
	}

	// TODO: compute variance only within new overlapping region (after matching)
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

		return result;
	}
}
