package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.janelia.stitching.analysis.FilterAdjacentShifts;

import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;
import net.imglib2.util.IntervalsNullable;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;

public class SubTileOperations
{
	/**
	 * Splits each tile into a grid of subtiles.
	 *
	 * @param tiles
	 * @param gridSize
	 * @return
	 */
	public static List< SubTile > subdivideTiles( final TileInfo[] tiles, final int[] gridSize )
	{
		final List< SubTile > subTiles = new ArrayList<>();
		for ( final TileInfo tile : tiles )
		{
			// make sure that all subtiles are of same size
			final long[] subTileSize = new long[ tile.numDimensions() ];
			for ( int d = 0; d < subTileSize.length; ++d )
				subTileSize[ d ] = tile.getSize( d ) / gridSize[ d ];

			final List< Interval > subTileIntervals = TileOperations.divideSpaceIgnoreSmaller(
					new FinalInterval( tile.getSize() ),
					new FinalDimensions( subTileSize )
				);

			for ( int i = 0; i < subTileIntervals.size(); ++i )
			{
				final SubTile subTile = new SubTile( tile, subTileIntervals.get( i ) );
				subTile.setIndex( subTiles.size() );
				subTiles.add( subTile );
			}
		}
		return subTiles;
	}

	/**
	 * Returns list of overlapping subtile pairs.
	 * If {@code adjacent} is set, retains only pairs that are adjacent in the transformed space (overlap by more than 50%)
	 *
	 * @param subTiles
	 * @param adjacentOnly
	 * @param worldTransformOnly
	 * @return
	 */
	public static List< SubTilePair > findOverlappingSubTiles(
			final List< SubTile > subTiles,
			final boolean adjacentOnly,
			final boolean worldTransformOnly )
	{
		// TODO: optimize
		final List< SubTilePair > overlappingSubTilePairs = new ArrayList<>();
		for ( int i = 0; i < subTiles.size(); i++ )
		{
			for ( int j = i + 1; j < subTiles.size(); j++ )
			{
				final SubTilePair subTilePair = new SubTilePair( subTiles.get( i ), subTiles.get( j ) );
				if ( isOverlappingSubTilePair( subTilePair, adjacentOnly, worldTransformOnly ) )
					overlappingSubTilePairs.add( subTilePair );
			}
		}
		return overlappingSubTilePairs;
	}

	/**
	 * Checks if there is an overlap between a pair of subtiles.
	 * If {@code adjacent} is set, a pair is considered overlapping if it is adjacent in the transformed space (overlap by more than 50%)
	 *
	 * @param subTilePair
	 * @param adjacent
	 * @param worldTransformOnly
	 * @return
	 */
	public static boolean isOverlappingSubTilePair(
			final SubTilePair subTilePair,
			final boolean adjacentOnly,
			final boolean worldTransformOnly )
	{
		if ( subTilePair.getA().getFullTile().getIndex().intValue() != subTilePair.getB().getFullTile().getIndex().intValue() )
		{
			final Pair< Interval, Interval > transformedSubTilePair = TransformedTileOperations.transformSubTilePair( subTilePair, worldTransformOnly );
			if ( transformedSubTilePair.getA() == null || transformedSubTilePair.getB() == null )
				return false;

			final Interval overlap = IntervalsNullable.intersect( transformedSubTilePair.getA(), transformedSubTilePair.getB() );
			if ( overlap != null )
			{
				final long[] minSubTileSize = new long[ Math.max( subTilePair.getA().numDimensions(), subTilePair.getB().numDimensions() ) ];
				for ( int d = 0; d < minSubTileSize.length; ++d )
					minSubTileSize[ d ] = Math.min( subTilePair.getA().dimension( d ), subTilePair.getB().dimension( d ) );

				if ( !adjacentOnly || FilterAdjacentShifts.isAdjacent( new FinalDimensions( minSubTileSize ), overlap ) )
					return true;
			}
		}
		return false;
	}

	/**
	 * Returns stage position of the middle point of the given subtile.
	 *
	 * @param subTile
	 * @return
	 */
	public static double[] getSubTileMiddlePointStagePosition( final SubTile subTile )
	{
		final double[] subTileMiddlePoint = getSubTileMiddlePoint( subTile );
		final double[] subTileMiddlePointStagePosition = new double[ subTile.numDimensions() ];
		for ( int d = 0; d < subTileMiddlePointStagePosition.length; ++d )
			subTileMiddlePointStagePosition[ d ] = subTileMiddlePoint[ d ] + subTile.getFullTile().getStagePosition( d );
		return subTileMiddlePointStagePosition;
	}

	/**
	 * Returns intervals in the fixed subtile coordinate space (that is, the first interval of the pair will have zero min).
	 *
	 * @param globalIntervals
	 * @return
	 */
	public static Pair< Interval, Interval > globalToFixedSpace( final Pair< Interval, Interval > globalIntervals )
	{
		final long[] globalToFixedSpaceOffset = Intervals.minAsLongArray( globalIntervals.getA() );
		return new ValuePair<>(
				IntervalsHelper.offset( globalIntervals.getA(), globalToFixedSpaceOffset ),
				IntervalsHelper.offset( globalIntervals.getB(), globalToFixedSpaceOffset )
			);
	}

	/**
	 * Returns overlap intervals that have been extended to capture the bounding box of a given search radius entirely.
	 * Expects that both subtile intervals and the search radius are given in the fixed subtile coordinate space (that is, {@code subTilePair.getA()} should have zero min).
	 * Returns overlap intervals in relative coordinate space of each subtile, e.g. (overlap in fixed subtile, overlap in moving subtile).
	 *
	 * @param subTilePair
	 * @param errorEllipse
	 * @return
	 */
	public static Pair< Interval, Interval > getAdjustedOverlapIntervals( final Pair< Interval, Interval > subTilePair, final ErrorEllipse errorEllipse )
	{
		final Interval fixedIntervalInFixedSpace = subTilePair.getA(), movingIntervalInFixedSpace = subTilePair.getB();
		if ( !Views.isZeroMin( fixedIntervalInFixedSpace ) )
			throw new IllegalArgumentException( "not in the fixed subtile space" );

		final Interval errorEllipseBoundingBox = Intervals.smallestContainingInterval( errorEllipse.estimateBoundingBox() );

		// try all corners of the bounding box of the error ellipse and use the largest overlap
		final int[] cornersPos = new int[ errorEllipseBoundingBox.numDimensions() ];
		final int[] cornersDimensions = new int[ errorEllipseBoundingBox.numDimensions() ];
		Arrays.fill( cornersDimensions, 2 );
		final IntervalIterator cornerIntervalIterator = new IntervalIterator( cornersDimensions );

		final long[] overlapInFixedSpaceMin = new long[ errorEllipseBoundingBox.numDimensions() ], overlapInFixedSpaceMax = new long[ errorEllipseBoundingBox.numDimensions() ];
		Arrays.fill( overlapInFixedSpaceMin, Long.MAX_VALUE );
		Arrays.fill( overlapInFixedSpaceMax, Long.MIN_VALUE );

		final long[] overlapInMovingSpaceMin = new long[ errorEllipseBoundingBox.numDimensions() ], overlapInMovingSpaceMax = new long[ errorEllipseBoundingBox.numDimensions() ];
		Arrays.fill( overlapInMovingSpaceMin, Long.MAX_VALUE );
		Arrays.fill( overlapInMovingSpaceMax, Long.MIN_VALUE );

		while ( cornerIntervalIterator.hasNext() )
		{
			cornerIntervalIterator.fwd();
			cornerIntervalIterator.localize( cornersPos );

			// get test moving position in the fixed space
			final long[] testMovingPositionInFixedSpace = new long[ errorEllipseBoundingBox.numDimensions() ];
			for ( int d = 0; d < testMovingPositionInFixedSpace.length; ++d )
				testMovingPositionInFixedSpace[ d ] = ( cornersPos[ d ] == 0 ? errorEllipseBoundingBox.min( d ) : errorEllipseBoundingBox.max( d ) );

			// get test moving interval in the fixed space
			final Interval testMovingIntervalInFixedSpace = IntervalsHelper.setPosition( movingIntervalInFixedSpace, testMovingPositionInFixedSpace );

			// get overlap between subtiles in the fixed space
			final Interval testOverlapInFixedSpace = IntervalsNullable.intersect( fixedIntervalInFixedSpace, testMovingIntervalInFixedSpace );

			if ( testOverlapInFixedSpace != null )
			{
				// update ROI in the fixed space
				for ( int d = 0; d < errorEllipseBoundingBox.numDimensions(); ++d )
				{
					overlapInFixedSpaceMin[ d ] = Math.min( testOverlapInFixedSpace.min( d ), overlapInFixedSpaceMin[ d ] );
					overlapInFixedSpaceMax[ d ] = Math.max( testOverlapInFixedSpace.max( d ), overlapInFixedSpaceMax[ d ] );
				}

				// update ROI in the moving space
				final Interval testOverlapInMovingSpace = IntervalsHelper.offset( testOverlapInFixedSpace, testMovingPositionInFixedSpace );
				for ( int d = 0; d < errorEllipseBoundingBox.numDimensions(); ++d )
				{
					overlapInMovingSpaceMin[ d ] = Math.min( testOverlapInMovingSpace.min( d ), overlapInMovingSpaceMin[ d ] );
					overlapInMovingSpaceMax[ d ] = Math.max( testOverlapInMovingSpace.max( d ), overlapInMovingSpaceMax[ d ] );
				}
			}
		}

		for ( int d = 0; d < errorEllipseBoundingBox.numDimensions(); ++d )
			if ( ( overlapInFixedSpaceMin[ d ] == Long.MAX_VALUE || overlapInFixedSpaceMax[ d ] == Long.MIN_VALUE ) || ( overlapInMovingSpaceMin[ d ] == Long.MAX_VALUE || overlapInMovingSpaceMax[ d ] == Long.MIN_VALUE ) )
				return null;

		final Pair< Interval, Interval > adjustedOverlaps = new ValuePair<>(
				new FinalInterval( overlapInFixedSpaceMin, overlapInFixedSpaceMax ),
				new FinalInterval( overlapInMovingSpaceMin, overlapInMovingSpaceMax )
			);

		if ( !Intervals.equalDimensions( adjustedOverlaps.getA(), adjustedOverlaps.getB() ) )
			throw new RuntimeException( "should not happen: adjusted overlap intervals are expected to be of the exact same size" );

		return adjustedOverlaps;
	}

	/**
	 * Returns overlap intervals that have been extended by the given padding value.
	 * Expects that both subtile intervals are given in the fixed subtile coordinate space (that is, subTilePair.getA()} should have zero min).
	 * Returns overlap intervals in relative coordinate space of each subtile, e.g. (overlap in fixed subtile, overlap in moving subtile).
	 *
	 * @param subTilePair
	 * @param padding
	 * @return
	 */
	public static Pair< Interval, Interval > getPaddedOverlapIntervals( final Pair< Interval, Interval > subTilePair, final long[] padding )
	{
		final Interval fixedIntervalInFixedSpace = subTilePair.getA(), movingIntervalInFixedSpace = subTilePair.getB();
		if ( !Views.isZeroMin( fixedIntervalInFixedSpace ) )
			throw new IllegalArgumentException( "not in the fixed subtile space" );

		final Interval overlapInFixedSpace = IntervalsNullable.intersect( fixedIntervalInFixedSpace, movingIntervalInFixedSpace );
		final Interval overlapInMovingSpace = IntervalsHelper.offset( overlapInFixedSpace, Intervals.minAsLongArray( movingIntervalInFixedSpace ) );

		final Interval paddedOverlapInFixedSpace = TileOperations.padInterval(
				overlapInFixedSpace,
				new FinalInterval( Intervals.dimensionsAsLongArray( fixedIntervalInFixedSpace ) ),
				padding
			);
		final Interval paddedOverlapInMovingSpace = TileOperations.padInterval(
				overlapInMovingSpace,
				new FinalInterval( Intervals.dimensionsAsLongArray( movingIntervalInFixedSpace ) ),
				padding
			);

		return new ValuePair<>( paddedOverlapInFixedSpace, paddedOverlapInMovingSpace );
	}

	/**
	 * Returns overlaps in full tiles with respect to the subtiles. Useful for cropping ROI from full tile image.
	 *
	 * @param subTilePair
	 * @param overlaps
	 * @return
	 */
	public static Pair< Interval, Interval > getOverlapsInFullTile( final SubTilePair subTilePair, final Pair< Interval, Interval > overlaps )
	{
		return new ValuePair<>(
				IntervalsHelper.translate( overlaps.getA(), Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTilePair.getA() ) ) ),
				IntervalsHelper.translate( overlaps.getB(), Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTilePair.getB() ) ) )
			);
	}

	/**
	 * Returns the middle point of the given subtile in the full tile coordinate space.
	 *
	 * @param subTile
	 * @return
	 */
	public static double[] getSubTileMiddlePoint( final SubTile subTile )
	{
		final double[] middlePoint = new double[ subTile.numDimensions() ];
		for ( int d = 0; d < middlePoint.length; ++d )
			middlePoint[ d ] = subTile.min( d ) + subTile.dimension( d ) * 0.5;
		return middlePoint;
	}

	/**
	 * Returns an interval of the given subtile with the specified middle point.
	 *
	 * @param middlePoint
	 * @param subTileSize
	 * @return
	 */
	public static RealInterval getSubTileInterval( final double[] middlePoint, final long[] subTileSize )
	{
		final double[] min = new double[ middlePoint.length ], max = new double[ middlePoint.length ];
		for ( int d = 0; d < middlePoint.length; ++d )
		{
			min[ d ] = middlePoint[ d ] - 0.5 * subTileSize[ d ];
			max[ d ] = middlePoint[ d ] + 0.5 * subTileSize[ d ] - 1;
		}
		return new FinalRealInterval( min, max );
	}

	/**
	 * Returns the offset between full tiles based on the offset between the subtiles.
	 *
	 * @param subTilePair
	 * @param subTileOffset
	 * @return
	 */
	public static double[] getFullTileOffset( final SubTilePair subTilePair, final double[] subTileOffset )
	{
		final double[] fullTileOffset = new double[ subTileOffset.length ];
		final SubTile fixedSubTile = subTilePair.getA(), movingSubTile = subTilePair.getB();
		for ( int d = 0; d < fullTileOffset.length; ++d )
			fullTileOffset[ d ] = subTileOffset[ d ] + fixedSubTile.min( d ) - movingSubTile.min( d );
		return fullTileOffset;
	}
}
