package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.janelia.stitching.analysis.FilterAdjacentShifts;

import mpicbg.imglib.custom.OffsetConverter;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;
import net.imglib2.util.IntervalsNullable;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;

public class SplitTileOperations
{
	/**
	 * Splits each tile into grid of smaller boxes, and for each box stores index mapping to the original tile.
	 *
	 * @param tiles
	 * @return
	 */
	public static List< TileInfo > splitTilesIntoBoxes( final TileInfo[] tiles, final int[] gridSize )
	{
		final List< TileInfo > tileSplitBoxes = new ArrayList<>();
		for ( final TileInfo tile : tiles )
		{
			// make sure that all tile boxes are of same size
			final long[] tileBoxSize = new long[ tile.numDimensions() ];
			for ( int d = 0; d < tileBoxSize.length; ++d )
				tileBoxSize[ d ] = tile.getSize( d ) / gridSize[ d ];

			final List< TileInfo > splitTile = TileOperations.divideSpaceIgnoreSmaller(
					new FinalInterval( tile.getSize() ),
					new FinalDimensions( tileBoxSize )
				);

			for ( final TileInfo box : splitTile )
			{
				box.setOriginalTile( tile );
				box.setIndex( tileSplitBoxes.size() );
				tileSplitBoxes.add( box );
			}
		}
		return tileSplitBoxes;
	}

	/**
	 * Returns list of overlapping tile box pairs.
	 * If {@code adjacent} is set, retains only pairs that are adjacent in the transformed space (overlap by more than 50%)
	 *
	 * @param tileBoxes
	 * @param adjacentOnly
	 * @return
	 */
	public static List< TilePair > findOverlappingTileBoxes( final List< TileInfo > tileBoxes, final boolean adjacentOnly )
	{
		final List< TilePair > overlappingBoxes = new ArrayList<>();
		for ( int i = 0; i < tileBoxes.size(); i++ )
		{
			for ( int j = i + 1; j < tileBoxes.size(); j++ )
			{
				final TilePair tileBoxPair = new TilePair( tileBoxes.get( i ), tileBoxes.get( j ) );
				if ( isOverlappingTileBoxPair( tileBoxPair, adjacentOnly ) )
					overlappingBoxes.add( tileBoxPair );
			}
		}
		return overlappingBoxes;
	}

	/**
	 * Checks if a tile box pair is an overlapping pair.
	 * If {@code adjacent} is set, a pair is considered overlapping if it is adjacent in the transformed space (overlap by more than 50%)
	 *
	 * @param tileBoxes
	 * @param adjacent
	 * @return
	 */
	public static boolean isOverlappingTileBoxPair( final TilePair tileBoxPair, final boolean adjacentOnly )
	{
		if ( tileBoxPair.getA().getOriginalTile().getIndex().intValue() != tileBoxPair.getB().getOriginalTile().getIndex().intValue() )
		{
			final Pair< Interval, Interval > transformedTileBoxPair = transformTileBoxPair( tileBoxPair );
			final Interval tileBoxesOverlap = IntervalsNullable.intersect( transformedTileBoxPair.getA(), transformedTileBoxPair.getB() );
			if ( tileBoxesOverlap != null )
			{
				if ( !adjacentOnly || FilterAdjacentShifts.isAdjacent( getMinTileDimensions( tileBoxPair ), tileBoxesOverlap ) )
					return true;
			}
		}
		return false;
	}

	/**
	 * Returns transformed tile box intervals for a pair of tile boxes.
	 *
	 * @param tileBoxPair
	 * @return
	 */
	public static Pair< Interval, Interval > transformTileBoxPair( final TilePair tileBoxPair )
	{
		return new ValuePair<>(
				transformTileBox( tileBoxPair.getA() ),
				transformTileBox( tileBoxPair.getB() )
			);
	}

	/**
	 * Returns a tile box interval in the global space.
	 * The center coordinate of the resulting interval is defined by transforming the middle point of the tile box.
	 *
	 * @param tileBox
	 * @return
	 */
	public static Interval transformTileBox( final TileInfo tileBox )
	{
		final InvertibleRealTransform tileTransform = TileOperations.getTileTransform( tileBox.getOriginalTile() );
		final double[] tileBoxMiddlePoint = getTileBoxMiddlePoint( tileBox );
		final double[] transformedTileBoxMiddlePoint = new double[ tileBoxMiddlePoint.length ];
		tileTransform.apply( tileBoxMiddlePoint, transformedTileBoxMiddlePoint );
		final RealInterval transformedTileBoxInterval = getTileBoxInterval( transformedTileBoxMiddlePoint, tileBox.getSize() );
		return TileOperations.roundRealInterval( transformedTileBoxInterval );
	}

	/**
	 * Returns intervals in the fixed tile box coordinate space (that is, the first interval has zero min).
	 *
	 * @param globalIntervals
	 * @return
	 */
	public static Pair< Interval, Interval > globalToFixedBoxSpace( final Pair< Interval, Interval > globalIntervals )
	{
		final long[] globalToFixedBoxSpaceOffset = Intervals.minAsLongArray( globalIntervals.getA() );
		return new ValuePair<>(
				IntervalsHelper.offset( globalIntervals.getA(), globalToFixedBoxSpaceOffset ),
				IntervalsHelper.offset( globalIntervals.getB(), globalToFixedBoxSpaceOffset )
			);
	}

	/**
	 * Returns overlap intervals that have been extended to capture the bounding box of a given search radius entirely.
	 * Expects that both tile box intervals and the search radius are given in the fixed box coordinate space (that is, tileBoxPair.getA() should have zero min).
	 * Returns overlap intervals in relative coordinate space of each tile box, e.g. (overlap in fixed tile box, overlap in moving tile box).
	 *
	 * @param tileBoxPair
	 * @param searchRadius
	 * @return
	 */
	public static Pair< Interval, Interval > getAdjustedOverlapIntervals( final Pair< Interval, Interval > tileBoxPair, final SearchRadius searchRadius )
	{
		final Interval fixedIntervalInFixedSpace = tileBoxPair.getA(), movingIntervalInFixedSpace = tileBoxPair.getB();
		if ( !Views.isZeroMin( fixedIntervalInFixedSpace ) )
			throw new IllegalArgumentException( "not in the fixed tile box space" );

		final Interval searchRadiusBoundingBox = Intervals.smallestContainingInterval( searchRadius.getBoundingBox() );

		// try all corners of the bounding box of the search radius and use the largest overlap
		final int[] cornersPos = new int[ searchRadiusBoundingBox.numDimensions() ];
		final int[] cornersDimensions = new int[ searchRadiusBoundingBox.numDimensions() ];
		Arrays.fill( cornersDimensions, 2 );
		final IntervalIterator cornerIntervalIterator = new IntervalIterator( cornersDimensions );

		final long[] overlapInFixedSpaceMin = new long[ searchRadiusBoundingBox.numDimensions() ], overlapInFixedSpaceMax = new long[ searchRadiusBoundingBox.numDimensions() ];
		Arrays.fill( overlapInFixedSpaceMin, Long.MAX_VALUE );
		Arrays.fill( overlapInFixedSpaceMax, Long.MIN_VALUE );

		final long[] overlapInMovingSpaceMin = new long[ searchRadiusBoundingBox.numDimensions() ], overlapInMovingSpaceMax = new long[ searchRadiusBoundingBox.numDimensions() ];
		Arrays.fill( overlapInMovingSpaceMin, Long.MAX_VALUE );
		Arrays.fill( overlapInMovingSpaceMax, Long.MIN_VALUE );

		while ( cornerIntervalIterator.hasNext() )
		{
			cornerIntervalIterator.fwd();
			cornerIntervalIterator.localize( cornersPos );

			// get test moving position in the fixed space
			final long[] testMovingPositionInFixedSpace = new long[ searchRadiusBoundingBox.numDimensions() ];
			for ( int d = 0; d < testMovingPositionInFixedSpace.length; ++d )
				testMovingPositionInFixedSpace[ d ] = ( cornersPos[ d ] == 0 ? searchRadiusBoundingBox.min( d ) : searchRadiusBoundingBox.max( d ) );

			// get test moving interval in the fixed space
			final Interval testMovingIntervalInFixedSpace = IntervalsHelper.setPosition( movingIntervalInFixedSpace, testMovingPositionInFixedSpace );

			// get overlap between tile boxes in the fixed space
			final Interval testOverlapInFixedSpace = IntervalsNullable.intersect( fixedIntervalInFixedSpace, testMovingIntervalInFixedSpace );

			if ( testOverlapInFixedSpace != null )
			{
				// update ROI in the fixed space
				for ( int d = 0; d < searchRadiusBoundingBox.numDimensions(); ++d )
				{
					overlapInFixedSpaceMin[ d ] = Math.min( testOverlapInFixedSpace.min( d ), overlapInFixedSpaceMin[ d ] );
					overlapInFixedSpaceMax[ d ] = Math.max( testOverlapInFixedSpace.max( d ), overlapInFixedSpaceMax[ d ] );
				}

				// update ROI in the moving space
				final Interval testOverlapInMovingSpace = IntervalsHelper.offset( testOverlapInFixedSpace, testMovingPositionInFixedSpace );
				for ( int d = 0; d < searchRadiusBoundingBox.numDimensions(); ++d )
				{
					overlapInMovingSpaceMin[ d ] = Math.min( testOverlapInMovingSpace.min( d ), overlapInMovingSpaceMin[ d ] );
					overlapInMovingSpaceMax[ d ] = Math.max( testOverlapInMovingSpace.max( d ), overlapInMovingSpaceMax[ d ] );
				}
			}
		}

		for ( int d = 0; d < searchRadiusBoundingBox.numDimensions(); ++d )
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
	 * Expects that both tile box intervals are given in the fixed box coordinate space (that is, tileBoxPair.getA() should have zero min).
	 * Returns overlap intervals in relative coordinate space of each tile box, e.g. (overlap in fixed tile box, overlap in moving tile box).
	 *
	 * @param tileBoxPair
	 * @param padding
	 * @return
	 */
	public static Pair< Interval, Interval > getPaddedOverlapIntervals( final Pair< Interval, Interval > tileBoxPair, final long[] padding )
	{
		final Interval fixedIntervalInFixedSpace = tileBoxPair.getA(), movingIntervalInFixedSpace = tileBoxPair.getB();
		if ( !Views.isZeroMin( fixedIntervalInFixedSpace ) )
			throw new IllegalArgumentException( "not in the fixed tile box space" );

		final Interval overlapInFixedSpace = IntervalsNullable.intersect( fixedIntervalInFixedSpace, movingIntervalInFixedSpace );
		final Interval overlapInMovingSpace = IntervalsHelper.offset( overlapInFixedSpace, Intervals.minAsLongArray( movingIntervalInFixedSpace ) );

		final Interval paddedOverlapInFixedSpace = TileOperations.padInterval(
				overlapInFixedSpace,
				new FinalDimensions( Intervals.dimensionsAsLongArray( fixedIntervalInFixedSpace ) ),
				padding
			);
		final Interval paddedOverlapInMovingSpace = TileOperations.padInterval(
				overlapInMovingSpace,
				new FinalDimensions( Intervals.dimensionsAsLongArray( movingIntervalInFixedSpace ) ),
				padding
			);

		return new ValuePair<>( paddedOverlapInFixedSpace, paddedOverlapInMovingSpace );
	}

	/**
	 * Returns overlaps in full tiles with respect to the tile boxes. Useful for cropping ROI from full tile image.
	 *
	 * @param tileBoxPair
	 * @param overlaps
	 * @return
	 */
	public static Pair< Interval, Interval > getOverlapsInFullTile( final TilePair tileBoxPair, final Pair< Interval, Interval > overlaps )
	{
		return new ValuePair<>(
				IntervalsHelper.translate( overlaps.getA(), Intervals.minAsLongArray( tileBoxPair.getA().getBoundaries() ) ),
				IntervalsHelper.translate( overlaps.getB(), Intervals.minAsLongArray( tileBoxPair.getB().getBoundaries() ) )
			);
	}

	/**
	 * Returns helper object containing required offset values for both tiles to be able to compute the shift vector.
	 *
	 * @param overlaps
	 * @return
	 */
	public static OffsetConverter getOffsetConverter( final Pair< Interval, Interval > overlaps )
	{
		return getOffsetConverter( overlaps, null );
	}

	/**
	 * Returns helper object containing required offset values for both tiles to be able to compute the shift vector.
	 *
	 * @param overlaps
	 * @param globalOffset
	 * @return
	 */
	public static OffsetConverter getOffsetConverter( final Pair< Interval, Interval > overlaps, final double[] globalOffset )
	{
		final Interval[] overlapsArr = new Interval[] { overlaps.getA(), overlaps.getB() };
		final int dim = Math.max( overlaps.getA().numDimensions(), overlaps.getB().numDimensions() );

		final long[][] roiToTileOffset = new long[ 2 ][];
		for ( int i = 0; i < 2; ++i )
		{
			roiToTileOffset[ i ] = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
				roiToTileOffset[ i ][ d ] = overlapsArr[ i ].min( d );
		}

		return new FinalOffsetConverter( roiToTileOffset, globalOffset != null ? globalOffset : new double[ dim ] );
	}

	/**
	 * Returns middle point in a given tile box.
	 *
	 * @param tileBox
	 * @return
	 */
	public static double[] getTileBoxMiddlePoint( final TileInfo tileBox )
	{
		final double[] middlePoint = new double[ tileBox.numDimensions() ];
		for ( int d = 0; d < middlePoint.length; ++d )
			middlePoint[ d ] = tileBox.getPosition( d ) + 0.5 * tileBox.getSize( d );
		return middlePoint;
	}

	/**
	 * Returns an interval of a given tile box with specified middle point.
	 *
	 * @param middlePoint
	 * @param boxSize
	 * @return
	 */
	public static RealInterval getTileBoxInterval( final double[] middlePoint, final long[] boxSize )
	{
		final double[] min = new double[ middlePoint.length ], max = new double[ middlePoint.length ];
		for ( int d = 0; d < middlePoint.length; ++d )
		{
			min[ d ] = middlePoint[ d ] - 0.5 * boxSize[ d ];
			max[ d ] = middlePoint[ d ] + 0.5 * boxSize[ d ] - 1;
		}
		return new FinalRealInterval( min, max );
	}

	/**
	 * Return min over tile dimensions for both tiles in a given pair.
	 *
	 * @param pair
	 * @return
	 */
	public static Dimensions getMinTileDimensions( final TilePair pair )
	{
		final long[] minDimensions = new long[ Math.max( pair.getA().numDimensions(), pair.getB().numDimensions() ) ];
		for ( int d = 0; d < minDimensions.length; ++d )
			minDimensions[ d ] = Math.min( pair.getA().getSize( d ), pair.getB().getSize( d ) );
		return new FinalDimensions( minDimensions );
	}

	/**
	 * Returns the offset between full tiles based on the offset between tile boxes and their positions within respective tiles.
	 *
	 * @param tileBoxPair
	 * @param tileBoxOffset
	 * @return
	 */
	public static double[] getFullTileOffset( final TilePair tileBoxPair, final double[] tileBoxOffset )
	{
		final TileInfo fixedTileBox = tileBoxPair.getA(), movingTileBox = tileBoxPair.getB();
		if ( fixedTileBox.getOriginalTile() == null || movingTileBox.getOriginalTile() == null )
			throw new IllegalArgumentException( "was given full tiles instead of tile boxes" );

		final double[] fullTileOffset = new double[ tileBoxOffset.length ];
		for ( int d = 0; d < fullTileOffset.length; ++d )
			fullTileOffset[ d ] = tileBoxOffset[ d ] + fixedTileBox.getPosition( d ) - movingTileBox.getPosition( d );
		return fullTileOffset;
	}
}
