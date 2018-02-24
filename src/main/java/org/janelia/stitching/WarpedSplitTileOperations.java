package org.janelia.stitching;

import java.util.ArrayList;
import java.util.List;

import org.janelia.stitching.analysis.FilterAdjacentShifts;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;
import net.imglib2.util.IntervalsNullable;

public class WarpedSplitTileOperations
{
	/**
	 * Returns list of overlapping tile box pairs.
	 * If {@code adjacent} is set, retains only pairs that are adjacent in the transformed space (overlap by more than 50%)
	 *
	 * @param tileBoxes
	 * @param adjacent
	 * @param tileSlabMapping
	 * @return
	 */
	public static List< TilePair > findOverlappingTileBoxes( final List< TileInfo > tileBoxes, final boolean adjacent, final TileSlabMapping tileSlabMapping )
	{
		final List< TilePair > overlappingBoxes = new ArrayList<>();
		for ( int i = 0; i < tileBoxes.size(); i++ )
		{
			for ( int j = i + 1; j < tileBoxes.size(); j++ )
			{
				final TilePair tileBoxPair = new TilePair( tileBoxes.get( i ), tileBoxes.get( j ) );
				if ( isOverlappingTileBoxPair( tileBoxPair, adjacent, tileSlabMapping ) )
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
	 * @param tileSlabMapping
	 * @return
	 */
	public static boolean isOverlappingTileBoxPair( final TilePair tileBoxPair, final boolean adjacent, final TileSlabMapping tileSlabMapping )
	{
		if ( tileBoxPair.getA().getOriginalTile().getIndex().intValue() != tileBoxPair.getB().getOriginalTile().getIndex().intValue() )
		{
			final Interval fixedTileBoxInterval = tileBoxPair.getA().getBoundaries();
			final Interval movingInFixedTileBoxInterval = transformMovingTileBox( tileBoxPair, tileSlabMapping );
			final Interval tileBoxesOverlap = IntervalsNullable.intersect( fixedTileBoxInterval, movingInFixedTileBoxInterval );
			if ( tileBoxesOverlap != null )
			{
				if ( !adjacent || FilterAdjacentShifts.isAdjacent( SplitTileOperations.getMinTileDimensions( tileBoxPair ), tileBoxesOverlap ) )
					return true;
			}
		}
		return false;
	}

	/**
	 * Returns an interval of the moving tile box being transformed into coordinate space of the fixed original tile.
	 * @param tileBoxPair
	 * @param tileSlabMapping
	 * @return
	 */
	public static Interval transformMovingTileBox( final TilePair tileBoxPair, final TileSlabMapping tileSlabMapping )
	{
		final TileInfo fixedTileBox = tileBoxPair.getA(), movingTileBox = tileBoxPair.getB();
		final InvertibleRealTransform fixedTileTransform = WarpedTileOperations.getTileTransform( fixedTileBox.getOriginalTile(), tileSlabMapping );
		return transformMovingTileBox( fixedTileTransform, movingTileBox, tileSlabMapping );
	}

	/**
	 * Returns an interval of the moving tile box being transformed into coordinate space of the fixed original tile.
	 *
	 * @param fixedTileTransform
	 * @param movingTileBox
	 * @param tileSlabMapping
	 * @return
	 */
	public static Interval transformMovingTileBox( final InvertibleRealTransform fixedTileTransform, final TileInfo movingTileBox, final TileSlabMapping tileSlabMapping )
	{
		final double[] movingMiddlePoint = SplitTileOperations.getTileBoxMiddlePoint( movingTileBox );
		final double[] movingInFixedMiddlePoint = new double[ movingMiddlePoint.length ];
		final InvertibleRealTransform movingTileTransform = WarpedTileOperations.getTileTransform( movingTileBox.getOriginalTile(), tileSlabMapping );
		final InvertibleRealTransformSequence movingToFixedTransform = new InvertibleRealTransformSequence();
		movingToFixedTransform.add( movingTileTransform );
		movingToFixedTransform.add( fixedTileTransform.inverse() );
		movingToFixedTransform.apply( movingMiddlePoint, movingInFixedMiddlePoint );
		final RealInterval movingInFixedTileBoxRealInterval = SplitTileOperations.getTileBoxInterval( movingInFixedMiddlePoint, movingTileBox.getSize() );
		return TileOperations.roundRealInterval( movingInFixedTileBoxRealInterval );
	}

	/**
	 * Returns overlap intervals of a given tile box pair in the coordinate space of each tile (useful for cropping).
	 *
	 * @param tileBoxPair
	 * @param tileSlabMapping
	 * @return
	 */
	public static Interval[] getOverlapIntervals( final TilePair tileBoxPair, final TileSlabMapping tileSlabMapping )
	{
		final Interval[] tileBoxesInFixedSpace = new Interval[] { tileBoxPair.getA().getBoundaries(), transformMovingTileBox( tileBoxPair, tileSlabMapping ) };
		final Interval overlapInFixedSpace = IntervalsNullable.intersect( tileBoxesInFixedSpace[ 0 ], tileBoxesInFixedSpace[ 1 ] );
		if ( overlapInFixedSpace == null )
			return null;

		final long[] originalMovingTileTopLeftCornerInFixedSpace = Intervals.minAsLongArray( IntervalsHelper.offset( tileBoxesInFixedSpace[ 1 ], Intervals.minAsLongArray( tileBoxPair.getB().getBoundaries() ) ) );
		final Interval[] originalTilesInFixedSpace = new Interval[] {
				new FinalInterval( tileBoxPair.getA().getOriginalTile().getSize() ),
				IntervalsHelper.translate( new FinalInterval( tileBoxPair.getB().getOriginalTile().getSize() ), originalMovingTileTopLeftCornerInFixedSpace ) };

		final Interval[] overlapsInOriginalTileSpace = new Interval[ 2 ];
		for ( int j = 0; j < 2; j++ )
			overlapsInOriginalTileSpace[ j ] = IntervalsHelper.offset( overlapInFixedSpace, Intervals.minAsLongArray( originalTilesInFixedSpace[ j ] ) );

		return overlapsInOriginalTileSpace;
	}

	/**
	 * Returns overlap intervals of a given tile box pair in the coordinate space of each tile (useful for cropping).
	 * The overlaps are extended by a given padding value.
	 *
	 * @param tileBoxPair
	 * @param padding
	 * @param tileSlabMapping
	 * @return
	 */
	public static Interval[] getPaddedOverlapIntervals( final TilePair tileBoxPair, final long[] padding, final TileSlabMapping tileSlabMapping )
	{
		final Interval[] overlapsInOriginalTileSpace = getOverlapIntervals( tileBoxPair, tileSlabMapping );
		if ( overlapsInOriginalTileSpace == null )
			return null;

		final Interval[] tileBoxesInFixedSpace = new Interval[] { tileBoxPair.getA().getBoundaries(), transformMovingTileBox( tileBoxPair, tileSlabMapping ) };
		final long[] originalMovingTileTopLeftCornerInFixedSpace = Intervals.minAsLongArray( IntervalsHelper.offset( tileBoxesInFixedSpace[ 1 ], Intervals.minAsLongArray( tileBoxPair.getB().getBoundaries() ) ) );
		final Interval[] originalTilesInFixedSpace = new Interval[] {
				new FinalInterval( tileBoxPair.getA().getOriginalTile().getSize() ),
				IntervalsHelper.translate( new FinalInterval( tileBoxPair.getB().getOriginalTile().getSize() ), originalMovingTileTopLeftCornerInFixedSpace ) };

		final Interval[] paddedOverlapsInOriginalTileSpace = new Interval[ 2 ];
		for ( int j = 0; j < 2; ++j )
			paddedOverlapsInOriginalTileSpace[ j ] = TileOperations.padInterval( overlapsInOriginalTileSpace[ j ], originalTilesInFixedSpace[ j ], padding );

		return paddedOverlapsInOriginalTileSpace;
	}
}
