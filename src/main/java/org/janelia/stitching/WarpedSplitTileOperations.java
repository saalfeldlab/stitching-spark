package org.janelia.stitching;

import java.util.ArrayList;
import java.util.List;

import org.janelia.stitching.analysis.FilterAdjacentShifts;

import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.util.IntervalsNullable;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class WarpedSplitTileOperations
{
	/**
	 * Returns list of overlapping tile box pairs.
	 * If {@code adjacent} is set, retains only pairs that are adjacent in the transformed space (overlap by more than 50%)
	 *
	 * @param tileBoxes
	 * @param adjacentOnly
	 * @param tileSlabMapping
	 * @return
	 */
	public static List< TilePair > findOverlappingTileBoxes( final List< TileInfo > tileBoxes, final boolean adjacentOnly, final TileSlabMapping tileSlabMapping )
	{
		final List< TilePair > overlappingBoxes = new ArrayList<>();
		for ( int i = 0; i < tileBoxes.size(); i++ )
		{
			for ( int j = i + 1; j < tileBoxes.size(); j++ )
			{
				final TilePair tileBoxPair = new TilePair( tileBoxes.get( i ), tileBoxes.get( j ) );
				if ( isOverlappingTileBoxPair( tileBoxPair, adjacentOnly, tileSlabMapping ) )
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
	public static boolean isOverlappingTileBoxPair( final TilePair tileBoxPair, final boolean adjacentOnly, final TileSlabMapping tileSlabMapping )
	{
		if ( tileBoxPair.getA().getOriginalTile().getIndex().intValue() != tileBoxPair.getB().getOriginalTile().getIndex().intValue() )
		{
			final Pair< Interval, Interval > transformedTileBoxPair = transformTileBoxPair( tileBoxPair, tileSlabMapping );
			final Interval tileBoxesOverlap = IntervalsNullable.intersect( transformedTileBoxPair.getA(), transformedTileBoxPair.getB() );
			if ( tileBoxesOverlap != null )
			{
				if ( !adjacentOnly || FilterAdjacentShifts.isAdjacent( SplitTileOperations.getMinTileDimensions( tileBoxPair ), tileBoxesOverlap ) )
					return true;
			}
		}
		return false;
	}

	/**
	 * Returns transformed tile box intervals for a pair of tile boxes.
	 *
	 * @param tileBoxPair
	 * @param tileSlabMapping
	 * @return
	 */
	public static Pair< Interval, Interval > transformTileBoxPair( final TilePair tileBoxPair, final TileSlabMapping tileSlabMapping )
	{
		return new ValuePair<>(
				transformTileBox( tileBoxPair.getA(), tileSlabMapping ),
				transformTileBox( tileBoxPair.getB(), tileSlabMapping )
			);
	}

	/**
	 * Returns a tile box interval in the global space.
	 * The center coordinate of the resulting interval is defined by transforming the middle point of the tile box.
	 *
	 * @param tileBox
	 * @param tileSlabMapping
	 * @return
	 */
	public static Interval transformTileBox( final TileInfo tileBox, final TileSlabMapping tileSlabMapping )
	{
		final InvertibleRealTransform tileTransform = WarpedTileOperations.getTileTransform( tileBox.getOriginalTile(), tileSlabMapping );
		final double[] tileBoxMiddlePoint = SplitTileOperations.getTileBoxMiddlePoint( tileBox );
		final double[] transformedTileBoxMiddlePoint = new double[ tileBoxMiddlePoint.length ];
		tileTransform.apply( tileBoxMiddlePoint, transformedTileBoxMiddlePoint );
		final RealInterval transformedTileBoxInterval = SplitTileOperations.getTileBoxInterval( transformedTileBoxMiddlePoint, tileBox.getSize() );
		return TileOperations.roundRealInterval( transformedTileBoxInterval );
	}
}
