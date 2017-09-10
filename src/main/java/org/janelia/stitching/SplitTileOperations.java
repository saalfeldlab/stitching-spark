package org.janelia.stitching;

import java.util.ArrayList;
import java.util.List;

import org.janelia.stitching.analysis.FilterAdjacentShifts;

import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;
import net.imglib2.util.IntervalsNullable;

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
			final Interval zeroMinTileInterval = new FinalInterval( tile.getSize() );
			final List< TileInfo > splitTile = TileOperations.divideSpaceByCount( zeroMinTileInterval, gridSize );
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
	 * Returns list of tile box pairs that are adjacent (overlap by more than 50%) in transformed space.
	 *
	 * @param tileBoxes
	 * @return
	 */
	public static List< TilePair > findOverlappingTileBoxes( final List< TileInfo > tileBoxes )
	{
		final List< TilePair > overlappingBoxes = new ArrayList<>();
		for ( int i = 0; i < tileBoxes.size(); i++ )
		{
			for ( int j = i + 1; j < tileBoxes.size(); j++ )
			{
				if ( tileBoxes.get( i ).getOriginalTile().getIndex().intValue() != tileBoxes.get( j ).getOriginalTile().getIndex().intValue() )
				{
					final TilePair tileBoxPair = new TilePair( tileBoxes.get( i ), tileBoxes.get( j ) );
					final Interval fixedTileBoxInterval = tileBoxPair.getA().getBoundaries();
					final Interval movingInFixedTileBoxInterval = transformMovingTileBox( tileBoxPair );
					final Interval tileBoxesOverlap = IntervalsNullable.intersect( fixedTileBoxInterval, movingInFixedTileBoxInterval );
					if ( tileBoxesOverlap != null && FilterAdjacentShifts.isAdjacent( getMinTileDimensions( tileBoxPair ), tileBoxesOverlap ) )
						overlappingBoxes.add( tileBoxPair );
				}
			}
		}
		return overlappingBoxes;
	}

	/**
	 * Returns an interval of the moving tile box being transformed into coordinate space of the fixed original tile.
	 * @param tileBoxPair
	 * @return
	 */
	public static Interval transformMovingTileBox( final TilePair tileBoxPair )
	{
		final TileInfo fixedTileBox = tileBoxPair.getA(), movingTileBox = tileBoxPair.getB();
		final double[] movingMiddlePoint = getTileBoxMiddlePoint( movingTileBox );
		final double[] movingInFixedMiddlePoint = new double[ movingMiddlePoint.length ];
		final AffineTransform3D fixedTileTransform = TileOperations.getTileTransform( fixedTileBox.getOriginalTile() );
		final AffineTransform3D movingTileTransform = TileOperations.getTileTransform( movingTileBox.getOriginalTile() );
		final AffineTransform3D movingToFixed = new AffineTransform3D();
		movingToFixed.preConcatenate( movingTileTransform ).preConcatenate( fixedTileTransform.inverse() );
		movingToFixed.apply( movingMiddlePoint, movingInFixedMiddlePoint );
		final RealInterval movingInFixedTileBoxRealInterval = getTileBoxInterval( movingInFixedMiddlePoint, movingTileBox.getSize() );
		return TileOperations.roundRealInterval( movingInFixedTileBoxRealInterval );
	}

	/**
	 * Returns overlap intervals tile box pair in the coordinate space of each tile (useful for cropping).
	 * The overlaps are extended to capture the bounding box of a given search radius entirely.
	 *
	 * @param tileBoxPair
	 * @param searchRadius
	 * @return
	 */
	public static Interval[] getOverlapIntervals( final TilePair tileBoxPair, final SearchRadius searchRadius )
	{
		final Interval searchRadiusBoundingBox = Intervals.smallestContainingInterval( searchRadius.getBoundingBox() );
		final long[] padding = new long[ searchRadiusBoundingBox.numDimensions() ];
		for ( int d = 0; d < padding.length; ++d )
			padding[ d ] = searchRadiusBoundingBox.dimension( d ) / 2;
		return getOverlapIntervals( tileBoxPair, padding );
	}

	/**
	 * Returns overlap intervals tile box pair in the coordinate space of each tile (useful for cropping).
	 * The overlaps are extended by a given padding value.
	 *
	 * @param tileBoxPair
	 * @param padding
	 * @return
	 */
	public static Interval[] getOverlapIntervals( final TilePair tileBoxPair, final long[] padding )
	{
		final Interval[] tileBoxesInFixedSpace = new Interval[] { tileBoxPair.getA().getBoundaries(), transformMovingTileBox( tileBoxPair ) };
		final Interval overlapInFixedSpace = IntervalsNullable.intersect( tileBoxesInFixedSpace[ 0 ], tileBoxesInFixedSpace[ 1 ] );
		if ( overlapInFixedSpace == null )
			throw new RuntimeException( "boxes do not overlap" );

		final long[] originalMovingTileTopLeftCornerInFixedSpace = Intervals.minAsLongArray( IntervalsHelper.offset( tileBoxesInFixedSpace[ 1 ], Intervals.minAsLongArray( tileBoxPair.getB().getBoundaries() ) ) );
		final Interval[] originalTilesInFixedSpace = new Interval[] {
				new FinalInterval( tileBoxPair.getA().getOriginalTile().getSize() ),
				IntervalsHelper.translate( new FinalInterval( tileBoxPair.getB().getOriginalTile().getSize() ), originalMovingTileTopLeftCornerInFixedSpace ) };

		final Interval[] overlapsInOriginalTileSpace = new Interval[ 2 ];
		for ( int j = 0; j < 2; j++ )
			overlapsInOriginalTileSpace[ j ] = IntervalsHelper.offset( overlapInFixedSpace, Intervals.minAsLongArray( originalTilesInFixedSpace[ j ] ) );

		final Interval[] paddedOverlapsInOriginalTileSpace = new Interval[ 2 ];
		for ( int j = 0; j < 2; ++j )
			paddedOverlapsInOriginalTileSpace[ j ] = TileOperations.padInterval(
					overlapsInOriginalTileSpace[ j ],
					originalTilesInFixedSpace[ j ],
					padding
				);
		return paddedOverlapsInOriginalTileSpace;
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
			max[ d ] = middlePoint[ d ] + 0.5 * boxSize[ d ];
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
}
