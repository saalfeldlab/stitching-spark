package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.janelia.stitching.analysis.FilterAdjacentShifts;

import mpicbg.imglib.custom.OffsetConverter;
import mpicbg.models.Point;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.iterator.IntervalIterator;
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
	 * Returns list of overlapping tile box pairs.
	 * If {@code adjacent} is set, retains only pairs that are adjacent in the transformed space (overlap by more than 50%)
	 *
	 * @param tileBoxes
	 * @param adjacent
	 * @return
	 */
	public static List< TilePair > findOverlappingTileBoxes( final List< TileInfo > tileBoxes, final boolean adjacent )
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
					if ( tileBoxesOverlap != null )
					{
						if ( !adjacent || FilterAdjacentShifts.isAdjacent( getMinTileDimensions( tileBoxPair ), tileBoxesOverlap ) )
							overlappingBoxes.add( tileBoxPair );
					}
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
		final AffineTransform3D fixedTileTransform = TileOperations.getTileTransform( fixedTileBox.getOriginalTile() );
		return transformMovingTileBox( fixedTileTransform, movingTileBox );
	}

	/**
	 * Returns an interval of the moving tile box being transformed into coordinate space of the fixed original tile.
	 *
	 * @param fixedTileTransform
	 * @param movingTileBox
	 * @return
	 */
	public static Interval transformMovingTileBox( final AffineTransform3D fixedTileTransform, final TileInfo movingTileBox )
	{
		final double[] movingMiddlePoint = getTileBoxMiddlePoint( movingTileBox );
		final double[] movingInFixedMiddlePoint = new double[ movingMiddlePoint.length ];
		final AffineTransform3D movingTileTransform = TileOperations.getTileTransform( movingTileBox.getOriginalTile() );
		final AffineTransform3D movingToFixed = new AffineTransform3D();
		movingToFixed.preConcatenate( movingTileTransform ).preConcatenate( fixedTileTransform.inverse() );
		movingToFixed.apply( movingMiddlePoint, movingInFixedMiddlePoint );
		final RealInterval movingInFixedTileBoxRealInterval = getTileBoxInterval( movingInFixedMiddlePoint, movingTileBox.getSize() );
		return TileOperations.roundRealInterval( movingInFixedTileBoxRealInterval );
	}

	/**
	 * Returns overlap intervals of a given tile box pair in the coordinate space of each tile (useful for cropping).
	 *
	 * @param tileBoxPair
	 * @param padding
	 * @return
	 */
	public static Interval[] getOverlapIntervals( final TilePair tileBoxPair )
	{
		final Interval[] tileBoxesInFixedSpace = new Interval[] { tileBoxPair.getA().getBoundaries(), transformMovingTileBox( tileBoxPair ) };
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
	 * @return
	 */
	public static Interval[] getPaddedOverlapIntervals( final TilePair tileBoxPair, final long[] padding )
	{
		final Interval[] overlapsInOriginalTileSpace = getOverlapIntervals( tileBoxPair );
		if ( overlapsInOriginalTileSpace == null )
			return null;

		final Interval[] tileBoxesInFixedSpace = new Interval[] { tileBoxPair.getA().getBoundaries(), transformMovingTileBox( tileBoxPair ) };
		final long[] originalMovingTileTopLeftCornerInFixedSpace = Intervals.minAsLongArray( IntervalsHelper.offset( tileBoxesInFixedSpace[ 1 ], Intervals.minAsLongArray( tileBoxPair.getB().getBoundaries() ) ) );
		final Interval[] originalTilesInFixedSpace = new Interval[] {
				new FinalInterval( tileBoxPair.getA().getOriginalTile().getSize() ),
				IntervalsHelper.translate( new FinalInterval( tileBoxPair.getB().getOriginalTile().getSize() ), originalMovingTileTopLeftCornerInFixedSpace ) };

		final Interval[] paddedOverlapsInOriginalTileSpace = new Interval[ 2 ];
		for ( int j = 0; j < 2; ++j )
			paddedOverlapsInOriginalTileSpace[ j ] = TileOperations.padInterval( overlapsInOriginalTileSpace[ j ], originalTilesInFixedSpace[ j ], padding );

		return paddedOverlapsInOriginalTileSpace;
	}

	/**
	 * Returns overlap intervals of a given tile box pair in the coordinate space of each tile (useful for cropping).
	 * The overlaps are extended to capture the bounding box of a given search radius entirely.
	 *
	 * @param tileBoxPair
	 * @param searchRadius
	 * @return
	 */
	public static Interval[] getAdjustedOverlapIntervals( final TilePair tileBoxPair, final SearchRadius searchRadius )
	{
		final Interval originalFixedTileInFixedSpace = new FinalInterval( tileBoxPair.getA().getOriginalTile().getSize() );
		final long[] movingTileBoxPositionInsideTile = Intervals.minAsLongArray( tileBoxPair.getB().getBoundaries() );
		final Dimensions originalMovingTileDimensions = new FinalDimensions( tileBoxPair.getB().getOriginalTile().getSize() );

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

			// get test position of the moving tile box in the fixed space
			final long[] testMovingTileBoxPositionInFixedSpace = new long[ searchRadiusBoundingBox.numDimensions() ];
			for ( int d = 0; d < testMovingTileBoxPositionInFixedSpace.length; ++d )
				testMovingTileBoxPositionInFixedSpace[ d ] = ( cornersPos[ d ] == 0 ? searchRadiusBoundingBox.min( d ) : searchRadiusBoundingBox.max( d ) );

			// calculate new test position of the original moving tile
			final long[] testOriginalMovingTilePositionInFixedSpace = new long[ testMovingTileBoxPositionInFixedSpace.length ];
			for ( int d = 0; d < testOriginalMovingTilePositionInFixedSpace.length; ++d )
				testOriginalMovingTilePositionInFixedSpace[ d ] = testMovingTileBoxPositionInFixedSpace[ d ] - movingTileBoxPositionInsideTile[ d ];

			final Interval testOriginalMovingTileInFixedSpace = IntervalsHelper.translate( new FinalInterval( originalMovingTileDimensions ), testOriginalMovingTilePositionInFixedSpace );
			final Interval testOriginalTilesOverlapInFixedSpace = IntervalsNullable.intersect( originalFixedTileInFixedSpace, testOriginalMovingTileInFixedSpace );

			if ( testOriginalTilesOverlapInFixedSpace != null )
			{
				// update overlap corners in the fixed space
				for ( int d = 0; d < searchRadiusBoundingBox.numDimensions(); ++d )
				{
					overlapInFixedSpaceMin[ d ] = Math.min( testOriginalTilesOverlapInFixedSpace.min( d ), overlapInFixedSpaceMin[ d ] );
					overlapInFixedSpaceMax[ d ] = Math.max( testOriginalTilesOverlapInFixedSpace.max( d ), overlapInFixedSpaceMax[ d ] );
				}

				// calculate and update overlap corners in the moving space
				final Interval testOriginalTilesOverlapInMovingSpace = IntervalsHelper.offset( testOriginalTilesOverlapInFixedSpace, testOriginalMovingTilePositionInFixedSpace );
				for ( int d = 0; d < searchRadiusBoundingBox.numDimensions(); ++d )
				{
					overlapInMovingSpaceMin[ d ] = Math.min( testOriginalTilesOverlapInMovingSpace.min( d ), overlapInMovingSpaceMin[ d ] );
					overlapInMovingSpaceMax[ d ] = Math.max( testOriginalTilesOverlapInMovingSpace.max( d ), overlapInMovingSpaceMax[ d ] );
				}
			}
		}

		for ( int d = 0; d < searchRadiusBoundingBox.numDimensions(); ++d )
			if ( ( overlapInFixedSpaceMin[ d ] == Long.MAX_VALUE || overlapInFixedSpaceMax[ d ] == Long.MIN_VALUE ) || ( overlapInMovingSpaceMin[ d ] == Long.MAX_VALUE || overlapInMovingSpaceMax[ d ] == Long.MIN_VALUE ) )
				return null;

		final Interval[] adjustedOverlapsInOriginalTileSpace = new Interval[ 2 ];
		adjustedOverlapsInOriginalTileSpace[ 0 ] = new FinalInterval( overlapInFixedSpaceMin, overlapInFixedSpaceMax );
		adjustedOverlapsInOriginalTileSpace[ 1 ] = new FinalInterval( overlapInMovingSpaceMin, overlapInMovingSpaceMax );
		return adjustedOverlapsInOriginalTileSpace;
	}

	/**
	 * Returns helper object containing required offset values for both tiles to be able to compute the shift vector.
	 *
	 * @param tileBoxPair
	 * @param overlapsInOriginalTileSpace
	 * @return
	 */
	public static OffsetConverter getOffsetConverter( final TilePair tileBoxPair, final Interval[] overlapsInOriginalTileSpace )
	{
		final int dim = tileBoxPair.getA().numDimensions();

		final long[][] roiToOriginalTileOffset = new long[ 2 ][];
		for ( int i = 0; i < 2; ++i )
		{
			roiToOriginalTileOffset[ i ] = new long[ dim ];
			for ( int d = 0; d < dim; ++d )
				roiToOriginalTileOffset[ i ][ d ] = overlapsInOriginalTileSpace[ i ].min( d );
		}

		final double[] originalTileOffsetToMovingTileBox = Intervals.minAsDoubleArray( tileBoxPair.getB().getBoundaries() );

		return new FinalOffsetConverter( roiToOriginalTileOffset, originalTileOffsetToMovingTileBox );
	}

	/**
	 * Creates a point pair that can be used as {@link PointMatch} between the fixed tile and the moving tile using center point of the fixed tile box.
	 *
	 * @param tileBoxPair
	 * @param originalTileOffset
	 * @return
	 */
	public static PointPair createPointPair( final TilePair tileBoxPair, final double[] originalTileOffset )
	{
		// create point pair using center point of each tile box
		final Point fixedTileBoxCenterPoint = new Point( getTileBoxMiddlePoint( tileBoxPair.getA() ) );
		final double[] movingTileBoxCenter = new double[ originalTileOffset.length ];
		for ( int d = 0; d < movingTileBoxCenter.length; ++d )
			movingTileBoxCenter[ d ] = fixedTileBoxCenterPoint.getL()[ d ] - originalTileOffset[ d ];
		final Point movingTileBoxCenterPoint = new Point( movingTileBoxCenter );
		final PointPair pointPair = new PointPair( fixedTileBoxCenterPoint, movingTileBoxCenterPoint );
		return pointPair;
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
}
