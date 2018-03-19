package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mpicbg.imglib.custom.OffsetConverter;
import net.imglib2.Dimensions;
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
	 *
	 * @param tileBoxPair
	 * @param searchRadius
	 * @return
	 */
	public static Pair< Interval, Interval > getAdjustedOverlapIntervals( final Pair< Interval, Interval > tileBoxPair, final SearchRadius searchRadius )
	{
		final Interval fixedInterval = tileBoxPair.getA(), movingInterval = tileBoxPair.getB();

		if ( !Views.isZeroMin( fixedInterval ) )
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
			final Interval testMovingIntervalInFixedSpace = IntervalsHelper.setPosition( movingInterval, testMovingPositionInFixedSpace );

			// get overlap between tile boxes in the fixed space
			final Interval testOverlapInFixedSpace = IntervalsNullable.intersect( fixedInterval, testMovingIntervalInFixedSpace );

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
	 * @param globalOffset
	 * @return
	 */
	public static OffsetConverter getOffsetConverter( final Pair< Interval, Interval > overlaps )
	{
		return getOffsetConverter( overlaps, null );
	}
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
	 * Creates a point pair that can be used as {@link PointMatch} between the fixed tile and the moving tile using center point of the fixed tile box.
	 *
	 * @param tileBoxPair
	 * @param originalTileOffset
	 * @return
	 */
	/*public static PointPair createPointPair( final TilePair tileBoxPair, final double[] originalTileOffset )
	{
		final double[] fixedTileBoxCenter = getTileBoxMiddlePoint( tileBoxPair.getA() );
		final double[] movingTileBoxCenter = getTileBoxMiddlePoint( tileBoxPair.getB() );

		final double[] movingTileBoxCenterInFixedSpace = new double[ movingTileBoxCenter.length ];
		for ( int d = 0; d < movingTileBoxCenterInFixedSpace.length; ++d )
			movingTileBoxCenterInFixedSpace[ d ] = fixedTileBoxCenter[ d ];

		PointMatch

		return new PointPair(
				new Point( fixedTileBoxCenter ),
				new Point( movingTileBoxCenterInFixedSpace )
			);
	}*/

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
