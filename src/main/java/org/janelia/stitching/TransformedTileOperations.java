package org.janelia.stitching;

import java.util.Arrays;

import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.Translation;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class TransformedTileOperations
{
	/**
	 * Returns tile transform, that is its affine transform if not null, or translation transform to the stage position of the tile otherwise (if allowed by the boolean flag).
	 *
	 * @param tile
	 * @param worldTransformOnly
	 * @return
	 */
	public static AffineGet getTileTransform( final TileInfo tile, final boolean worldTransformOnly )
	{
		final AffineGet ret;
		if ( tile.getTransform() != null )
			ret = tile.getTransform();
		else if ( !worldTransformOnly )
			ret = new Translation( tile.getStagePosition() );
		else
			ret = null;
		return ret;
	}

	/**
	 * Estimates the bounding box of a transformed (or shifted) tile.
	 *
	 * @param tile
	 * @param worldTransformOnly
	 * @return
	 */
	public static Interval getTransformedBoundingBox( final TileInfo tile, final boolean worldTransformOnly )
	{
		final AffineGet tileTransform = getTileTransform( tile, worldTransformOnly );
		return tileTransform != null ? getTransformedBoundingBox( new FinalInterval( tile.getSize() ), tileTransform ) : null;
	}

	/**
	 * Estimates the bounding box of a transformed interval inside a tile.
	 *
	 * @param interval
	 * @param transform
	 * @return
	 */
	public static Interval getTransformedBoundingBox( final RealInterval interval, final InvertibleRealTransform transform )
	{
		return Intervals.smallestContainingInterval( getTransformedBoundingBoxReal( interval, transform ) );
	}

	/**
	 * Estimates the bounding box of a transformed interval inside a tile.
	 *
	 * @param interval
	 * @param transform
	 * @return
	 */
	public static RealInterval getTransformedBoundingBoxReal( final RealInterval interval, final InvertibleRealTransform transform )
	{
		final double[] transformedMin = new double[ interval.numDimensions() ], transformedMax = new double[ interval.numDimensions() ];
		Arrays.fill( transformedMin, Double.POSITIVE_INFINITY );
		Arrays.fill( transformedMax, Double.NEGATIVE_INFINITY );

		final double[] cornerPosition = new double[ interval.numDimensions() ], transformedCornerPosition = new double[ interval.numDimensions() ];

		final int[] cornerDimensions = new int[ interval.numDimensions() ];
		Arrays.fill( cornerDimensions, 2 );
		final IntervalIterator cornerIterator = new IntervalIterator( cornerDimensions );

		while ( cornerIterator.hasNext() )
		{
			cornerIterator.fwd();
			for ( int d = 0; d < interval.numDimensions(); ++d )
				cornerPosition[ d ] = cornerIterator.getIntPosition( d ) == 0 ? interval.realMin( d ) : interval.realMax( d );

			transform.apply( cornerPosition, transformedCornerPosition );

			for ( int d = 0; d < interval.numDimensions(); ++d )
			{
				transformedMin[ d ] = Math.min( transformedCornerPosition[ d ], transformedMin[ d ] );
				transformedMax[ d ] = Math.max( transformedCornerPosition[ d ], transformedMax[ d ] );
			}
		}

		return new FinalRealInterval( transformedMin, transformedMax );
	}

	/**
	 * Returns transformed intervals for a pair of subtiles.
	 *
	 * @param subTilePair
	 * @param worldTransformOnly
	 * @return
	 */
	public static Pair< Interval, Interval > transformSubTilePair( final SubTilePair subTilePair, final boolean worldTransformOnly )
	{
		return new ValuePair<>(
				transformSubTile( subTilePair.getA(), worldTransformOnly ),
				transformSubTile( subTilePair.getB(), worldTransformOnly )
			);
	}

	/**
	 * Returns transformed subtile interval in the global space.
	 * The center coordinate of the resulting interval is defined by transforming the middle point of the subtile.
	 *
	 * @param subTile
	 * @param worldTransformOnly
	 * @return
	 */
	public static Interval transformSubTile( final SubTile subTile, final boolean worldTransformOnly )
	{
		final AffineGet tileTransform = getTileTransform( subTile.getFullTile(), worldTransformOnly );
		return tileTransform != null ? transformSubTile( subTile, tileTransform ) : null;
	}

	/**
	 * Returns transformed subtile interval in the global space.
	 * The center coordinate of the resulting interval is defined by transforming the middle point of the subtile.
	 *
	 * @param subTile
	 * @param fullTileTransform
	 * @return
	 */
	public static Interval transformSubTile( final SubTile subTile, final RealTransform fullTileTransform )
	{
		final RealInterval transformedSubTileInterval = SubTileOperations.getSubTileInterval(
				transformSubTileMiddlePoint( subTile, fullTileTransform ),
				Intervals.dimensionsAsLongArray( subTile )
			);
		return TileOperations.roundRealInterval( transformedSubTileInterval );
	}

	/**
	 * Transforms the middle point of the given subtile.
	 *
	 * @param subTile
	 * @param worldTransformOnly
	 * @return
	 */
	public static double[] transformSubTileMiddlePoint( final SubTile subTile, final boolean worldTransformOnly )
	{
		final AffineGet tileTransform = getTileTransform( subTile.getFullTile(), worldTransformOnly );
		return tileTransform != null ? transformSubTileMiddlePoint( subTile, tileTransform ) : null;
	}

	/**
	 * Transforms middle point of a given tile box.
	 *
	 * @param subTile
	 * @param fullTileTransform
	 * @return
	 */
	public static double[] transformSubTileMiddlePoint( final SubTile subTile, final RealTransform fullTileTransform )
	{
		final double[] transformedSubTileMiddlePoint = new double[ subTile.numDimensions() ];
		fullTileTransform.apply( SubTileOperations.getSubTileMiddlePoint( subTile ), transformedSubTileMiddlePoint );
		return transformedSubTileMiddlePoint;
	}
}
