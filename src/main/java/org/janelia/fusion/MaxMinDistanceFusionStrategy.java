package org.janelia.fusion;

import org.janelia.stitching.TileInfo;

import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealLocalizable;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class MaxMinDistanceFusionStrategy< T extends RealType< T > & NativeType< T > > extends FusionStrategy< T >
{
	private final RandomAccessibleInterval< FloatType > maxMinDistances;

	private Cursor< FloatType > maxMinDistancesCursor;
	private Cursor< T > outCursor;

	public MaxMinDistanceFusionStrategy( final Interval targetInterval, final T type )
	{
		super( targetInterval, type );
		this.maxMinDistances = ArrayImgs.floats( Intervals.dimensionsAsLongArray( targetInterval ) );
	}

	@Override
	public void setCursors( final Interval intersectionIntervalInTargetInterval )
	{
		maxMinDistancesCursor = Views.flatIterable( Views.interval( maxMinDistances, intersectionIntervalInTargetInterval ) ).cursor();
		outCursor = Views.flatIterable( Views.interval( out, intersectionIntervalInTargetInterval ) ).cursor();
	}

	@Override
	public void moveCursorsForward()
	{
		maxMinDistancesCursor.fwd();
		outCursor.fwd();
	}

	@Override
	public void updateValue( final TileInfo tile, final RealLocalizable pointInsideTile, final T value )
	{
		double minDistance = Double.MAX_VALUE;
		for ( int d = 0; d < pointInsideTile.numDimensions(); ++d )
		{
			final double dist = Math.min(
					pointInsideTile.getDoublePosition( d ),
					tile.getSize( d ) - 1 - pointInsideTile.getDoublePosition( d )
				);
			minDistance = Math.min( dist, minDistance );
		}
		if ( minDistance >= maxMinDistancesCursor.get().getRealDouble() )
		{
			maxMinDistancesCursor.get().setReal( minDistance );
			outCursor.get().set( value );
		}
	}
}