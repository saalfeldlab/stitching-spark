package org.janelia.fusion;

import org.janelia.stitching.TileInfo;

import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RealLocalizable;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

public class MaxIntensityFusionStrategy< T extends RealType< T > & NativeType< T > > extends FusionStrategy< T >
{
	private Cursor< T > outCursor;

	public MaxIntensityFusionStrategy( final Interval targetInterval, final T type )
	{
		super( targetInterval, type );
	}

	@Override
	public void setCursors( final Interval intersectionIntervalInTargetInterval )
	{
		outCursor = Views.flatIterable( Views.interval( out, intersectionIntervalInTargetInterval ) ).cursor();
	}

	@Override
	public void moveCursorsForward()
	{
		outCursor.fwd();
	}

	@Override
	public void updateValue( final TileInfo tile, final RealLocalizable pointInsideTile, final T value )
	{
		outCursor.get().setReal( Math.max( outCursor.get().getRealDouble(), value.getRealDouble() ) );
	}
}