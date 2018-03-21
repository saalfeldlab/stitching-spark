package org.janelia.fusion;

import org.janelia.stitching.TileInfo;

import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealLocalizable;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

public abstract class FusionStrategy< T extends RealType< T > & NativeType< T > >
{
	protected final RandomAccessibleInterval< T > out;

	public FusionStrategy( final Interval targetInterval, final T type )
	{
		this.out = new ArrayImgFactory< T >().create( targetInterval, type.createVariable() );
	}

	public FusionResult< T > getFusionResult()
	{
		return new FusionResult<>( out );
	}

	public abstract void setCursors( final Interval intersectionIntervalInTargetInterval );
	public abstract void moveCursorsForward();
	public abstract void updateValue( final TileInfo tile, final RealLocalizable pointInsideTile, final T value );
}