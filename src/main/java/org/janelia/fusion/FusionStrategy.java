package org.janelia.fusion;

import org.janelia.stitching.TileInfo;

import net.imglib2.Interval;
import net.imglib2.RealLocalizable;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;

public abstract class FusionStrategy< T extends RealType< T > & NativeType< T > >
{
	protected final ImagePlusImg< T, ? > out;

	public FusionStrategy( final Interval targetInterval, final T type )
	{
		this.out = new ImagePlusImgFactory< T >().create( Intervals.dimensionsAsLongArray( targetInterval ), type.createVariable() );
	}

	public ImagePlusImg< T, ? > getOutImage()
	{
		return out;
	}

	public abstract void setCursors( final Interval intersectionIntervalInTargetInterval );
	public abstract void moveCursorsForward();
	public abstract void updateValue( final TileInfo tile, final RealLocalizable pointInsideTile, final T value );
}