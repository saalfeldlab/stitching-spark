package org.janelia.fusion;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

public class FusionResult< T extends RealType< T > & NativeType< T > >
{
	private final RandomAccessibleInterval< T > out;

	public FusionResult( final RandomAccessibleInterval< T > out )
	{
		this.out = out;
	}

	public RandomAccessibleInterval< T > getOutImage()
	{
		return out;
	}
}
