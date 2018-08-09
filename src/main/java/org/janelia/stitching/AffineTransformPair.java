package org.janelia.stitching;

import net.imglib2.realtransform.AffineGet;
import net.imglib2.util.Pair;

/**
 * Holds an ordered pair of {@link AffineGet} objects.
 *
 * @author Igor Pisarev
 */

public class AffineTransformPair implements Pair< AffineGet, AffineGet >
{
	private AffineGet[] affineTransformPair;

	public AffineTransformPair( final AffineGet t1, final AffineGet t2 )
	{
		affineTransformPair = new AffineGet[] { t1, t2 };
	}

	protected AffineTransformPair() { }

	@Override
	public AffineGet getA()
	{
		return affineTransformPair[ 0 ];
	}

	@Override
	public AffineGet getB()
	{
		return affineTransformPair[ 1 ];
	}

	public AffineGet[] toArray()
	{
		return affineTransformPair.clone();
	}

	public void swap()
	{
		final AffineGet tmp = affineTransformPair[ 0 ];
		affineTransformPair[ 0 ] = affineTransformPair[ 1 ];
		affineTransformPair[ 1 ] = tmp;
	}
}
