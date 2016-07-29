package org.janelia.stitching;

import net.imglib2.AbstractInterval;

/**
 * @author pisarevi
 *
 */
public class Boundaries extends AbstractInterval
{
	public Boundaries( final int dim )
	{
		super( dim );
	}

	public long[] getMin()
	{
		return min.clone();
	}

	public long[] getMax()
	{
		return max.clone();
	}

	public void setMin( final int d, final long val )
	{
		min[ d ] = val;
	}

	public void setMax( final int d, final long val )
	{
		max[ d ] = val;
	}

	public long[] getDimensions()
	{
		final long[] ret = new long[ numDimensions() ];
		dimensions( ret );
		return ret;
	}

	public boolean validate()
	{
		for ( int d = 0; d < numDimensions(); d++ )
			if ( dimension( d ) <= 0 )
				return false;
		return true;
	}
}
