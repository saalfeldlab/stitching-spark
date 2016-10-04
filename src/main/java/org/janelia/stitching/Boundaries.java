package org.janelia.stitching;

import net.imglib2.AbstractInterval;
import net.imglib2.Dimensions;
import net.imglib2.Interval;

/**
 * Represents a box in n-dimensional space.
 * Primarily used as a bounding box of a set of tiles.
 *
 * @author Igor Pisarev
 */

public class Boundaries extends AbstractInterval
{
	public Boundaries( final int dim )
	{
		super( dim );
	}
	public Boundaries( final Interval interval )
	{
		super( interval);
	}
	public Boundaries( final Dimensions dimensions )
	{
		super( dimensions );
	}
	public Boundaries( final long[] min, final long[] max )
	{
		super( min, max );
	}
	public Boundaries( final long[] dimensions )
	{
		super( dimensions );
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
