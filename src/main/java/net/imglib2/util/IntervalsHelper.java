package net.imglib2.util;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;

public class IntervalsHelper
{
	public static Interval translate( final Interval interval, final long[] offset )
	{
		final int n = interval.numDimensions();
		final long[] min = new long[ n ];
		final long[] max = new long[ n ];
		interval.min( min );
		interval.max( max );
		for ( int d = 0; d < n; ++d )
		{
			final long t = offset[ d ];
			min[ d ] += t;
			max[ d ] += t;
		}
		return new FinalInterval( min, max );
	}
}
