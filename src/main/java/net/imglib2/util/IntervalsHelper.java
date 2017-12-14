package net.imglib2.util;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;

public class IntervalsHelper
{
	public static FinalInterval translate( final Interval interval, final long... translation )
	{
		final int n = interval.numDimensions();
		final long[] min = new long[ n ];
		final long[] max = new long[ n ];
		interval.min( min );
		interval.max( max );
		for ( int d = 0; d < translation.length; ++d )
		{
			min[ d ] += translation[ d ];
			max[ d ] += translation[ d ];
		}
		return new FinalInterval( min, max );
	}

	public static FinalInterval offset( final Interval interval, final long... offset )
	{
		final long[] translation = new long[ offset.length ];
		for ( int d = 0; d < translation.length; ++d )
			translation[ d ] = -offset[ d ];
		return translate( interval, translation );
	}
}
