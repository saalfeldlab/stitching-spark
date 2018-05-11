package net.imglib2.util;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RealInterval;

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

	public static FinalInterval setPosition( final Interval interval, final long... position )
	{
		return translate( new FinalInterval( Intervals.dimensionsAsLongArray( interval ) ), position );
	}

	public static Interval roundRealInterval( final RealInterval realInterval )
	{
		final long[] min = new long[ realInterval.numDimensions() ], max = new long[ realInterval.numDimensions() ];
		for ( int d = 0; d < realInterval.numDimensions(); ++d )
		{
			min[ d ] = Math.round( realInterval.realMin( d ) );
			max[ d ] = Math.round( realInterval.realMax( d ) );
		}
		return new FinalInterval( min, max );
	}
}
