package net.imglib2.util;

import net.imglib2.FinalRealInterval;
import net.imglib2.RealInterval;

public class RealIntervals
{
	/**
	 * Compute the intersection of two intervals.
	 *
	 * Create a {@link FinalRealInterval} , which is the intersection of the input
	 * intervals (i.e., the area contained in both input intervals).
	 *
	 * @param intervalA
	 *            input interval
	 * @param intervalB
	 *            input interval
	 * @return intersection of input intervals
	 */
	// TODO: PR to add this to net.imglib2.util.Intervals class
	public static FinalRealInterval intersectReal( final RealInterval intervalA, final RealInterval intervalB )
	{
		assert intervalA.numDimensions() == intervalB.numDimensions();

		final int n = intervalA.numDimensions();
		final double[] min = new double[ n ];
		final double[] max = new double[ n ];
		for ( int d = 0; d < n; ++d )
		{
			min[ d ] = Math.max( intervalA.realMin( d ), intervalB.realMin( d ) );
			max[ d ] = Math.min( intervalA.realMax( d ), intervalB.realMax( d ) );
		}
		return new FinalRealInterval( min, max );
	}
}
