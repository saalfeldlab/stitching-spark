package net.imglib2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.imglib2.iterator.IntervalIterator;

public class KDIntervalTree< T > implements EuclideanSpace
{
	public final static class ValueWithId< T >
	{
		public final T value;
		public final int id;

		public ValueWithId( final T value, final int id )
		{
			this.value = value;
			this.id = id;
		}
	}

	protected final KDTree< ValueWithId< T > > kdTree;
	protected final int size;
	protected final int n;

	/**
	 * Construct a KDIntervalTree from the elements in the given list.
	 *
	 * <p>
	 * Note that the constructor can be called with the same list for both
	 * {@code values == positions} if {@code T extends RealInterval}.
	 * </p>
	 *
	 * @param values
	 *            a list of values
	 * @param intervals
	 *            a list of intervals corresponding to the values
	 */
	public KDIntervalTree( final List< T > values, final List< ? extends RealInterval > intervals )
	{
		assert values.size() == intervals.size();

		// test that dimensionality is preserved
		n = intervals.iterator().next().numDimensions();
		assert ( verifyDimensions( intervals, n ) );

		final List< ValueWithId< T > > cornerValuesWithId = new ArrayList<>();
		final List< RealPoint > corners = new ArrayList<>();

		// generate points at every corner of each interval
		final int[] cornerIteratorDimensions = new int[ n ];
		Arrays.fill( cornerIteratorDimensions, 2 );
		final IntervalIterator cornerIterator = new IntervalIterator( cornerIteratorDimensions );
		final double[] intervalCornerPosition = new double[ n ];
		for ( int i = 0; i < Math.max( values.size(), intervals.size() ); ++i )
		{
			final T value = values.get( i );
			final RealInterval interval = intervals.get( i );

			cornerIterator.reset();
			while ( cornerIterator.hasNext() )
			{
				cornerIterator.fwd();
				for ( int d = 0; d < n; ++d )
					intervalCornerPosition[ d ] = cornerIterator.getIntPosition( d ) == 0 ? interval.realMin( d ) : interval.realMax( d );

				corners.add( new RealPoint( intervalCornerPosition ) );
				cornerValuesWithId.add( new ValueWithId<>( value, i ) );
			}
		}

		kdTree = new KDTree<>( cornerValuesWithId, corners );
		size = intervals.size();
	}

	/**
	 * Check whether all intervals in the intervals list have dimension n.
	 *
	 * @return true, if all intervals have dimension n.
	 */
	protected static boolean verifyDimensions( final List< ? extends RealInterval > intervals, final int n )
	{
		for ( final RealInterval interval : intervals )
			if ( interval.numDimensions() != n )
				return false;
		return true;
	}

	public long size()
	{
		return size;
	}

	public KDTree< ValueWithId< T > > getIntervalsCornerPointsKDTree()
	{
		return kdTree;
	}

	@Override
	public int numDimensions()
	{
		return n;
	}
}
