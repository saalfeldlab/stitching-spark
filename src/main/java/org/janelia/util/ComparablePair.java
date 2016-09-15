package org.janelia.util;

/**
 * Holds an ordered pair of objects of arbitrary type that can be compared with each other.
 * Defines the ascending sorting order by the first element, and by the second if the first elements are equal.
 *
 * @author Igor Pisarev
 */

public final class ComparablePair< A extends Comparable< ? super A >, B extends Comparable< ? super B > > implements Comparable< ComparablePair< ? extends A, ? extends B > >
{
	public A first;
	public B second;

	public ComparablePair( final A first, final B second )
	{
		this.first = first;
		this.second = second;
	}

	@Override
	public int compareTo( final ComparablePair< ? extends A, ? extends B > other )
	{
		final int compareFirst = first.compareTo( other.first );
		if ( compareFirst != 0)
			return compareFirst;

		return second.compareTo( other.second );
	}

	@Override
	public String toString()
	{
		return "(" + first.toString() + "," + second.toString() + ")";
	}
}
