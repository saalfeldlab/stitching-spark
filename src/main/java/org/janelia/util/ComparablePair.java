package org.janelia.util;

import net.imglib2.util.ValuePair;

/**
 * Holds an ordered pair of objects of arbitrary type that can be compared with each other.
 * Defines the ascending sorting order by the first element, and by the second if the first elements are equal.
 *
 * @author Igor Pisarev
 */

public class ComparablePair<
	A extends Comparable< ? super A >,
	B extends Comparable< ? super B > >
extends
	ValuePair< A, B >
implements
	Comparable< ComparablePair< ? extends A, ? extends B > >
{
	public ComparablePair( final A a, final B b )
	{
		super( a, b );
	}

	@Override
	public int compareTo( final ComparablePair< ? extends A, ? extends B > other )
	{
		final int compareFirst = a.compareTo( other.a );
		if ( compareFirst != 0)
			return compareFirst;

		return b.compareTo( other.b );
	}

	@Override
	public String toString()
	{
		return String.format( "(%s,%s)", a, b );
	}
}
