/*
 * #%L
 * ImgLib2: a general-purpose, multidimensional image processing library.
 * %%
 * Copyright (C) 2009 - 2016 Tobias Pietzsch, Stephan Preibisch, Stephan Saalfeld,
 * John Bogovic, Albert Cardona, Barry DeZonia, Christian Dietz, Jan Funke,
 * Aivar Grislis, Jonathan Hale, Grant Harris, Stefan Helfrich, Mark Hiner,
 * Martin Horn, Steffen Jaensch, Lee Kamentsky, Larry Lindsey, Melissa Linkert,
 * Mark Longair, Brian Northan, Nick Perry, Curtis Rueden, Johannes Schindelin,
 * Jean-Yves Tinevez and Michael Zinsmaier.
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

package net.imglib2.neighborsearch;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.imglib2.KDIntervalTree;
import net.imglib2.KDIntervalTree.ValueWithId;
import net.imglib2.KDTreeNode;
import net.imglib2.RealInterval;
import net.imglib2.RealLocalizable;

public class OverlappingIntervalNeighborSearchOnKDIntervalTree< T > implements IntervalNeighborSearch< T >
{
	protected final IntervalNeighborSearchOnKDTree< ValueWithId< T > > intervalNeighborSearchOnCornerPointsKDTree;

	public OverlappingIntervalNeighborSearchOnKDIntervalTree( final KDIntervalTree< T > intervalTree )
	{
		intervalNeighborSearchOnCornerPointsKDTree = new IntervalNeighborSearchOnKDTree<>( intervalTree.getIntervalsCornerPointsKDTree() );
	}

	@Override
	public List< T > search( final RealInterval interval )
	{
		// find corners contained inside the queried interval
		final List< ValueWithId< T > > found = intervalNeighborSearchOnCornerPointsKDTree.search( interval );
		final Map< Integer, T > foundUnique = new LinkedHashMap<>();
		found.forEach( valueWithId -> foundUnique.put( valueWithId.id, valueWithId.value ) );

		// find intervals that fully contain the queried interval (and thus were not included in the above set because all their corners are outside the queried interval)
		final List< T > foundFullyContaining = findFullyContaining( interval );

		final List< T > result = new ArrayList<>();
		result.addAll( foundUnique.values() );
		result.addAll( foundFullyContaining );

		return result;
	}

	@Override
	public int numDimensions()
	{
		return intervalNeighborSearchOnCornerPointsKDTree.numDimensions();
	}

	protected List< T > findFullyContaining( final RealInterval interval )
	{
		// find points with coordinates smaller than min of the queried interval in all dimensions
		final List< ValueWithId< T > > foundLeft = new ArrayList<>();
		findLeft( intervalNeighborSearchOnCornerPointsKDTree.tree.getRoot(), interval, foundLeft );
		final Map< Integer, T > foundLeftUnique = new LinkedHashMap<>();
		foundLeft.forEach( valueWithId -> foundLeftUnique.put( valueWithId.id, valueWithId.value ) );

		// find points with coordinates larger than max of the queried interval in all dimensions
		final List< ValueWithId< T > > foundRight = new ArrayList<>();
		findRight( intervalNeighborSearchOnCornerPointsKDTree.tree.getRoot(), interval, foundRight );
		final Map< Integer, T > foundRightUnique = new LinkedHashMap<>();
		foundRight.forEach( valueWithId -> foundRightUnique.put( valueWithId.id, valueWithId.value ) );

		// intervals fully contain the queried interval when there is one point < min (top-left corner) and one point > max (bottom-right corner)
		final Map< Integer, T > foundUnique = new LinkedHashMap<>( foundLeftUnique );
		foundUnique.keySet().retainAll( foundRightUnique.keySet() );

		return new ArrayList<>( foundUnique.values() );
	}

	protected static < T > void findLeft( final KDTreeNode< ValueWithId< T > > current, final RealInterval interval, final List< ValueWithId< T > > foundLeft )
	{
		if ( testLeft( current, interval ) )
			foundLeft.add( current.get() );

		if ( current.left != null )
			findLeft( current.left, interval, foundLeft );

		if ( current.right != null && !testRight( current, interval, current.getSplitDimension() ) )
			findLeft( current.right, interval, foundLeft );
	}

	private static boolean testLeft( final RealLocalizable pos, final RealInterval interval )
	{
		for ( int d = 0; d < pos.numDimensions(); ++d )
			if ( !testLeft( pos, interval, d ) )
				return false;
		return true;
	}

	private static boolean testLeft( final RealLocalizable pos, final RealInterval interval, final int d )
	{
		return pos.getDoublePosition( d ) < interval.realMin( d );
	}

	protected static < T > void findRight( final KDTreeNode< ValueWithId< T > > current, final RealInterval interval, final List< ValueWithId< T > > foundRight )
	{
		if ( testRight( current, interval ) )
			foundRight.add( current.get() );

		if ( current.left != null && !testLeft( current, interval, current.getSplitDimension() ) )
			findRight( current.left, interval, foundRight );

		if ( current.right != null )
			findRight( current.right, interval, foundRight );
	}

	private static boolean testRight( final RealLocalizable pos, final RealInterval interval )
	{
		for ( int d = 0; d < pos.numDimensions(); ++d )
			if ( !testRight( pos, interval, d ) )
				return false;
		return true;
	}

	private static boolean testRight( final RealLocalizable pos, final RealInterval interval, final int d )
	{
		return pos.getDoublePosition( d ) > interval.realMax( d );
	}
}
