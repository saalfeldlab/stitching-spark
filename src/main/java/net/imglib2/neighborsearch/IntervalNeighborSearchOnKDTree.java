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
import java.util.List;

import net.imglib2.KDTree;
import net.imglib2.KDTreeNode;
import net.imglib2.RealInterval;
import net.imglib2.RealLocalizable;

public class IntervalNeighborSearchOnKDTree< T > implements IntervalNeighborSearch< T >
{
	protected KDTree< T > tree;
	protected final int n;

	public IntervalNeighborSearchOnKDTree( final KDTree< T > tree )
	{
		this.tree = tree;
		this.n = tree.numDimensions();
	}

	@Override
	public List< T > search( final RealInterval interval )
	{
		final List< T > found = new ArrayList<>();
		searchNode( tree.getRoot(), interval, found );
		return found;
	}

	@Override
	public int numDimensions()
	{
		return n;
	}

	protected void searchNode( final KDTreeNode< T > current, final RealInterval interval, final List< T > found )
	{
		if ( testWithinInterval( current, interval ) )
			found.add( current.get() );

		final boolean onlyLeft  = current.getSplitCoordinate() > interval.realMax( current.getSplitDimension() );
		final boolean onlyRight = current.getSplitCoordinate() < interval.realMin( current.getSplitDimension() );

		if ( current.left != null && ( onlyLeft || onlyLeft == onlyRight ) )
			searchNode( current.left, interval, found );

		if ( current.right != null && ( onlyRight || onlyLeft == onlyRight ) )
			searchNode( current.right, interval, found );
	}

	private static boolean testWithinInterval( final RealLocalizable pos, final RealInterval interval )
	{
		for ( int d = 0; d < pos.numDimensions(); ++d )
			if ( pos.getDoublePosition( d ) < interval.realMin( d ) || pos.getDoublePosition( d ) > interval.realMax( d ) )
				return false;
		return true;
	}
}
