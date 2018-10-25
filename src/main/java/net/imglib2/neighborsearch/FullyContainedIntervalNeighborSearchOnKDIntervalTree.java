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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.imglib2.KDIntervalTree;
import net.imglib2.RealInterval;

public class FullyContainedIntervalNeighborSearchOnKDIntervalTree< T > implements IntervalNeighborSearch< T >
{
	protected final IntervalNeighborSearchOnKDTree< KDIntervalTree< T >.ValueWithId > intervalNeighborSearchOnCornerPointsKDTree;
	protected final int numFullyContainedCornerPoints;

	public FullyContainedIntervalNeighborSearchOnKDIntervalTree( final KDIntervalTree< T > intervalTree )
	{
		intervalNeighborSearchOnCornerPointsKDTree = new IntervalNeighborSearchOnKDTree<>( intervalTree.getIntervalsCornerPointsKDTree() );
		numFullyContainedCornerPoints = 1 << numDimensions();
	}

	@Override
	public List< T > search( final RealInterval interval )
	{
		final List< KDIntervalTree< T >.ValueWithId > found = intervalNeighborSearchOnCornerPointsKDTree.search( interval );
		final Map< Integer, T > foundUnique = new LinkedHashMap<>();
		found.forEach( valueWithId -> foundUnique.put( valueWithId.id, valueWithId.value ) );

		// count how many times each id occurs in the resulting set
		final Map< Integer, Integer > foundIdToCount = new HashMap<>();
		found.forEach( valueWithId -> foundIdToCount.put( valueWithId.id, foundIdToCount.getOrDefault( valueWithId.id, 0 ) + 1 ) );

		// filter entries whose number of occurrences is equal to total number of corners
		final Stream< T > foundFullyContainedStream = foundUnique.keySet().stream().filter( id -> foundIdToCount.get( id ) == numFullyContainedCornerPoints ).map( id -> foundUnique.get( id ) );
		return new ArrayList<>( foundFullyContainedStream.collect( Collectors.toList() ) );
	}

	@Override
	public int numDimensions()
	{
		return intervalNeighborSearchOnCornerPointsKDTree.numDimensions();
	}
}
