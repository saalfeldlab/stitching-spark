/*-
 * #%L
 * Software for the reconstruction of multi-view microscopic acquisitions
 * like Selective Plane Illumination Microscopy (SPIM) Data.
 * %%
 * Copyright (C) 2012 - 2017 Multiview Reconstruction developers.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */
package net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mpicbg.spim.data.generic.base.Entity;
import mpicbg.spim.data.generic.sequence.AbstractSequenceDescription;
import mpicbg.spim.data.generic.sequence.BasicViewDescription;
import mpicbg.spim.data.sequence.Channel;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class Group< V > implements Iterable< V >
{
	protected Set< V > views;

	public Group( final Collection< V > views )
	{
		this.views = new HashSet<>();
		this.views.addAll( views );
	}

	public Group( final V view )
	{
		this.views = new HashSet<>();
		this.views.add( view );
	}

	public Group()
	{
		this.views = new HashSet<>();
	}

	public Set< V > getViews() { return views; }
	public int size() { return views.size(); }
	public boolean contains( final V view ) { return views.contains( view ); }

	@Override
	public Iterator< V > iterator() { return views.iterator(); }

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ( ( views == null ) ? 0 : views.hashCode() );
		return result;
	}

	@Override
	public boolean equals( final Object obj )
	{
		if ( this == obj )
			return true;

		if ( obj == null )
			return false;

		if ( getClass() != obj.getClass() )
			return false;

		final Group< ? > other = (Group< ? >) obj;

		if ( views == null )
		{
			if ( other.views != null )
				return false;
		}
		else if ( !views.equals( other.views ) )
		{
			return false;
		}

		return true;
	}

	@Override
	public String toString() { return gvids( this ); }

	public void filterMissingViews( final Set< ? extends V > missingViews )
	{
		filterMissingViews( this, missingViews );
	}

	public static < V > void filterMissingViews( final Group< V > group, final Set< ? extends V > missingViews )
	{
		for ( final V viewId : new ArrayList<>( group.getViews() ) )
		{
			if ( missingViews.contains( viewId ) )
				group.getViews().remove( viewId );
		}
	}

	public static < V extends ViewId > List< V > getViewsSorted( final Collection< V > views )
	{
		final ArrayList< V > sorted = new ArrayList<>();
		sorted.addAll( views );
		Collections.sort( sorted );
		return sorted;
	}

	public static < V extends ViewId > ArrayList< Group< V > > getGroupsSorted( final Collection< Group< V > > groups )
	{
		final ArrayList< Group< V > > sorted = new ArrayList<>();
		sorted.addAll( groups );

		Collections.sort( sorted, new Comparator< Group< V > >()
		{
			@Override
			public int compare( final Group< V > o1, final Group< V > o2 )
			{
				return Group.getViewsSorted( o1.getViews() ).get( 0 ).compareTo( Group.getViewsSorted( o2.getViews() ).get( 0 ) );
			}
		} );

		return sorted;
	}

	/**
	 * Combining means to combine all views that share everything except what they are grouped after, e.g. channel
	 * (for example used to group GFP, RFP, DAPI for a Tile)
	 *
	 * v1: angle 0, channel 0
	 * v2: angle 1, channel 0
	 * v3: angle 1, channel 1
	 *
	 * combine by channel: [v1], [v2 v3]
	 *
	 * @param vds - the ViewDescriptions to process
	 * @param groupingFactors - by which attribute(s)
	 * @param <V> - something that extends {@link BasicViewDescription}
	 * @return - list of groups
	 */
	public static < V extends BasicViewDescription< ? > > List< Group< V > > combineBy(final List<V> vds,
			final Set<Class<? extends Entity>> groupingFactors)
	{
		return combineOrSplitBy( vds, groupingFactors, true );
	}

	/**
	 * Split means to make a group for every instance of an attribute, e.g. channel
	 * (for example group everthing that is GFP and everything that it RFP)
	 *
	 * v1: angle 0, channel 0
	 * v2: angle 1, channel 0
	 * v3: angle 1, channel 1
	 *
	 * split by channel: [v1 v2], [v3]
	 *
	 * @param vds - the ViewDescriptions to process
	 * @param groupingFactors - by which attribute(s)
	 * @param <V> - something that extends {@link BasicViewDescription}
	 * @return - list of groups
	 */

	public static < V extends BasicViewDescription< ? > > List<Group<V>> splitBy(final List<V> vds,
			final Set<Class<? extends Entity>> groupingFactors)
	{
		return combineOrSplitBy( vds, groupingFactors, false );
	}

	/**
	 * Combining means to combine all views that share everything except what they are grouped after, e.g. channel
	 * (for example used to group GFP, RFP, DAPI for a Tile)
	 *
	 * Split means the opposite in some way, make a group for every instance of an attribute, e.g. channel
	 * (for example group everthing that is GFP and everything that it RFP)
	 *
	 * v1: angle 0, channel 0
	 * v2: angle 1, channel 0
	 * v3: angle 1, channel 1
	 *
	 * combine by channel: [v1], [v2 v3]
	 * split by channel: [v1 v2], [v3]
	 *
	 * @param vds - the ViewDescriptions to process
	 * @param groupingFactors - by which attribute(s)
	 * @param combine - combine or split
	 * @param <V> - something that extends {@link BasicViewDescription}
	 * @return - list of groups
	 */
	public static < V extends BasicViewDescription< ? > > List<Group<V>> combineOrSplitBy(final List<V> vds,
			final Set<Class<? extends Entity>> groupingFactors, final boolean combine)
	{
		final Map<List<Entity>, Group<V>> res = new HashMap<>();

		// pre-sort vd List
		Collections.sort( vds );


		for (final V vd : vds) {
			final List<Entity> key = new ArrayList<>();

			if ((combine && !groupingFactors.contains(TimePoint.class)) || (!combine && groupingFactors.contains(TimePoint.class))) {
				key.add(vd.getTimePoint());
			}

			for (final Entity e : vd.getViewSetup().getAttributes().values()) {
				if ((combine && !groupingFactors.contains(e.getClass())) || (!combine && groupingFactors.contains(e.getClass())) )
					key.add(e);
			}

			if (!res.containsKey(key))
				res.put(key, new Group<V>());

			res.get(key).getViews().add(vd);
		}

		return new ArrayList<>(res.values());
	}

	public static < V extends ViewId > ArrayList< Group< V > > toGroups( final Collection< V > views )
	{
		final ArrayList< Group< V > > groups = new ArrayList<>();

		for ( final V view : views )
			groups.add( new Group< >( view ) );

		return groups;
	}

	public static ArrayList< Group< ViewId > > toViewIdGroups( final Collection< ? extends ViewId > views )
	{
		final ArrayList< Group< ViewId > > groups = new ArrayList<>();

		for ( final ViewId view : views )
			groups.add( new Group< >( view ) );

		return groups;
	}

	public static < V extends ViewId > ArrayList< Group< V > > toGroup( final Collection< V > views )
	{
		final ArrayList< Group< V > > groups = new ArrayList<>();
		final Group< V > group = new Group<>();

		for ( final V view : views )
			group.getViews().add( view );

		groups.add( group );

		return groups;
	}

	public static ArrayList< Group< ViewId > > toViewIdGroup( final Collection< ? extends ViewId > views )
	{
		final ArrayList< Group< ViewId > > groups = new ArrayList<>();
		final Group< ViewId > group = new Group<>();

		for ( final ViewId view : views )
			group.getViews().add( view );

		groups.add( group );

		return groups;
	}

	public static < V extends ViewId > ArrayList< Group< V > > groupByChannel( final List< V > viewIds, final AbstractSequenceDescription< ?, ? extends BasicViewDescription< ? >, ? > sd )
	{
		final ArrayList< V > input = new ArrayList<>();
		final ArrayList< Group< V > > grouped = new ArrayList<>();

		input.addAll( viewIds );

		while ( input.size() > 0 )
		{
			final BasicViewDescription< ? > vd1 = sd.getViewDescriptions().get( input.get( 0 ) );
			final ArrayList< V > localGroup = new ArrayList<>();
			localGroup.add( input.get( 0 ) );
			input.remove( 0 );

			for ( int i = input.size() - 1; i >=0; --i )
			{
				boolean attributesSame = true;

				final BasicViewDescription< ? > vd2 = sd.getViewDescriptions().get( input.get( i ) );

				final int id1 = vd1.getViewSetup().getAttribute( Channel.class ).getId();
				final int id2 = vd2.getViewSetup().getAttribute( Channel.class ).getId();

				// same timepoint, different channel
				if ( vd1.getTimePointId() == vd2.getTimePointId() && id1 != id2 )
				{
					final Map< String, Entity > map1 = vd1.getViewSetup().getAttributes();
					final Map< String, Entity > map2 = vd2.getViewSetup().getAttributes();

					for ( final String key : map1.keySet() )
					{
						if ( key.toLowerCase().equals( "channel" ) )
							continue;

						if ( map1.containsKey( key ) && map2.containsKey( key ) )
						{
							if ( !map1.get( key ).equals( map2.get( key ) ) )
								attributesSame = false;
						}
						else
						{
							attributesSame = false;
						}
					}
				}
				else
				{
					attributesSame = false;
				}

				if ( attributesSame )
				{
					localGroup.add( input.get( i ) );
					input.remove( i );
				}
			}

			// sort by channel, so it is always the same order
			Collections.sort( localGroup, new Comparator< ViewId >()
			{
				@Override
				public int compare( final ViewId o1, final ViewId o2 )
				{
					final int id1 = sd.getViewDescriptions().get( o1 ).getViewSetup().getAttribute( Channel.class ).getId();
					final int id2 = sd.getViewDescriptions().get( o2 ).getViewSetup().getAttribute( Channel.class ).getId();

					return id1 - id2;
				}
			} );

			grouped.add( new Group< >( localGroup ) );
		}

		return grouped;
	}

	public static < V > boolean containsBoth(
			final V viewIdA,
			final V viewIdB,
			final Collection< ? extends Group< V > > groupCollection )
	{
		if ( groupCollection == null )
			return false;

		for ( final Group< V > group : groupCollection )
			if ( group.contains( viewIdA ) && group.contains( viewIdB ) )
				return true;

		return false;
	}

	public static < V > Group< V > isContained(
			final V view,
			final Collection< ? extends Group< V > > groupCollection )
	{
		if ( groupCollection == null )
			return null;

		for ( final Group< V > group : groupCollection )
			if ( group.contains( view ) )
				return group;

		return null;
	}

	/**
	 * Test if there is any overlap between two groups (at least one view part of both)
	 *
	 * @param groupA - first group
	 * @param groupB - second group
	 * @param <V> - view id type
	 * @return whether there is overlap
	 */
	public static < V > boolean overlaps( final Group< V > groupA, final Group< V > groupB )
	{
		for ( final V viewA : groupA )
			if ( groupB.contains( viewA ) )
				return true;

		return false;
	}

	/**
	 * Identifies all groups that a certain view is a member of
	 *
	 * @param view - a view
	 * @param groups - all views
	 * @param <V> - view id type
	 * @return the groups that view is a member of
	 */
	public static < V > ArrayList< Group< V > > memberOf( final V view, final Set< Group< V > > groups )
	{
		final ArrayList< Group< V > > memberOf = new ArrayList<>();

		for ( final Group< V > group : groups )
			if ( group.getViews().contains( view ) )
				memberOf.add( group );

		return memberOf;
	}

	/**
	 * Identify all pairs of overlapping groups
	 *
	 * @param groups the groups
	 * @param <V> - view id type
	 * @return all overlapping group pairs
	 */
	public static < V > ArrayList< Pair< Group< V >, Group< V > > > overlappingGroups( final Collection< Group< V > > groups )
	{
		final ArrayList< Group< V > > g = new ArrayList<>();
		g.addAll( groups );

		final ArrayList< Pair< Group< V >, Group< V > > > overlappingGroups = new ArrayList<>();

		for ( int a = 0; a < g.size() - 1; ++a )
			for ( int b = a + 1; b < g.size(); ++b )
				if ( overlaps( g.get( a ), g.get( b ) ) )
					overlappingGroups.add( new ValuePair<>( g.get( a ), g.get( b ) ) );

		return overlappingGroups;
	}

	/**
	 * Merges all overlapping groups into a new List, the input remains unchanged
	 *
	 * @param groups the groups to merge
	 * @param <V> - view id type
	 * @return merger Groups
	 */
	public static < V > ArrayList< Group< V > > mergeAllOverlappingGroups( final Collection< Group< V > > groups )
	{
		final ArrayList< Group< V > > g = new ArrayList<>();
		g.addAll( groups );

		while ( true )
		{
			final Pair< Integer, Integer > pair = nextOverlappingGroup( g );

			if ( pair == null )
				break;

			final int indexA = pair.getA();
			final int indexB = pair.getB();

			final Group< V > groupA = g.get( indexA );
			final Group< V > groupB = g.get( indexB );

			g.remove( indexB ); // always bigger then indexA
			g.remove( indexA );

			g.add( merge( groupA, groupB ) );
		}

		return g;
	}

	/**
	 * Identify the indices of the next overlapping groups ordered by size, or return null if none are overlapping
	 *
	 * @param groups list of Groups
	 * @param <V> - view id type
	 * @return index of first overlapping pair (Gi, Gj) in grops
	 */
	public static < V > Pair< Integer, Integer > nextOverlappingGroup( final List< Group< V > > groups )
	{
		for ( int a = 0; a < groups.size() - 1; ++a )
			for ( int b = a + 1; b < groups.size(); ++b )
				if ( overlaps( groups.get( a ), groups.get( b ) ) )
					return new ValuePair<>( a, b );

		return null;
	}

	/**
	 * Merges two Groups of views
	 *
	 * @param groupA first Group
	 * @param groupB second Group
	 * @param <V> - view id type
	 * @return merged Group
	 */
	public static < V > Group< V > merge( final Group< V > groupA, final Group< V > groupB  )
	{
		final ArrayList< V > list = new ArrayList<>();

		list.addAll( groupA.getViews() );
		list.addAll( groupB.getViews() );

		return new Group<>( list );
	}

	/**
	 * Merges two Groups of views
	 *
	 * @param groupA first Group
	 * @param groupB second Group
	 * @param <V> - view id type
	 * @return merged Group
	 */
	public static < V > Group< V > mergeOverlapping( final Group< V > groupA, final Group< V > groupB  )
	{
		final ArrayList< V > list = new ArrayList<>();

		list.addAll( groupA.getViews() );
		list.addAll( groupB.getViews() );

		return new Group<>( list );
	}

	public static < V > void removeEmptyGroups( final List< Group < V > > groups )
	{
		for ( int i = groups.size() - 1; i >= 0; --i )
			if ( groups.get( i ).size() == 0 )
				groups.remove( i );
	}

	public static String pvid( final ViewId viewId ) { return "tpId=" + viewId.getTimePointId() + " setupId=" + viewId.getViewSetupId(); }
	public static String pvids( final ViewId viewId ) { return viewId.getTimePointId() + "-" + viewId.getViewSetupId(); }
	@SuppressWarnings("unchecked")
	public static String gvids( final Collection< ? > group )
	{
		if ( group == null )
			return "--null--";
		else if ( group.size() == 0 )
			return "--empty--";

		String groupS = "";

		if ( ViewId.class.isInstance( group.iterator().next() ) )
		{
			final List< ViewId > sorted = getViewsSorted( (Collection<ViewId>)(group) );

			if ( sorted.size() == 1 )
				return pvids( sorted.get( 0 ) );

			final ArrayList< Integer > breaks = new ArrayList<>();

			breaks.add( 0 );

			for ( int i = 1; i < sorted.size(); ++i )
				if ( sorted.get( i - 1 ).getTimePointId() != sorted.get( i ).getTimePointId() || sorted.get( i ).getViewSetupId() - sorted.get( i - 1 ).getViewSetupId() != 1 )
					breaks.add( i );

			breaks.add( sorted.size() );

			for ( int i = 0; i < breaks.size() - 1; ++i )
			{
				final int from = breaks.get( i );
				final int to = breaks.get( i + 1 ) - 1;

				if ( from == to )
					groupS += pvids( sorted.get( from ) ) + ", ";
				else
					groupS += pvids( sorted.get( from ) ) + " >-> " + pvids( sorted.get( to ) ) + ", ";
			}
		}
		else
			for ( final Object a : group )
				groupS += a + " ";

		return groupS.substring( 0, groupS.length() - 2 ).trim();
	}

	public static String gvids( final Group< ? > group )
	{
		return gvids( group.getViews() );
	}

	public static void main( final String[] args )
	{
		final ViewId v0 = new ViewId( 0, 0 );
		final ViewId v1 = new ViewId( 0, 1 );
		final ViewId v2 = new ViewId( 0, 2 );
		final ViewId v3 = new ViewId( 1, 0 );
		final ViewId v4 = new ViewId( 1, 1 );
		final ViewId v5 = new ViewId( 1, 2 );

		final Group< ViewId > g0 = new Group<>();
		final Group< ViewId > g1 = new Group<>();
		final Group< ViewId > g2 = new Group<>();

		g0.getViews().add( v0 );
		g0.getViews().add( v2 );
		g0.getViews().add( v5 );

		g1.getViews().add( v1 );
		g1.getViews().add( v3 );
		g1.getViews().add( v4 );

		g2.getViews().add( v2 );
		g2.getViews().add( v4 );

		final ArrayList< Group< ViewId > > groups = new ArrayList<>();
		groups.add( g0 );
		groups.add( g1 );
		groups.add( g2 );

		for ( final Group< ViewId > g : mergeAllOverlappingGroups( groups ) )
		{
			System.out.println( g );
		}

		final Group< ViewId > g3 = new Group<>();
		System.out.println( g3 );
		g3.getViews().add( new ViewId( 0, 0 ) );
		System.out.println( g3 );

		for ( int i = 2; i < 20; ++i )
			g3.getViews().add( new ViewId( 0, i ) );
		System.out.println( g3 );

		g3.getViews().add( new ViewId( 0, 25 ) );
		System.out.println( g3 );

		for ( int i = 26; i < 40; ++i )
			g3.getViews().add( new ViewId( 1, i ) );
		System.out.println( g3 );

	}
}
