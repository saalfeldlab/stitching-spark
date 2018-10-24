package net.imglib2.neighborsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.FinalRealInterval;
import net.imglib2.KDTree;
import net.imglib2.RealInterval;
import net.imglib2.RealLocalizable;
import net.imglib2.RealPoint;
import net.imglib2.util.Intervals;

public class IntervalNeighborSearchOnKDTreeTest
{
	@Test
	public void test2D()
	{
		final int points = 8;

		final List< Integer > indexes = new ArrayList<>();
		for ( int i = 0; i < points; ++i )
			indexes.add( i );

		final List< RealLocalizable > positions = new ArrayList<>();
		positions.add( new RealPoint( 5, 8 ) );
		positions.add( new RealPoint( -4, 1 ) );
		positions.add( new RealPoint( 9, -2 ) );
		positions.add( new RealPoint( 3, 0 ) );
		positions.add( new RealPoint( 8, 7 ) );
		positions.add( new RealPoint( 2, 4 ) );
		positions.add( new RealPoint( -9, -5 ) );
		positions.add( new RealPoint( 6, 3 ) );

		final IntervalNeighborSearch< Integer > treeSearch = new IntervalNeighborSearchOnKDTree<>( new KDTree<>( indexes, new ArrayList<>( positions ) ) );
		final IntervalNeighborSearch< Integer > exhaustiveSearch = new IntervalNeighborExhaustiveSearch<>( indexes, positions );

		final RealInterval interval = new FinalRealInterval( new double[] { 0, 0 }, new double[] { 8, 8 } );

		final List< Integer > exhaustiveSearchResult = exhaustiveSearch.search( interval );
		final List< Integer > treeSearchResult = treeSearch.search( interval );

		Collections.sort( exhaustiveSearchResult );
		Collections.sort( treeSearchResult );

		Assert.assertArrayEquals( exhaustiveSearchResult.toArray(), treeSearchResult.toArray() );
	}

	@Test
	public void randomTest()
	{
		final Random rnd = new Random();
		for ( int t = 0; t < 500; ++t )
		{
			final int dim = rnd.nextInt( 3 ) + 2;
			final int points = rnd.nextInt( 10000 ) + 2;
			final List< Integer > indexes = new ArrayList<>();
			final List< RealLocalizable > positions = new ArrayList<>();
			for ( int i = 0; i < points; ++i )
			{
				indexes.add( i );
				final double[] pos = new double[ dim ];
				for ( int d = 0; d < dim; ++d )
					pos[ d ] = ( rnd.nextDouble() - 0.5 ) * 100;
				positions.add( new RealPoint( pos ) );
			}

			final IntervalNeighborSearch< Integer > treeSearch = new IntervalNeighborSearchOnKDTree<>( new KDTree<>( indexes, new ArrayList<>( positions ) ) );
			final IntervalNeighborSearch< Integer > exhaustiveSearch = new IntervalNeighborExhaustiveSearch<>( indexes, positions );

			final double[] min = new double[ dim ], max = new double[ dim ];
			for ( int d = 0; d < dim; ++d )
			{
				min[ d ] = ( rnd.nextDouble() - 0.5 ) * 120;
				max[ d ] = min[ d ] + rnd.nextDouble() * 100;
			}
			final RealInterval interval = new FinalRealInterval( min, max );

			final List< Integer > exhaustiveSearchResult = exhaustiveSearch.search( interval );
			final List< Integer > treeSearchResult = treeSearch.search( interval );

			Collections.sort( exhaustiveSearchResult );
			Collections.sort( treeSearchResult );

			try
			{
				Assert.assertArrayEquals( exhaustiveSearchResult.toArray(), treeSearchResult.toArray() );
			}
			catch ( final AssertionError e )
			{
				System.out.println( "Exhaustive search result: " + exhaustiveSearchResult );
				System.out.println( "Tree search result: " + treeSearchResult );
				System.out.println();
				System.out.println( "Points: ");
				for ( final RealLocalizable pos : positions )
				{
					final double[] posArr = new double[ dim ];
					pos.localize( posArr );
					System.out.println( "  " + Arrays.toString( posArr ) );
				}
				System.out.println();
				System.out.println( "ROI: min=" + Arrays.toString( Intervals.minAsDoubleArray( interval ) ) + ", max=" + Arrays.toString( Intervals.maxAsDoubleArray( interval ) ) );

				throw e;
			}
		}
	}


	private static class IntervalNeighborExhaustiveSearch< T > implements IntervalNeighborSearch< T >
	{
		private final List< T > values;
		private final List< RealLocalizable > positions;

		public IntervalNeighborExhaustiveSearch( final List< T > values, final List< RealLocalizable > positions )
		{
			this.values = values;
			this.positions = positions;
		}

		@Override
		public List< T > search( final RealInterval interval )
		{
			final int dim = numDimensions();
			final List< T > found = new ArrayList<>();
			for ( int i = 0; i < Math.max( values.size(), positions.size() ); ++i )
			{
				boolean within = true;
				for ( int d = 0; d < dim; ++d )
					if ( positions.get( i ).getDoublePosition( d ) < interval.realMin( d ) || positions.get( i ).getDoublePosition( d ) > interval.realMax( d ) )
						within = false;
				if ( within )
					found.add( values.get( i ) );
			}
			return found;
		}

		@Override
		public int numDimensions()
		{
			return positions.get( 0 ).numDimensions();
		}
	}
}
