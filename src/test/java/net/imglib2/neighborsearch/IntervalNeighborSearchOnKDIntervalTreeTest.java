package net.imglib2.neighborsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.KDIntervalTree;
import net.imglib2.RealInterval;
import net.imglib2.util.Intervals;

public class IntervalNeighborSearchOnKDIntervalTreeTest
{
	@Test
	public void testOverlapping2D()
	{
		final List< Integer > values = new ArrayList<>();
		values.add( 4 );
		values.add( 2 );
		values.add( 7 );
		values.add( 4 );
		values.add( 6 );
		values.add( 5 );

		final List< RealInterval > intervals = new ArrayList<>();
		intervals.add( Intervals.createMinMaxReal( 0, 0, 10, 10 ) );
		intervals.add( Intervals.createMinMaxReal( 5.5, -2.5, 7.5, 11.5 ) );
		intervals.add( Intervals.createMinMaxReal( 12.5, 3, 12.5, 10 ) );
		intervals.add( Intervals.createMinMaxReal( 15, 15, 25, 25 ) );
		intervals.add( Intervals.createMinMaxReal( 2, 3, 6, 6 ) );
		intervals.add( Intervals.createMinMaxReal( -5, 9, 15, 15 ) );

		final KDIntervalTree< Integer > intervalTree = new KDIntervalTree<>( values, intervals );
		Assert.assertEquals( 6, intervalTree.size() );
		Assert.assertEquals( 24, intervalTree.getIntervalsCornerPointsKDTree().size() );

		final IntervalNeighborSearch< Integer > overlappingNeighborIntervalSearch = new OverlappingIntervalNeighborSearchOnKDIntervalTree<>( intervalTree );

		Assert.assertArrayEquals( new int[] { 4, 6 }, valuesListToSortedArray( overlappingNeighborIntervalSearch.search( Intervals.createMinMaxReal( 5, 5, 7, 7 ) ) ) );
		Assert.assertArrayEquals( new int[] { 4, 4, 5, 7 }, valuesListToSortedArray( overlappingNeighborIntervalSearch.search( Intervals.createMinMaxReal( 9, 5, 16, 20 ) ) ) );
	}

	@Test
	public void testFullyContained2D()
	{
		final List< Integer > values = new ArrayList<>();
		values.add( 4 );
		values.add( 2 );
		values.add( 7 );
		values.add( 4 );
		values.add( 6 );
		values.add( 5 );

		final List< RealInterval > intervals = new ArrayList<>();
		intervals.add( Intervals.createMinMaxReal( 0, 0, 10, 10 ) );
		intervals.add( Intervals.createMinMaxReal( 5.5, -2.5, 7.5, 11.5 ) );
		intervals.add( Intervals.createMinMaxReal( 12.5, 3, 12.5, 10 ) );
		intervals.add( Intervals.createMinMaxReal( 15, 15, 25, 25 ) );
		intervals.add( Intervals.createMinMaxReal( 2, 3, 6, 6 ) );
		intervals.add( Intervals.createMinMaxReal( -5, 9, 15, 15 ) );

		final KDIntervalTree< Integer > intervalTree = new KDIntervalTree<>( values, intervals );
		Assert.assertEquals( 6, intervalTree.size() );
		Assert.assertEquals( 24, intervalTree.getIntervalsCornerPointsKDTree().size() );

		final IntervalNeighborSearch< Integer > fullyContainedNeighborIntervalSearch = new FullyContainedIntervalNeighborSearchOnKDIntervalTree<>( intervalTree );

		Assert.assertArrayEquals( new int[] { 2, 4, 6, 7 }, valuesListToSortedArray( fullyContainedNeighborIntervalSearch.search( Intervals.createMinMaxReal( -4.5, -3, 15, 25 ) ) ) );
	}

	private static int[] valuesListToSortedArray( final List< Integer > values )
	{
		final int[] arr = values.stream().mapToInt( val -> val.intValue() ).toArray();
		Arrays.sort( arr );
		return arr;
	}
}
