package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.RealPoint;

public class PointSetCollinearityCheckTest
{
	private void testWithShuffle( final List< RealPoint > points, final boolean expectedResult )
	{
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertEquals( expectedResult, PointSetCollinearityCheck.testCollinearity( points ) );
		}
	}

	@Test
	public void testCollinearity2D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, 1 ) );
		points.add( new RealPoint( 22, 7 ) );
		points.add( new RealPoint( 14, 3 ) );
		points.add( new RealPoint( -5, -6.5 ) );
		testWithShuffle( points, true );
	}

	@Test
	public void testCollinearityHorizontal2D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 8, 3 ) );
		points.add( new RealPoint( 5, 3 ) );
		points.add( new RealPoint( -100, 3 ) );
		points.add( new RealPoint( 1000, 3 ) );
		testWithShuffle( points, true );
	}

	@Test
	public void testCollinearityVertical2D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 3, 8 ) );
		points.add( new RealPoint( 3, 5 ) );
		points.add( new RealPoint( 3, -100 ) );
		points.add( new RealPoint( 3, 1000 ) );
		testWithShuffle( points, true );
	}

	@Test
	public void testCollinearityIdenticalPoints2D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, 1 ) );
		points.add( new RealPoint( 10, 1 ) );
		points.add( new RealPoint( 10, 1 ) );
		points.add( new RealPoint( 10, 1 ) );
		testWithShuffle( points, true );
	}

	@Test
	public void testNotCollinear2D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 2, 5 ) );
		points.add( new RealPoint( 1, 4 ) );
		points.add( new RealPoint( 7, 2 ) );
		points.add( new RealPoint( 3, 6 ) );
		testWithShuffle( points, false );
	}

	@Test
	public void testCollinearity3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, 1, 2 ) );
		points.add( new RealPoint( 22, 7, -1 ) );
		points.add( new RealPoint( 14, 3, 1 ) );
		points.add( new RealPoint( -5, -6.5, 5.75 ) );
		testWithShuffle( points, true );
	}

	@Test
	public void testCollinearityParallelToX3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( -2.2, 10, 1 ) );
		points.add( new RealPoint( -2.2, 22, 7 ) );
		points.add( new RealPoint( -2.2, 14, 3 ) );
		points.add( new RealPoint( -2.2, -5, -6.5 ) );
		testWithShuffle( points, true );
	}

	@Test
	public void testCollinearityParallelToY3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, -2.2, 1 ) );
		points.add( new RealPoint( 22, -2.2, 7 ) );
		points.add( new RealPoint( 14, -2.2, 3 ) );
		points.add( new RealPoint( -5, -2.2, -6.5 ) );
		testWithShuffle( points, true );
	}

	@Test
	public void testCollinearityParallelToZ3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, 1, -2.2 ) );
		points.add( new RealPoint( 22, 7, -2.2 ) );
		points.add( new RealPoint( 14, 3, -2.2 ) );
		points.add( new RealPoint( -5, -6.5, -2.2 ) );
		testWithShuffle( points, true );
	}

	@Test
	public void testCollinearityIdenticalPoints3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, 1, 2 ) );
		points.add( new RealPoint( 10, 1, 2 ) );
		points.add( new RealPoint( 10, 1, 2 ) );
		points.add( new RealPoint( 10, 1, 2 ) );
		testWithShuffle( points, true );
	}

	@Test
	public void testNotCollinear3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 2, 5, 0 ) );
		points.add( new RealPoint( 1, 4, 7 ) );
		points.add( new RealPoint( 7, 2, 8 ) );
		points.add( new RealPoint( 3, 6, -4 ) );
		testWithShuffle( points, false );
	}
}
