package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.RealPoint;

public class PointSetCollinearityAndCoplanarityCheckTest
{
	@Test
	public void testCollinearity2D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, 1 ) );
		points.add( new RealPoint( 22, 7 ) );
		points.add( new RealPoint( 14, 3 ) );
		points.add( new RealPoint( -5, -6.5 ) );
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertTrue( PointSetCollinearityAndCoplanarityCheck.testCollinearity( points ) );
		}
	}

	@Test
	public void testCollinearityHorizontal2D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 8, 3 ) );
		points.add( new RealPoint( 5, 3 ) );
		points.add( new RealPoint( -100, 3 ) );
		points.add( new RealPoint( 1000, 3 ) );
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertTrue( PointSetCollinearityAndCoplanarityCheck.testCollinearity( points ) );
		}
	}

	@Test
	public void testCollinearityVertical2D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 3, 8 ) );
		points.add( new RealPoint( 3, 5 ) );
		points.add( new RealPoint( 3, -100 ) );
		points.add( new RealPoint( 3, 1000 ) );
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertTrue( PointSetCollinearityAndCoplanarityCheck.testCollinearity( points ) );
		}
	}

	@Test
	public void testNotCollinear2D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 2, 5 ) );
		points.add( new RealPoint( 1, 4 ) );
		points.add( new RealPoint( 7, 2 ) );
		points.add( new RealPoint( 3, 6 ) );
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertFalse( PointSetCollinearityAndCoplanarityCheck.testCollinearity( points ) );
		}
	}

	@Test
	public void testCollinearity3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, 1, 2 ) );
		points.add( new RealPoint( 22, 7, -1 ) );
		points.add( new RealPoint( 14, 3, 1 ) );
		points.add( new RealPoint( -5, -6.5, 5.75 ) );
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertTrue( PointSetCollinearityAndCoplanarityCheck.testCollinearity( points ) );
		}
	}

	@Test
	public void testCollinearityParallelToX3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( -2.2, 10, 1 ) );
		points.add( new RealPoint( -2.2, 22, 7 ) );
		points.add( new RealPoint( -2.2, 14, 3 ) );
		points.add( new RealPoint( -2.2, -5, -6.5 ) );
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertTrue( PointSetCollinearityAndCoplanarityCheck.testCollinearity( points ) );
		}
	}

	@Test
	public void testCollinearityParallelToY3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, -2.2, 1 ) );
		points.add( new RealPoint( 22, -2.2, 7 ) );
		points.add( new RealPoint( 14, -2.2, 3 ) );
		points.add( new RealPoint( -5, -2.2, -6.5 ) );
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertTrue( PointSetCollinearityAndCoplanarityCheck.testCollinearity( points ) );
		}
	}

	@Test
	public void testCollinearityParallelToZ3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 10, 1, -2.2 ) );
		points.add( new RealPoint( 22, 7, -2.2 ) );
		points.add( new RealPoint( 14, 3, -2.2 ) );
		points.add( new RealPoint( -5, -6.5, -2.2 ) );
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertTrue( PointSetCollinearityAndCoplanarityCheck.testCollinearity( points ) );
		}
	}

	@Test
	public void testNotCollinear3D()
	{
		final List< RealPoint > points = new ArrayList<>();
		points.add( new RealPoint( 2, 5, 0 ) );
		points.add( new RealPoint( 1, 4, 7 ) );
		points.add( new RealPoint( 7, 2, 8 ) );
		points.add( new RealPoint( 3, 6, -4 ) );
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( points );
			Assert.assertFalse( PointSetCollinearityAndCoplanarityCheck.testCollinearity( points ) );
		}
	}
}
