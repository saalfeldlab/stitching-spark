package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.janelia.stitching.PointMatchSpatialLayout.SpatialLayout;
import org.junit.Assert;
import org.junit.Test;

import mpicbg.models.Point;
import mpicbg.models.PointMatch;

public class PointMatchSpatialLayoutTest
{
	private void testWithShuffle( final List< PointMatch > pointMatches, final SpatialLayout expectedLayout )
	{
		for ( int i = 0; i < 100; ++i )
		{
			Collections.shuffle( pointMatches );
			Assert.assertEquals( expectedLayout, PointMatchSpatialLayout.determine( pointMatches ) );
		}
	}

	@Test
	public void testCollinearity2D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1 } ), new Point( new double[] { 10, 1 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 22, 7 } ), new Point( new double[] { 22, 7 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 14, 3 } ), new Point( new double[] { 14, 3 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { -5, -6.5 } ), new Point( new double[] { -5, -6.5 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Collinear );
	}

	@Test
	public void testCollinearityHorizontal2D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 8, 3 } ), new Point( new double[] { 8, 3 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 5, 3 } ), new Point( new double[] { 5, 3 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { -100, 3 } ), new Point( new double[] { -100, 3 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 1000, 3 } ), new Point( new double[] { 1000, 3 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Collinear );
	}

	@Test
	public void testCollinearityVertical2D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 3, 8 } ), new Point( new double[] { 3, 8 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 3, 5 } ), new Point( new double[] { 3, 5 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 3, -100 } ), new Point( new double[] { 3, -100 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 3, 1000 } ), new Point( new double[] { 3, 1000 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Collinear );
	}

	@Test
	public void testCollinearityIdenticalPoints2D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1 } ), new Point( new double[] { 10, 1 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1 } ), new Point( new double[] { 10, 1 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1 } ), new Point( new double[] { 10, 1 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1 } ), new Point( new double[] { 10, 1 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Collinear );
	}

	@Test
	public void testNotCollinear2D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 2, 5 } ), new Point( new double[] { 2, 5 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 1, 4 } ), new Point( new double[] { 1, 4 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 7, 2 } ), new Point( new double[] { 7, 2 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 3, 6 } ), new Point( new double[] { 3, 6 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Coplanar );
	}

	@Test
	public void testCollinearity3D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1, 2 } ), new Point( new double[] { 10, 1, 2 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 22, 7, -1 } ), new Point( new double[] { 22, 7, -1 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 14, 3, 1 } ), new Point( new double[] { 14, 3, 1 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { -5, -6.5, 5.75 } ), new Point( new double[] { -5, -6.5, 5.75 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Collinear );
	}

	@Test
	public void testCollinearityParallelToX3D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { -2.2, 10, 1 } ), new Point( new double[] { -2.2, 10, 1 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { -2.2, 22, 7 } ), new Point( new double[] { -2.2, 22, 7 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { -2.2, 14, 3 } ), new Point( new double[] { -2.2, 14, 3 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { -2.2, -5, -6.5 } ), new Point( new double[] { -2.2, -5, -6.5 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Collinear );
	}

	@Test
	public void testCollinearityParallelToY3D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 10, -2.2, 1 } ), new Point( new double[] { 10, -2.2, 1 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 22, -2.2, 7 } ), new Point( new double[] { 22, -2.2, 7 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 14, -2.2, 3 } ), new Point( new double[] { 14, -2.2, 3 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { -5, -2.2, -6.5 } ), new Point( new double[] { -5, -2.2, -6.5 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Collinear );
	}

	@Test
	public void testCollinearityParallelToZ3D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1, -2.2 } ), new Point( new double[] { 10, 1, -2.2 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 22, 7, -2.2 } ), new Point( new double[] { 22, 7, -2.2 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 14, 3, -2.2 } ), new Point( new double[] { 14, 3, -2.2 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { -5, -6.5, -2.2 } ), new Point( new double[] { -5, -6.5, -2.2 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Collinear );
	}

	@Test
	public void testCollinearityIdenticalPoints3D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1, 2 } ), new Point( new double[] { 10, 1, 2 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1, 2 } ), new Point( new double[] { 10, 1, 2 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1, 2 } ), new Point( new double[] { 10, 1, 2 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 10, 1, 2 } ), new Point( new double[] { 10, 1, 2 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Collinear );
	}

	@Test
	public void testCoplanar3D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 2, 5, 0 } ), new Point( new double[] { 2, 5, 0 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 1, 4, 7 } ), new Point( new double[] { 1, 4, 7 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 7, 2, 8 } ), new Point( new double[] { 7, 2, 8 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 3, 6, -4 } ), new Point( new double[] { 3, 6, -4 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Coplanar );
	}

	@Test
	public void testOther3D()
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		pointMatches.add( new PointMatch( new Point( new double[] { 2, 5, 0 } ), new Point( new double[] { 2, 5, 0 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 1, 4, 7 } ), new Point( new double[] { 1, 4, 7 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 7, 2, 8 } ), new Point( new double[] { 7, 2, 8 } ) ) );
		pointMatches.add( new PointMatch( new Point( new double[] { 3, 7, -4 } ), new Point( new double[] { 3, 7, -4 } ) ) );
		testWithShuffle( pointMatches, SpatialLayout.Other );
	}
}
