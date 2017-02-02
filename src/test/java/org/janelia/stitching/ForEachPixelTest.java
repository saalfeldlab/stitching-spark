package org.janelia.stitching;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;

public class ForEachPixelTest
{

	@Test
	public void test()
	{
		final Dimensions dimensions = new FinalDimensions( new long[] { 10, 10, 10 } );

		final long[] mins = new long[] { 8, 8, 8 };
		final long[] maxs = new long[] { 9, 9, 9 };

		final Interval interval = new FinalInterval( mins, maxs );

		final List< Long > visited = new ArrayList<>();
		TileOperations.forEachPixel( interval, dimensions, i -> visited.add( i ) );

		Assert.assertArrayEquals( new Long[]
				{
						(long)888, (long)889, (long)898, (long)899, (long)988, (long)989, (long)998, (long)999
				},
				visited.toArray( new Long[ 0 ] ) );
	}
}
