package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.util.Intervals;

public class IntervalPaddingTest
{
	@Test
	public void testBothSidesEven()
	{
		final Interval interval = TileOperations.padInterval(
				new FinalInterval(
						new long[] { 50 },
						new long[] { 60 }
					),
				new FinalDimensions( new long[] { 100 } ),
				new long[] { 10 }
			);
		Assert.assertArrayEquals( new long[] { 45 }, Intervals.minAsLongArray( interval ) );
		Assert.assertArrayEquals( new long[] { 65 }, Intervals.maxAsLongArray( interval ) );
	}

	@Test
	public void testBothSidesOdd()
	{
		final Interval interval = TileOperations.padInterval(
				new FinalInterval(
						new long[] { 50 },
						new long[] { 60 }
					),
				new FinalDimensions( new long[] { 100 } ),
				new long[] { 11 }
			);
		Assert.assertArrayEquals( new long[] { 44 }, Intervals.minAsLongArray( interval ) );
		Assert.assertArrayEquals( new long[] { 65 }, Intervals.maxAsLongArray( interval ) );
	}

	@Test
	public void testMaxSide()
	{
		final Interval interval = TileOperations.padInterval(
				new FinalInterval(
						new long[] {  0 },
						new long[] { 44 }
					),
				new FinalDimensions( new long[] { 500 } ),
				new long[] { 200 }
			);
		Assert.assertArrayEquals( new long[] {   0 }, Intervals.minAsLongArray( interval ) );
		Assert.assertArrayEquals( new long[] { 244 }, Intervals.maxAsLongArray( interval ) );
	}

	@Test
	public void testMinSide()
	{
		final Interval interval = TileOperations.padInterval(
				new FinalInterval(
						new long[] { 6 },
						new long[] { 8 }
					),
				new FinalDimensions( new long[] { 9 } ),
				new long[] { 1 }
			);
		Assert.assertArrayEquals( new long[] { 5 }, Intervals.minAsLongArray( interval ) );
		Assert.assertArrayEquals( new long[] { 8 }, Intervals.maxAsLongArray( interval ) );
	}

	@Test
	public void testLargerThanOuterSpace()
	{
		final Interval interval = TileOperations.padInterval(
				new FinalInterval(
						new long[] { 2, 70 },
						new long[] { 5, 80 }
					),
				new FinalDimensions( new long[] { 9, 150 } ),
				new long[] { 20, 1000 }
			);
		Assert.assertArrayEquals( new long[] { 0,   0 }, Intervals.minAsLongArray( interval ) );
		Assert.assertArrayEquals( new long[] { 8, 149 }, Intervals.maxAsLongArray( interval ) );
	}
}

