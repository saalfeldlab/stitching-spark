package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.FinalDimensions;

public class IntervalPaddingTest
{
	@Test
	public void testBothSidesEven()
	{
		final Boundaries interval = TileOperations.padInterval(
				new Boundaries(
						new long[] { 50 },
						new long[] { 60 }
					),
				new FinalDimensions( new long[] { 100 } ),
				new long[] { 10 }
			);
		Assert.assertArrayEquals( new long[] { 45 }, interval.getMin() );
		Assert.assertArrayEquals( new long[] { 65 }, interval.getMax() );
	}

	@Test
	public void testBothSidesOdd()
	{
		final Boundaries interval = TileOperations.padInterval(
				new Boundaries(
						new long[] { 50 },
						new long[] { 60 }
					),
				new FinalDimensions( new long[] { 100 } ),
				new long[] { 11 }
			);
		Assert.assertArrayEquals( new long[] { 44 }, interval.getMin() );
		Assert.assertArrayEquals( new long[] { 65 }, interval.getMax() );
	}

	@Test
	public void testMaxSide()
	{
		final Boundaries interval = TileOperations.padInterval(
				new Boundaries(
						new long[] {  0 },
						new long[] { 44 }
					),
				new FinalDimensions( new long[] { 500 } ),
				new long[] { 200 }
			);
		Assert.assertArrayEquals( new long[] {   0 }, interval.getMin() );
		Assert.assertArrayEquals( new long[] { 244 }, interval.getMax() );
	}

	@Test
	public void testMinSide()
	{
		final Boundaries interval = TileOperations.padInterval(
				new Boundaries(
						new long[] { 6 },
						new long[] { 8 }
					),
				new FinalDimensions( new long[] { 9 } ),
				new long[] { 1 }
			);
		Assert.assertArrayEquals( new long[] { 5 }, interval.getMin() );
		Assert.assertArrayEquals( new long[] { 8 }, interval.getMax() );
	}

	@Test
	public void testLargerThanOuterSpace()
	{
		final Boundaries interval = TileOperations.padInterval(
				new Boundaries(
						new long[] { 2, 70 },
						new long[] { 5, 80 }
					),
				new FinalDimensions( new long[] { 9, 150 } ),
				new long[] { 20, 1000 }
			);
		Assert.assertArrayEquals( new long[] { 0,   0 }, interval.getMin() );
		Assert.assertArrayEquals( new long[] { 8, 149 }, interval.getMax() );
	}
}

