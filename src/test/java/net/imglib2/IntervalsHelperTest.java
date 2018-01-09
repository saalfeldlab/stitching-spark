package net.imglib2;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;

public class IntervalsHelperTest
{
	@Test
	public void translateTest1()
	{
		final Interval interval = new FinalInterval( new long[] { 5 }, new long[] { 7 } );
		final Interval intervalTranslated = IntervalsHelper.translate( interval, new long[] { 7 } );
		Assert.assertArrayEquals( new long[] { 12 }, Intervals.minAsLongArray( intervalTranslated ) );
		Assert.assertArrayEquals( new long[] { 14 }, Intervals.maxAsLongArray( intervalTranslated ) );
	}

	@Test
	public void translateTest2()
	{
		final Interval interval = new FinalInterval( new long[] { 5, -2 }, new long[] { 7, 1 } );
		final Interval intervalTranslated = IntervalsHelper.translate( interval, new long[] { -2, 8 } );
		Assert.assertArrayEquals( new long[] { 3, 6 }, Intervals.minAsLongArray( intervalTranslated ) );
		Assert.assertArrayEquals( new long[] { 5, 9 }, Intervals.maxAsLongArray( intervalTranslated ) );
	}

	@Test
	public void translateTest3()
	{
		final Interval interval = new FinalInterval( new long[] { 0, 0, 0 }, new long[] { 2, 3, 4 } );
		final Interval intervalTranslated = IntervalsHelper.translate( interval, new long[] { 7, 6, 5 } );
		Assert.assertArrayEquals( new long[] { 7, 6, 5 }, Intervals.minAsLongArray( intervalTranslated ) );
		Assert.assertArrayEquals( new long[] { 9, 9, 9 }, Intervals.maxAsLongArray( intervalTranslated ) );
	}
}
