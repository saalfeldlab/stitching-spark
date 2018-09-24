package org.janelia.flatfield;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.RandomAccessiblePair;
import net.imglib2.view.Views;

public class FlatfieldCorrectedRandomAccessibleTest
{
	@Test
	public void test()
	{
		final long[] dimensions = new long[] { 2, 2 };

		final RandomAccessibleInterval< ByteType > img = ArrayImgs.bytes( new byte[] { 10, 6, 15, 24 }, dimensions );

		final RandomAccessibleInterval< FloatType > flatfieldA = ArrayImgs.floats( new float[] { 1.0f, 2.0f, 1.5f, 3.0f }, dimensions );
		final RandomAccessibleInterval< FloatType > flatfieldB = ArrayImgs.floats( new float[] { 5.0f, 6.0f, 4.0f, 2.5f }, dimensions );
		final RandomAccessiblePair< FloatType, FloatType > flatfield = new RandomAccessiblePair<>( flatfieldA, flatfieldB );

		final FlatfieldCorrectedRandomAccessible< ByteType, FloatType > flatfieldCorrectedAccessible = new FlatfieldCorrectedRandomAccessible<>( img, flatfield );

		final RandomAccessibleInterval< FloatType > correctedImg = Views.interval( flatfieldCorrectedAccessible, img );

		final float[] correctedValues = new float[ ( int ) Intervals.numElements( correctedImg ) ];
		int counter = 0;
		for ( final Iterator< FloatType > it = Views.flatIterable( correctedImg ).iterator(); it.hasNext(); )
			correctedValues[ counter++ ] = it.next().get();

		Assert.assertArrayEquals( new float[] { 15.0f, 18.0f, 26.5f, 74.5f }, correctedValues, 1e-6f );
	}

	@Test
	public void testInterval()
	{
		final long[] dimensions = new long[] { 2, 2 };

		final RandomAccessibleInterval< ByteType > img = ArrayImgs.bytes( new byte[] { 10, 6, 15, 24 }, dimensions );
		final RandomAccessibleInterval< ByteType > imgCrop = Views.interval( img, Intervals.createMinMax( 1, 0, 1, 1 ) );

		final RandomAccessibleInterval< FloatType > flatfieldA = ArrayImgs.floats( new float[] { 1.0f, 2.0f, 1.5f, 3.0f }, dimensions );
		final RandomAccessibleInterval< FloatType > flatfieldB = ArrayImgs.floats( new float[] { 5.0f, 6.0f, 4.0f, 2.5f }, dimensions );
		final RandomAccessiblePair< FloatType, FloatType > flatfield = new RandomAccessiblePair<>( flatfieldA, flatfieldB );

		final FlatfieldCorrectedRandomAccessible< ByteType, FloatType > flatfieldCorrectedAccessible = new FlatfieldCorrectedRandomAccessible<>( imgCrop, flatfield );

		final RandomAccessibleInterval< FloatType > correctedImgCrop = Views.interval( flatfieldCorrectedAccessible, imgCrop );

		final float[] correctedValues = new float[ ( int ) Intervals.numElements( correctedImgCrop ) ];
		int counter = 0;
		for ( final Iterator< FloatType > it = Views.flatIterable( correctedImgCrop ).iterator(); it.hasNext(); )
			correctedValues[ counter++ ] = it.next().get();

		Assert.assertArrayEquals( new float[] { 18.0f, 74.5f }, correctedValues, 1e-6f );
	}
}
