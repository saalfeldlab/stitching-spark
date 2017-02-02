package org.janelia.stitching;

import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import bdv.export.Downsample;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.util.IntervalIndexer;

public class DownsampleTest 
{
	@Test
	public void testDownsample()
	{
		final Random rnd = new Random();
		
		final long[] dim = new long[] { 2, 3, 4 };
		final ArrayImg< UnsignedByteType, ByteArray > img = ArrayImgs.unsignedBytes( dim );
		final byte[] arr = img.update( null ).getCurrentStorageArray();
		
		rnd.nextBytes( arr );
		
		final long[] dim_expected = new long[] { 1, 1, 2 };
		final ArrayImg< UnsignedByteType, ByteArray > img_expected = ArrayImgs.unsignedBytes( dim_expected );
		final byte[] arr_expected = img_expected.update( null ).getCurrentStorageArray();
		
		arr_expected[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 0 }, dim_expected ) ] = ( byte )
				Math.round( ( double ) (
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 0 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 0, 0 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 1, 0 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 1, 0 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 1 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 0, 1 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 1, 1 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 1, 1 }, dim ) ] )
				) / 8 );
		
		arr_expected[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 1 }, dim_expected ) ] = ( byte )
				Math.round( ( double ) (
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 2 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 0, 2 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 1, 2 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 1, 2 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 0, 3 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 0, 3 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 0, 1, 3 }, dim ) ] ) +
					Byte.toUnsignedInt( arr[ ( int ) IntervalIndexer.positionToIndex( new long[] { 1, 1, 3 }, dim ) ] )
				) / 8 );
		
		final ArrayImg< UnsignedByteType, ByteArray > img_actual = ArrayImgs.unsignedBytes( dim_expected );
		Downsample.downsample( img, img_actual, new int[] { 2, 2, 2 } );
		final byte[] arr_actual = img_actual.update( null ).getCurrentStorageArray();

		try
		{
			Assert.assertArrayEquals( arr_expected, arr_actual );
		}
		catch ( final AssertionError e )
		{
			System.out.println( "Input: " + Arrays.toString( arr ) );
			System.out.println( "Expected: " + Arrays.toString( arr_expected ) );
			System.out.println( "Actual: " + Arrays.toString( arr_actual ) );
			throw e;
		}
	}
}
