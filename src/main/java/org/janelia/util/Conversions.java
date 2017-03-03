package org.janelia.util;

import java.util.ArrayList;
import java.util.List;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

/**
 * Utility class to convert data types for use across libraries.
 *
 * @author Igor Pisarev
 */

public class Conversions {

	// for compatibility across libraries

	public static float[] toFloatArray( final double[] arr )
	{
		final float[] ret = new float[ arr.length ];
		for ( int i = 0; i < arr.length; i++ )
			ret[ i ] = (float)arr[ i ];
		return ret;
	}
	public static double[] toDoubleArray( final float[] arr )
	{
		final double[] ret = new double[ arr.length ];
		for ( int i = 0; i < arr.length; i++ )
			ret[ i ] = arr[ i ];
		return ret;
	}

	public static long[] toLongArray( final int[] arr )
	{
		final long[] ret = new long[ arr.length ];
		for ( int i = 0; i < arr.length; i++ )
			ret[ i ] = arr[ i ];
		return ret;
	}

	public static int[] toIntArray( final long[] arr )
	{
		final int[] ret = new int[ arr.length ];
		for ( int i = 0; i < arr.length; i++ )
			ret[ i ] = (int)arr[ i ];
		return ret;
	}

	public static int[] parseIntArray( final String[] arrStr )
	{
		final int[] arr = new int[ arrStr.length ];
		for ( int i = 0; i < arr.length; i++ )
			arr[ i ] = Integer.parseInt( arrStr[ i ].trim() );
		return arr;
	}

	public static double[] parseDoubleArray( final String[] arrStr )
	{
		final double[] arr = new double[ arrStr.length ];
		for ( int i = 0; i < arr.length; i++ )
			arr[ i ] = Double.parseDouble( arrStr[ i ].trim() );
		return arr;
	}

	public static List< Integer > arrToList( final int[] arr )
	{
		final List< Integer> list = new ArrayList<>();
		for ( final int val : arr )
			list.add( val );
		return list;
	}


	public static Short[] toBoxedArray( final short[] arr )
	{
		final Short[] ret = new Short[ arr.length ];
		for ( int i = 0; i < ret.length; ++i )
			ret[ i ] = arr[ i ];
		return ret;
	}
	public static Integer[] toBoxedArray( final int[] arr )
	{
		final Integer[] ret = new Integer[ arr.length ];
		for ( int i = 0; i < ret.length; ++i )
			ret[ i ] = arr[ i ];
		return ret;
	}
	public static Long[] toBoxedArray( final long[] arr )
	{
		final Long[] ret = new Long[ arr.length ];
		for ( int i = 0; i < ret.length; ++i )
			ret[ i ] = arr[ i ];
		return ret;
	}


	public static < T extends RealType< T > & NativeType< T > > ImagePlusImg< FloatType, ? > convertImageToFloat( final RandomAccessibleInterval< T > src )
	{
		final ImagePlusImg< FloatType, ? > dst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( src ) );

		final Cursor< T > srcCursor = Views.flatIterable( src ).cursor();
		final Cursor< FloatType > dstCursor = Views.flatIterable( dst ).cursor();

		while ( srcCursor.hasNext() || dstCursor.hasNext() )
			dstCursor.next().set( srcCursor.next().getRealFloat() );

		return dst;
	}
}
