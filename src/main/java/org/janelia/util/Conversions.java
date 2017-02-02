package org.janelia.util;

import java.util.ArrayList;
import java.util.List;

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
}
