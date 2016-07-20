package org.janelia.stitching;

public class Conversions {

	// for compatibility across libraries
	
	public static float[] toFloatArray( final double[] arr ) {
		final float[] ret = new float[ arr.length ];
		for ( int i = 0; i < arr.length; i++ )
			ret[ i ] = (float)arr[ i ];
		return ret;
	}
	
	public static long[] toLongArray( final int[] arr ) {
		final long[] ret = new long[ arr.length ];
		for ( int i = 0; i < arr.length; i++ )
			ret[ i ] = (long)arr[ i ];
		return ret;
	}
}
