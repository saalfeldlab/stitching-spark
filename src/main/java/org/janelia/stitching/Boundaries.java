package org.janelia.stitching;

/**
 * @author pisarevi
 *
 */

public class Boundaries {
	
	private long[] min, max;
	
	public Boundaries( final int dim ) {
		min = new long[ dim ];
		max = new long[ dim ];
	}
	
	public long[] getMin() {
		return min.clone();
	}
	
	public long[] getMax() {
		return max.clone();
	}
	
	public long getMin( final int d ) {
		return min[ d ];
	}
	
	public long getMax( final int d ) {
		return max[ d ];
	}
	
	public void setMin( final int d, final long val ) {
		min[ d ] = val;
	}

	public void setMax( final int d, final long val ) {
		max[ d ] = val;
	}
	
	public int getDimensionality() {
		assert min.length == max.length;
		return min.length;
	}
	
	public long getDimensions( final int d ) {
		return getMax( d ) - getMin( d );
	}
	
	public long[] getDimensions() {
		final long[] dimensions = new long [ getDimensionality() ];
		for ( int d = 0; d < dimensions.length; d++ )
			dimensions[ d ] = getDimensions( d );
		return dimensions;
	}
	
	public boolean validate() {
		for ( int d = 0; d < getDimensionality(); d++ )
			if ( getMax( d ) < getMin( d ) )
				return false;
		return true;
	}
}
