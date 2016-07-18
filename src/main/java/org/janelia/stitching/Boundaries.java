package org.janelia.stitching;

public class Boundaries {
	
	private int[] min, max;
	
	public Boundaries( final int dim ) {
		min = new int[ dim ];
		max = new int[ dim ];
	}
	
	public int getMin( final int d ) {
		return min[ d ];
	}
	
	public int getMax( final int d ) {
		return max[ d ];
	}
	
	public void setMin( final int d, final int val ) {
		min[ d ] = val;
	}

	public void setMax( final int d, final int val ) {
		max[ d ] = val;
	}
	
	public int getDimensionality() {
		assert min.length == max.length;
		return min.length;
	}
	
	public boolean validate() {
		for ( int d = 0; d < getDimensionality(); d++ )
			if ( getMax( d ) < getMin( d ) )
				return false;
		return true;
	}
}
