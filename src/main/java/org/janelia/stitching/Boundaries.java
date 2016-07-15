package org.janelia.stitching;

public class Boundaries {
	
	private float[] min, max;
	
	public Boundaries( final int dim ) {
		min = new float[ dim ];
		max = new float[ dim ];
	}
	
	public float getMin( final int d ) {
		return min[ d ];
	}
	
	public float getMax( final int d ) {
		return max[ d ];
	}
	
	public void setMin( final int d, final float val ) {
		min[ d ] = val;
	}

	public void setMax( final int d, final float val ) {
		max[ d ] = val;
	}
	
	public int getDimensionality() {
		assert min.length == max.length;
		return min.length;
	}
}
