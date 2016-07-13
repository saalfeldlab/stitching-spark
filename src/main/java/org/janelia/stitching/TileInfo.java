package org.janelia.stitching;

import java.io.Serializable;

/**
 * @author pisarevi
 *
 */

public class TileInfo implements Serializable {
	
	private static final long serialVersionUID = -3986869827110711078L;
	
	// required
	private String file;
	private float[] position;
	
	// optional
	private int[] size;
	private Integer index;
	
	public String getFile() {
		return file;
	}
	
	public void setFile( final String file) {
		this.file = file;
	}
	
	public float[] getPosition() {
		return position;
	}
	
	public void setPosition( final float[] position ) {
		this.position = position;
	}
	
	public int[] getSize() {
		return size;
	}
	
	public void setSize( final int[] size ) {
		this.size = size;
		for ( final int s : size )
			assert s >= 0;
	}
	
	public Integer getIndex() {
		return index;
	}
	
	public void setIndex( final Integer index ) {
		this.index = index;
	}
	
	public int getDimensionality() {
		return position.length;
	}
}
