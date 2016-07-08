package org.janelia.stitching;

import java.io.Serializable;

/**
 * @author pisarevi
 *
 */

public class TileInfo implements Serializable {
	
	private static final long serialVersionUID = -3986869827110711078L;
	
	private String file;
	private float[] position;
	private int[] size;
	private int index;
	
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
		this.size= size;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex( final int index ) {
		this.index = index;
	}
	
	public int getDimensionality() {
		assert position.length == size.length;
		return size.length;
	}
}
