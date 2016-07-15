package org.janelia.stitching;

import java.io.Serializable;

/**
 * @author pisarevi
 *
 */

public class TileInfo implements Serializable {
	
	private static final long serialVersionUID = -3986869827110711078L;
	
	private Integer index;
	private String file;
	private float[] position;
	private int[] size;
	
	public TileInfo( final int dim ) {
		position = new float[ dim ];
		size = new int[ dim ];
	}
	
	protected TileInfo() { }
	
	public String getFile() {
		return file;
	}
	
	public void setFile( final String file) {
		this.file = file;
	}
	
	public float getPosition( final int d ) {
		return position[ d ];
	}
	
	public void setPosition( final int d, final float val ) {
		position[ d ] = val;
	}
	
	public float[] getPosition() {
		return position;
	}
	
	public void setPosition( final float[] position ) {
		this.position = position;
	}
	
	public int getSize( final int d ) {
		return size[ d ];
	}
	
	public void setSize( final int d, final int val ) {
		assert val >= 0;
		size[ d ] = val;
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
	
	@Override
	public TileInfo clone() {
		final TileInfo newTile = new TileInfo();
		newTile.setIndex( index == null ? null : (int)index);
		newTile.setFile( file );
		newTile.setPosition( position == null ? null : position.clone() );
		newTile.setSize( size == null ? null : size.clone() );
		return newTile;
	}
}
