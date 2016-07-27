package org.janelia.stitching;

import java.io.Serializable;

/**
 * @author pisarevi
 *
 */

public class TileInfo implements Serializable {
	
	private static final long serialVersionUID = -3986869827110711078L;
	
	private ImageType type;
	private Integer index;
	private String file;
	private double[] position;
	private long[] size;
	
	public TileInfo( final int dim ) {
		position = new double[ dim ];
		size = new long[ dim ];
	}
	
	protected TileInfo() { }
	
	public String getFile() {
		return file;
	}
	
	public void setFile( final String file) {
		this.file = file;
	}
	
	public double getPosition( final int d ) {
		return position[ d ];
	}
	
	public void setPosition( final int d, final double val ) {
		position[ d ] = val;
	}
	
	public double[] getPosition() {
		return position;
	}
	
	public void setPosition( final double[] position ) {
		this.position = position;
	}
	
	public long getSize( final int d ) {
		return size[ d ];
	}
	
	public void setSize( final int d, final long val ) {
		assert val >= 0;
		size[ d ] = val;
	}
	
	public long[] getSize() {
		return size;
	}
	
	public void setSize( final long[] size ) {
		this.size = size;
		for ( final long s : size )
			assert s >= 0;
	}
	
	public ImageType getType() {
		return type;
	}
	
	public void setType( final ImageType type ) {
		this.type = type;
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
	
	public Boundaries getBoundaries() {
		final Boundaries b = new Boundaries( getDimensionality() );
		for ( int d = 0; d < getDimensionality(); d++ ) {
			b.setMin( d, Math.round( getPosition(d) ) );
			b.setMax( d, Math.round( getPosition(d) ) + getSize(d) );
		}
		return b;
	}
	
	@Override
	public TileInfo clone() {
		final TileInfo newTile = new TileInfo();
		newTile.setType( type );
		newTile.setIndex( index == null ? null : new Integer( index.intValue() ) );
		newTile.setFile( file );
		newTile.setPosition( position == null ? null : position.clone() );
		newTile.setSize( size == null ? null : size.clone() );
		return newTile;
	}
}
