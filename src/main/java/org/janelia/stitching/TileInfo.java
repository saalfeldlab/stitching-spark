package org.janelia.stitching;

import java.io.Serializable;

import net.imglib2.RealInterval;
import net.imglib2.RealPositionable;

/**
 * Represents tile image metadata.
 *
 * @author Igor Pisarev
 */

public class TileInfo implements Cloneable, Serializable, RealInterval {

	private static final long serialVersionUID = -3986869827110711078L;

	private ImageType type;
	private Integer index;
	private String file;
	private double[] position;
	private long[] size;
	private double[] pixelResolution;

	public TileInfo( final int dim ) {
		position = new double[ dim ];
		size = new long[ dim ];
		pixelResolution = new double[ dim ];
	}

	protected TileInfo() { }

	public String getFilePath() {
		return file;
	}

	public void setFilePath( final String filePath ) {
		this.file = filePath;
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

	public double[] getMax() {
		final double[] max = new double[ numDimensions() ];
		for ( int d = 0; d < max.length; d++ )
			max[ d ] = getMax( d );
		return max;
	}

	public double getMax( final int d ) {
		return position[ d ] + size[ d ] - 1;
	}

	public void setPosition( final double[] position ) {
		this.position = position;
	}

	public double[] getPixelResolution() {
		return pixelResolution;
	}

	public double getPixelResolution( final int d ) {
		return pixelResolution[ d ];
	}

	public void setPixelResolution( final double[] pixelResolution ) {
		this.pixelResolution = pixelResolution;
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
		if ( size != null )
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

	public boolean isNull() {
		return file == null;
	}

	@Override
	public int numDimensions() {
		return position.length;
	}

	public Boundaries getBoundaries() {
		final Boundaries b = new Boundaries( numDimensions() );
		for ( int d = 0; d < numDimensions(); d++ ) {
			b.setMin( d, Math.round( getPosition(d) ) );
			b.setMax( d, Math.round( getPosition(d) ) + getSize(d) - 1 );
		}
		return b;
	}

	@Override
	public TileInfo clone() {
		final TileInfo newTile = new TileInfo();
		newTile.setType( type );
		newTile.setIndex( index == null ? null : new Integer( index.intValue() ) );
		newTile.setFilePath( file );
		newTile.setPosition( position == null ? null : position.clone() );
		newTile.setSize( size == null ? null : size.clone() );
		newTile.setPixelResolution( pixelResolution == null ? null : pixelResolution.clone() );
		return newTile;
	}

	@Override
	public double realMin( final int d )
	{
		return position[ d ];
	}

	@Override
	public void realMin( final double[] min )
	{
		for ( int d = 0; d < min.length; ++d )
			min[ d ] = realMin( d );
	}

	@Override
	public void realMin( final RealPositionable min )
	{
		for ( int d = 0; d < min.numDimensions(); ++d )
			min.setPosition( realMin( d ), d );
	}

	@Override
	public double realMax( final int d )
	{
		return position[ d ] + size[ d ] - 1;
	}

	@Override
	public void realMax( final double[] max )
	{
		for ( int d = 0; d < max.length; ++d )
			max[ d ] = realMax( d );
	}

	@Override
	public void realMax( final RealPositionable max )
	{
		for ( int d = 0; d < max.numDimensions(); ++d )
			max.setPosition( realMax( d ), d );
	}
}
