package org.janelia.stitching;

import net.imglib2.FinalRealInterval;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineGet;

/**
 * Represents tile image metadata.
 *
 * @author Igor Pisarev
 */

public class TileInfo implements Cloneable {

	private ImageType type;
	private Integer index;
	private String file;
	private double[] position;
	private long[] size;
	private double[] pixelResolution;

	private AffineGet transform;

	public TileInfo( final int dim )
	{
		position = new double[ dim ];
		size = new long[ dim ];
		pixelResolution = new double[ dim ];
	}

	public TileInfo( final double[] stagePosition )
	{
		this( stagePosition.length );
		this.position = stagePosition;
	}

	public TileInfo( final double[] stagePosition, final long[] size )
	{
		this( stagePosition );
		if ( stagePosition.length != size.length )
			throw new IllegalArgumentException( "wrong dimensionality" );
		setSize( size );
	}

	TileInfo() { }

	public String getFilePath() {
		return file;
	}

	public void setFilePath( final String filePath ) {
		this.file = filePath;
	}

	public double getStagePosition( final int d ) {
		return position[ d ];
	}

	public void setStagePosition( final int d, final double val ) {
		position[ d ] = val;
	}

	public double[] getStagePosition() {
		return position;
	}

	public void setStagePosition( final double[] position ) {
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

	public AffineGet getTransform() {
		return transform;
	}

	public void setTransform( final AffineGet transform ) {
		this.transform = transform;
	}

	public boolean isNull() {
		return file == null;
	}

	public int numDimensions() {
		return position.length;
	}

	public RealInterval getStageInterval() {
		final double[] min = new double[ position.length ], max = new double[ position.length ];
		for ( int d = 0; d < min.length; ++d )
		{
			min[ d ] = position[ d ];
			max[ d ] = position[ d ] + size[ d ] - 1;
		}
		return new FinalRealInterval( min, max );
	}

	@Override
	public TileInfo clone()
	{
		final TileInfo newTile = new TileInfo( position == null ? null : position.clone() );
		newTile.setType( type );
		newTile.setIndex( index == null ? null : new Integer( index.intValue() ) );
		newTile.setFilePath( file );
		newTile.setSize( size == null ? null : size.clone() );
		newTile.setPixelResolution( pixelResolution == null ? null : pixelResolution.clone() );
		newTile.setTransform( transform == null ? null : ( AffineGet ) transform.copy() );
		return newTile;
	}
}
