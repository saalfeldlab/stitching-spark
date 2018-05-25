package org.janelia.stitching;

import net.imglib2.RealInterval;
import net.imglib2.RealPositionable;

/**
 * Represents a box within a tile as a result of subdivision.
 *
 * @author Igor Pisarev
 */

public class SubdividedTileBox implements RealInterval
{
	private final TileInfo fullTile;

	private Integer index;
	private double[] position;
	private long[] size;

	public SubdividedTileBox( final TileInfo fullTile )
	{
		this.fullTile = fullTile;

		position = new double[ fullTile.numDimensions() ];
		size = new long[ fullTile.numDimensions() ];
	}

	public double getPosition( final int d )
	{
		return position[ d ];
	}

	public void setPosition( final int d, final double val )
	{
		position[ d ] = val;
	}

	public double[] getPosition()
	{
		return position;
	}

	public void setPosition( final double[] position )
	{
		this.position = position;
	}

	public long getSize( final int d )
	{
		return size[ d ];
	}

	public void setSize( final int d, final long val )
	{
		assert val >= 0;
		size[ d ] = val;
	}

	public long[] getSize()
	{
		return size;
	}

	public void setSize( final long[] size )
	{
		this.size = size;
		if ( size != null )
			for ( final long s : size )
				assert s >= 0;
	}

	public Integer getIndex()
	{
		return index;
	}

	public void setIndex( final Integer index )
	{
		this.index = index;
	}

	public TileInfo getFullTile()
	{
		return fullTile;
	}

	@Override
	public int numDimensions()
	{
		return fullTile.numDimensions();
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
