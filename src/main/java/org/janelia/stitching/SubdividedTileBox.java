package org.janelia.stitching;

import net.imglib2.RealInterval;
import net.imglib2.RealPositionable;

/**
 * Represents a box within a tile as a result of subdivision.
 *
 * @author Igor Pisarev
 */

public class SubdividedTileBox implements RealInterval {

	public static class Tag
	{
		private final int value;

		public Tag( final int value )
		{
			this.value = value;
		}

		@Override
		public boolean equals( final Object other )
		{
			if ( other instanceof Tag )
				return value == ( ( Tag ) other ).value;
			else
				return super.equals( other );
		}

		@Override
		public int hashCode()
		{
			return Integer.hashCode( value );
		}
	}

	private Integer index;
	private double[] position;
	private long[] size;

	private final TileInfo fullTile;
	private final Tag tag;

	public SubdividedTileBox( final TileInfo fullTile, final Tag tag )
	{
		this.fullTile = fullTile;
		this.tag = tag;

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

	public double[] getMax()
	{
		final double[] max = new double[ numDimensions() ];
		for ( int d = 0; d < max.length; d++ )
			max[ d ] = getMax( d );
		return max;
	}

	public double getMax( final int d )
	{
		return position[ d ] + size[ d ] - 1;
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

	public Tag getTag()
	{
		return tag;
	}

	@Override
	public int numDimensions()
	{
		return fullTile.numDimensions();
	}

	public Boundaries getBoundaries()
	{
		final Boundaries b = new Boundaries( numDimensions() );
		for ( int d = 0; d < numDimensions(); d++ ) {
			b.setMin( d, Math.round( getPosition(d) ) );
			b.setMax( d, Math.round( getPosition(d) ) + getSize(d) - 1 );
		}
		return b;
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
