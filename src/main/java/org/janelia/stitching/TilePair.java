package org.janelia.stitching;

import net.imglib2.util.Pair;

/**
 * Holds an ordered pair of {@link TileInfo} objects.
 *
 * @author Igor Pisarev
 */

public class TilePair implements Pair< TileInfo, TileInfo >
{
	private final TileInfo[] tilePair;

	public TilePair( final TileInfo t1, final TileInfo t2 )
	{
		tilePair = new TileInfo[] { t1, t2 };
	}

	@Override
	public TileInfo getA()
	{
		return tilePair[ 0 ];
	}

	@Override
	public TileInfo getB()
	{
		return tilePair[ 1 ];
	}

	public TileInfo[] toArray()
	{
		return tilePair.clone();
	}

	public void swap()
	{
		final TileInfo tmp = tilePair[ 0 ];
		tilePair[ 0 ] = tilePair[ 1 ];
		tilePair[ 1 ] = tmp;
	}

	@Override
	public String toString()
	{
		return "(" + tilePair[ 0 ].getIndex() + "," + tilePair[ 1 ].getIndex() + ")";
	}
}
