package org.janelia.stitching;

import net.imglib2.util.Pair;

/**
 * Holds an ordered pair of {@link TileInfo} objects.
 *
 * @author Igor Pisarev
 */

public class SubdividedTileBoxPair implements Pair< SubdividedTileBox, SubdividedTileBox >
{
	private SubdividedTileBox[] tileBoxPair;

	public SubdividedTileBoxPair( final SubdividedTileBox t1, final SubdividedTileBox t2 )
	{
		tileBoxPair = new SubdividedTileBox[] { t1, t2 };
	}

	protected SubdividedTileBoxPair() { }

	@Override
	public SubdividedTileBox getA()
	{
		return tileBoxPair[ 0 ];
	}

	@Override
	public SubdividedTileBox getB()
	{
		return tileBoxPair[ 1 ];
	}

	public SubdividedTileBox[] toArray()
	{
		return tileBoxPair.clone();
	}

	public void swap()
	{
		final SubdividedTileBox tmp = tileBoxPair[ 0 ];
		tileBoxPair[ 0 ] = tileBoxPair[ 1 ];
		tileBoxPair[ 1 ] = tmp;
	}

	public TilePair getOriginalTilePair()
	{
		return new TilePair( tileBoxPair[ 0 ].getFullTile(), tileBoxPair[ 1 ].getFullTile() );
	}

	@Override
	public String toString()
	{
		return "(" + tileBoxPair[ 0 ].getIndex() + "," + tileBoxPair[ 1 ].getIndex() + ")" + " of tiles " + getOriginalTilePair().toString();
	}
}
