package org.janelia.stitching;

import net.imglib2.util.Pair;

/**
 * Holds an ordered pair of {@link SubTile} objects.
 *
 * @author Igor Pisarev
 */

public class SubTilePair implements Pair< SubTile, SubTile >
{
	private SubTile[] subTilePair;

	public SubTilePair( final SubTile t1, final SubTile t2 )
	{
		subTilePair = new SubTile[] { t1, t2 };
	}

	protected SubTilePair() { }

	@Override
	public SubTile getA()
	{
		return subTilePair[ 0 ];
	}

	@Override
	public SubTile getB()
	{
		return subTilePair[ 1 ];
	}

	public SubTile[] toArray()
	{
		return subTilePair.clone();
	}

	public void swap()
	{
		final SubTile tmp = subTilePair[ 0 ];
		subTilePair[ 0 ] = subTilePair[ 1 ];
		subTilePair[ 1 ] = tmp;
	}

	public TilePair getFullTilePair()
	{
		return new TilePair( subTilePair[ 0 ].getFullTile(), subTilePair[ 1 ].getFullTile() );
	}

	@Override
	public String toString()
	{
		return "(" + subTilePair[ 0 ].getIndex() + "," + subTilePair[ 1 ].getIndex() + ")" + " of tiles " + getFullTilePair().toString();
	}
}
