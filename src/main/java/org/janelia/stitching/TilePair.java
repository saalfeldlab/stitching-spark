package org.janelia.stitching;

import java.io.Serializable;

/**
 * Holds an ordered pair of {@link TileInfo} objects.
 *
 * @author Igor Pisarev
 */

public class TilePair implements Serializable
{
	private static final long serialVersionUID = -3199669876217383109L;

	private TileInfo[] tilePair;

	public TilePair( final TileInfo t1, final TileInfo t2 )
	{
		tilePair = new TileInfo[] { t1, t2 };
	}

	protected TilePair() { }

	public TileInfo[] toArray() { return tilePair.clone(); }
	public TileInfo first() { return tilePair[ 0 ]; }
	public TileInfo second() { return tilePair[ 1 ]; }
}
