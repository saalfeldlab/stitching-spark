package org.janelia.stitching.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class CopyTilesPositions
{
	public static void main( final String[] args ) throws IOException
	{
		final TileInfo[] tilesCopyFrom = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final TreeMap< Integer, TileInfo > tilesMapCopyTo = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( args[ 1 ] ) );
		final List< TileInfo > tilesTo = new ArrayList<>();
		for ( final TileInfo tileCopyFrom : tilesCopyFrom )
		{
			final TileInfo tileTo = tilesMapCopyTo.get( tileCopyFrom.getIndex() );
			tileTo.setPosition( tileCopyFrom.getPosition().clone() );
			tilesTo.add( tileTo );
		}
		System.out.println( "tilesCopyFrom size="+tilesCopyFrom.length);
		System.out.println( "tilesMapCopyTo size="+tilesMapCopyTo.size() );
		System.out.println( "tilesTo size="+tilesTo.size() );
		TileInfoJSONProvider.saveTilesConfiguration( tilesTo.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix( args[ 1 ], "_final" ) );
	}
}
