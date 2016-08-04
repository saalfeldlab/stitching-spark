package org.janelia.stitching.analysis;

import java.io.IOException;
import java.util.ArrayList;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class FilterTileConfiguration
{
	public static void main( final String[] args ) throws IOException
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final String filenameFilter = "_ch1";

		final ArrayList< TileInfo > filteredTiles = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( tile.getFile().contains( filenameFilter ) )
				filteredTiles.add( tile );

		TileInfoJSONProvider.saveTilesConfiguration( filteredTiles.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix( args[0], filenameFilter ) );
	}
}
