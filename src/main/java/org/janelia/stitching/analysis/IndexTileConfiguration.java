package org.janelia.stitching.analysis;

import java.io.IOException;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class IndexTileConfiguration
{
	public static void main(final String[] args) throws IOException
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration(args[0]);
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ].setIndex( i );

		TileInfoJSONProvider.saveTilesConfiguration(tiles, Utils.addFilenameSuffix(args[0], "_indexed"));
	}
}
