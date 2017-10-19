package org.janelia.stitching.analysis;

import java.io.IOException;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class IndexTileConfiguration
{
	public static void main(final String[] args) throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( args[0] ) );
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ].setIndex( i );

		TileInfoJSONProvider.saveTilesConfiguration(tiles, dataProvider.getJsonWriter( Utils.addFilenameSuffix(args[0], "_indexed" ) ));
	}
}
