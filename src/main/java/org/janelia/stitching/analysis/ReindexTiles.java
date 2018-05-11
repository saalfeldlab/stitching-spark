package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class ReindexTiles
{
	public static void main( final String[] args ) throws Exception
	{
		final String referenceTilesConfigPath = args[ 0 ], checkTilesConfigPath = args[ 1 ];
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > referenceTilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( referenceTilesConfigPath ) ) ) );
		final TileInfo[] checkTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( checkTilesConfigPath ) ) );

		final Map< String, TileInfo > referenceTilesGridCoordinates = new HashMap<>();
		for ( final TileInfo referenceTile : referenceTilesMap.values() )
			referenceTilesGridCoordinates.put( Utils.getTileCoordinatesString( referenceTile ), referenceTile );

		for ( final TileInfo checkTile : checkTiles )
		{
			final TileInfo referenceTile = referenceTilesGridCoordinates.get( Utils.getTileCoordinatesString( checkTile ) );
			checkTile.setIndex( referenceTile.getIndex() );
		}

		TileInfoJSONProvider.saveTilesConfiguration( checkTiles, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( checkTilesConfigPath, "_reindexed" ) ) ) );
	}
}
