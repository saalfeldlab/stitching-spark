package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.Map;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class ReplaceStageCoordinates
{
	public static void main( final String[] args ) throws Exception
	{
		final String tilesConfigPath = args[ 0 ], replacementCoordsTilesPath = args[ 1 ];
		final DataProvider dataProvider  = DataProviderFactory.createFSDataProvider();
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( tilesConfigPath ) ) );
		final Map< Integer, TileInfo > replacementCoordsTiles = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( replacementCoordsTilesPath ) ) ) );
		for ( final TileInfo tile : tiles )
			tile.setStagePosition( replacementCoordsTiles.get( tile.getIndex() ).getStagePosition().clone() );
		final String tilesConfigOutputPath = Utils.addFilenameSuffix( tilesConfigPath, "_replaced-stage-coords" );
		TileInfoJSONProvider.saveTilesConfiguration( tiles, dataProvider.getJsonWriter( URI.create( tilesConfigOutputPath ) ) );
		System.out.println( "Done" );
	}
}
