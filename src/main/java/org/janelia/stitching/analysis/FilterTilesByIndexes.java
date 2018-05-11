package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class FilterTilesByIndexes
{
	public static void main( final String[] args ) throws Exception
	{
		final String tileConfigPath = args[ 0 ];
		final String tileIndexesStr = args[ 1 ];
		final Set< Integer > tileIndexes = new TreeSet<>();
		for ( final String tileIndexStr : tileIndexesStr.trim().split( "," ) )
			tileIndexes.add( Integer.parseInt( tileIndexStr ) );

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args [ 0 ] ) ) ) );

		final List< TileInfo > filteredTiles = new ArrayList<>();
		for ( final Integer tileIndex : tileIndexes )
			filteredTiles.add( tilesMap.get( tileIndex ) );

		TileInfoJSONProvider.saveTilesConfiguration(
				filteredTiles.toArray( new TileInfo[ 0 ] ),
				dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( tileConfigPath, "_filtered" ) ) )
			);

		System.out.println( "Filtered " + filteredTiles.size() + " tiles out of " + tilesMap.size() );
	}
}
