package org.janelia.stitching.analysis;

import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class CompareTileConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TreeMap<Integer,TileInfo> tilesBefore = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( args[ 0 ] ) ) );
		final TreeMap<Integer, TileInfo > tilesAfter = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( args[ 1 ] ) ) );

		System.out.println( "Tiles before: " + tilesBefore.size() );
		System.out.println( "Tiles after: " + tilesAfter.size() );
	}
}
