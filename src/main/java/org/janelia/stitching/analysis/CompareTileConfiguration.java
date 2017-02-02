package org.janelia.stitching.analysis;

import java.util.TreeMap;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class CompareTileConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final TreeMap<Integer,TileInfo> tilesBefore = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] ) );
		final TreeMap<Integer, TileInfo > tilesAfter = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( args[ 1 ] ) );

		System.out.println( "Tiles before: " + tilesBefore.size() );
		System.out.println( "Tiles after: " + tilesAfter.size() );
	}
}
