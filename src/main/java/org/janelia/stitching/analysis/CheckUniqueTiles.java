package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class CheckUniqueTiles
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final Set< String > gridCoordinates = new HashSet<>();
		for ( final TileInfo tile : tiles )
		{
			if ( !dataProvider.fileExists( URI.create( tile.getFilePath() ) ) )
				throw new Exception( "tile does not exist" );
			gridCoordinates.add( Utils.getTileCoordinatesString( tile ).toLowerCase() );
		}
		if ( gridCoordinates.size() != tiles.length )
			throw new Exception( "some of the tiles are duplicated" );
		System.out.println( "OK" );
	}
}
