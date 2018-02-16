package org.janelia.stitching.analysis;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;

public class ChangeTilePaths
{
	public static void main( final String[] args ) throws IOException
	{
		final String config = args[ 0 ];
		final String newTileFolder = args[ 1 ];
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( DataProviderFactory.createFSDataProvider().getJsonReader( URI.create( config ) ) );
		for ( final TileInfo tile : tiles )
			tile.setFilePath( Paths.get( newTileFolder, Paths.get( tile.getFilePath() ).getFileName().toString() ).toString() );
		TileInfoJSONProvider.saveTilesConfiguration( tiles, DataProviderFactory.createFSDataProvider().getJsonWriter( URI.create( config ) ) );
		System.out.println( "Done" );
	}
}
