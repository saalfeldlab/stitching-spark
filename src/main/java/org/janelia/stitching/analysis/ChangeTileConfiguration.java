package org.janelia.stitching.analysis;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class ChangeTileConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final TileInfo[] tilesFrom = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final TileInfo[] tilesTo = TileInfoJSONProvider.loadTilesConfiguration( args[ 1 ] );

		if ( tilesFrom.length != tilesTo.length )
			throw new Exception( "Tiles count mismatch" );

		for ( int i = 0; i < tilesFrom.length; i++ )
			tilesTo[ i ].setPosition( tilesFrom[ i ].getPosition() );

		TileInfoJSONProvider.saveTilesConfiguration( tilesTo, Utils.addFilenameSuffix( args[ 1 ], "_changed" ) );
	}
}
