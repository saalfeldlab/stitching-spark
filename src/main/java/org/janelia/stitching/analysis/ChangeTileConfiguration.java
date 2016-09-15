package org.janelia.stitching.analysis;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

/**
 * Replaces tile positions in one tile configuration with positions from another tile configuration.
 *
 * @author Igor Pisarev
 */

public class ChangeTileConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final TileInfo[] tilesFrom = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final TileInfo[] tilesTo = TileInfoJSONProvider.loadTilesConfiguration( args[ 1 ] );
		final String what = args[ 2 ].trim();

		if ( tilesFrom.length != tilesTo.length )
			throw new Exception( "Tiles count mismatch" );

		for ( int i = 0; i < tilesFrom.length; i++ )
		{
			if ( what.equals( "position" ) )
				tilesTo[ i ].setPosition( tilesFrom[ i ].getPosition() );
			else if ( what.equals( "filepath" ) )
				tilesTo[ i ].setFilePath( tilesFrom[ i ].getFilePath() );
			else
				throw new Exception( "Unknown property: " + what );
		}

		TileInfoJSONProvider.saveTilesConfiguration( tilesTo, Utils.addFilenameSuffix( args[ 1 ], "_changed_" + what ) );

		System.out.println( "Done" );
	}
}
