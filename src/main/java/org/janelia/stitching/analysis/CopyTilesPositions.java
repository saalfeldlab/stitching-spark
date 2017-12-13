package org.janelia.stitching.analysis;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class CopyTilesPositions
{
	public static void main( final String[] args ) throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tilesCopyFrom = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final TreeMap< Integer, TileInfo > tilesMapCopyTo = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) ) );
		final List< TileInfo > tilesTo = new ArrayList<>();
		for ( final TileInfo tileCopyFrom : tilesCopyFrom )
		{
			final TileInfo tileTo = tilesMapCopyTo.get( tileCopyFrom.getIndex() );
			tileTo.setPosition( tileCopyFrom.getPosition().clone() );
			tilesTo.add( tileTo );
		}
		System.out.println( "tilesCopyFrom size="+tilesCopyFrom.length);
		System.out.println( "tilesMapCopyTo size="+tilesMapCopyTo.size() );
		System.out.println( "tilesTo size="+tilesTo.size() );
		TileInfoJSONProvider.saveTilesConfiguration( tilesTo.toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 1 ], "_final" ) ) ) );
	}
}
