package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
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
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TreeMap<Integer,TileInfo> tilesFrom = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) ) );
		final TreeMap<Integer, TileInfo > tilesTo = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) ) );
		final String what = args[ 2 ].trim();

		final int tilesPerChannel = tilesTo.size();
		final int ch = 0;//tilesTo.firstKey() / tilesPerChannel;
		if ( tilesTo.firstKey() % tilesPerChannel != 0 )
			throw new Exception( "Only a part of a channel image collection is present" );

		System.out.println( "tilesTo before: " + tilesTo.size() );
		System.out.println( "ch=" + ch );

		for (final Iterator<Map.Entry<Integer, TileInfo>> it = tilesTo.entrySet().iterator(); it.hasNext(); )
		{
			final Map.Entry<Integer, TileInfo> entry = it.next();
			if( !tilesFrom.containsKey( entry.getKey() - ch * tilesPerChannel ) )
				it.remove();
		}

		int processed = 0;
		for ( final TileInfo tileFrom : tilesFrom.values() )
		{
			final int ind = ch * tilesPerChannel + tileFrom.getIndex();
			if ( !tilesTo.containsKey( ind ) )
				continue;

			processed++;
			final TileInfo tileTo = tilesTo.get( ind );

			if ( what.equals( "position" ) )
				tileTo.setStagePosition( tileFrom.getStagePosition() );
			else if ( what.equals( "filepath" ) )
				tileTo.setFilePath( tileFrom.getFilePath() );
			else
				throw new Exception( "Unknown property: " + what );
		}

		System.out.println( "Processed: " + processed );

		final TileInfo[] result = new ArrayList<>( tilesTo.values() ).toArray( new TileInfo[0] );
		TileInfoJSONProvider.saveTilesConfiguration( result, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 1 ], "_changed_" + what ) ) ) );

		System.out.println( "Done" );
	}
}
