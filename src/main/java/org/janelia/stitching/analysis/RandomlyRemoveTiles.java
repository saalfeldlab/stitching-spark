package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class RandomlyRemoveTiles
{
	private static final Random rnd = new Random( 69997 ); // repeatable results

	public static void main( final String[] args ) throws Exception
	{
		final String path = args[ 0 ];
		final int numTilesRemoved = Integer.parseInt( args[ 1 ] );
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( path ) ) );
		final List< TileInfo > newTiles = new ArrayList<>();

		final Set< Integer > removed = new HashSet<>();
		for ( int i = 0; i < numTilesRemoved; ++i )
		{
			int randomIndex;
			do
			{
				randomIndex = rnd.nextInt( tiles.length );
			}
			while ( removed.contains( randomIndex ) );

			removed.add( randomIndex );
		}

		for ( int i = 0; i < tiles.length; ++i )
			if ( !removed.contains( i ) )
				newTiles.add( tiles[ i ] );

		System.out.println( "Removed:" );
		for ( final int removedIndex : removed )
			System.out.println( "tile " + tiles[ removedIndex ].getIndex() );

		TileInfoJSONProvider.saveTilesConfiguration( newTiles.toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( path, "_minus_" + numTilesRemoved ) ) ) );
	}
}
