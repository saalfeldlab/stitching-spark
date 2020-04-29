package org.janelia.stitching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;

public class CheckTilesOnDisk
{
	public static boolean allTilesArePresent( final DataProvider dataProvider, final List< TileInfo[] > channels ) throws IOException
	{
		// find missing tiles on disk
		final Map< Integer, List< TileInfo > > channelsToMissingTiles = new TreeMap<>();
		for ( int ch = 0; ch < channels.size(); ++ch )
		{
			final List< TileInfo > missingTiles = new ArrayList<>();
			for ( final TileInfo tile : channels.get( ch ) )
				if ( !dataProvider.exists( tile.getFilePath() ) )
					missingTiles.add( tile );
			if ( !missingTiles.isEmpty() )
				channelsToMissingTiles.put( ch, missingTiles );
		}

		// print missing tiles if any
		if ( !channelsToMissingTiles.isEmpty() )
		{
			System.out.println( System.lineSeparator() + "The following tiles do not exist on disk:" + System.lineSeparator() );
			for ( final List< TileInfo > missingTiles : channelsToMissingTiles.values() )
			{
				for ( final TileInfo missingTile : missingTiles )
					System.out.println( "  " + missingTile.getFilePath() );
				System.out.println();
			}
		}

		return channelsToMissingTiles.isEmpty();
	}

	public static void main( final String[] args ) throws Exception
	{
		if ( args.length == 0 )
			throw new IllegalArgumentException( "no arguments provided" );

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final List< TileInfo[] > channels = new ArrayList<>();
		for ( final String path : args )
			channels.add( dataProvider.loadTiles( path ) );

		System.out.println( "Searching for missing tiles..." );
		if ( allTilesArePresent( dataProvider, channels ) )
			System.out.println( "All tiles are present" );
	}
}
