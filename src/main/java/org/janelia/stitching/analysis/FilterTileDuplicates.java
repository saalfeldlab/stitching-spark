package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.janelia.util.ComparableTuple;
import org.janelia.util.Conversions;

public class FilterTileDuplicates
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		System.out.println( "Total number of tiles = " + tiles.length );

		// build a map of tile coordinates to find duplicates
		final TreeMap< ComparableTuple< Integer >, List< TileInfo > > coordinatesToTiles = new TreeMap<>();
		for ( final TileInfo tile : tiles )
		{
			final int[] coordinates = Utils.getTileCoordinates( tile );
			final ComparableTuple< Integer > key = new ComparableTuple<>( Conversions.toBoxedArray( coordinates ) );
			if ( !coordinatesToTiles.containsKey( key ) )
				coordinatesToTiles.put( key, new ArrayList<>() );
			coordinatesToTiles.get( key ).add( tile );
		}

		// loop over the coordinate groups
		final Set< TileInfo > retainedTilesSet = new HashSet<>();
		for ( final Entry< ComparableTuple< Integer >, List< TileInfo > > entry : coordinatesToTiles.entrySet() )
		{
			// check if there are any duplicates for the particular stage position
			if ( entry.getValue().size() > 1 )
			{
				System.out.println( String.format( "Found %d tiles at %s", entry.getValue().size(), entry.getKey() ) );

				// pick the tile with the largest timestamp because the image may have been retaken in case of camera errors
				long largestTimestamp = Long.MIN_VALUE;
				TileInfo candidateTile = null;
				for ( final TileInfo duplicateTile : entry.getValue() )
				{
					final long timestamp = Utils.getTileTimestamp( duplicateTile );
					if ( largestTimestamp < timestamp )
					{
						largestTimestamp = timestamp;
						candidateTile = duplicateTile;
					}
				}
				retainedTilesSet.add( candidateTile );
			}
			else
			{
				retainedTilesSet.add( entry.getValue().get( 0 ) );
			}
		}

		final List< TileInfo > retainedTiles = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( retainedTilesSet.contains( tile ) )
				retainedTiles.add( tile );

		System.out.println( "---------" );
		if ( retainedTiles.size() == tiles.length )
		{
			System.out.println( "All tiles are retained, no changes" );
		}
		else
		{
			System.out.println( String.format("Removed %d duplicated tiles out of %d tiles", tiles.length - retainedTiles.size(), tiles.length ) );
			TileInfoJSONProvider.saveTilesConfiguration( retainedTiles.toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 0 ], "_retained" ) ) ) );
		}
	}
}
