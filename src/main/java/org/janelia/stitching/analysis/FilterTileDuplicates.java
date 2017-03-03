package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.janelia.util.ComparableTuple;
import org.janelia.util.Conversions;

public class FilterTileDuplicates
{
	public static void main( final String[] args ) throws Exception
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		System.out.println( "Total number of tiles = " + tiles.length );

		// build a map of tile dimensions
		final TreeMap< ComparableTuple< Long >, List< TileInfo > > dimensions = new TreeMap<>();
		for ( final TileInfo tile : tiles )
		{
			final ComparableTuple< Long > key = new ComparableTuple<>( Conversions.toBoxedArray( tile.getSize() ) );
			if ( !dimensions.containsKey( key ) )
				dimensions.put( key, new ArrayList<>() );
			dimensions.get( key ).add( tile );
		}

		// sort the tile dimensions by the number of tiles
		final TreeMap< Integer, ComparableTuple< Long > > tilesCountToDimensions = new TreeMap<>();
		for ( final Entry< ComparableTuple< Long >, List< TileInfo > > entry : dimensions.entrySet() )
			tilesCountToDimensions.put( entry.getValue().size(), entry.getKey() );

		System.out.println( "Tiles count to dimensions:" );
		for ( final Entry< Integer, ComparableTuple< Long > > entry : tilesCountToDimensions.descendingMap().entrySet() )
			System.out.println( String.format( "  %s: %s", entry.getKey(), entry.getValue() ) );


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

		// build a helper structure for having the inverse mapping: tile -> dimensions
		final TreeMap< Integer, ComparableTuple< Long > > tileToDimensions = new TreeMap<>();
		for ( final Entry< ComparableTuple< Long >, List< TileInfo > > entry : dimensions.entrySet() )
			for ( final TileInfo tile : entry.getValue() )
				tileToDimensions.put( tile.getIndex(), entry.getKey() );

		// loop over the coordinate groups
		final Set< TileInfo > retainedTilesSet = new HashSet<>(), removedTilesSet = new HashSet<>();
		for ( final Entry< ComparableTuple< Integer >, List< TileInfo > > entry : coordinatesToTiles.entrySet() )
		{
			// check if there are any duplicates for the particular stage position
			if ( entry.getValue().size() > 1 )
			{
				System.out.println( String.format( "%d tiles at %s", entry.getValue().size(), entry.getKey() ) );

				// try to find the candidate tile to retain that has 'regular' dimensions
				TileInfo candidateTile = null;
				for ( final TileInfo duplicateTile : entry.getValue() )
				{
					if ( tileToDimensions.get( duplicateTile.getIndex() ).compareTo( tilesCountToDimensions.lastEntry().getValue() ) == 0 )
					{
						if ( candidateTile == null )
							candidateTile = duplicateTile;
						else
							throw new Exception( "Duplicate tiles don't have a single candidate" );
					}
					else
					{
						removedTilesSet.add( duplicateTile );
					}
				}

				if ( candidateTile == null )
					throw new Exception( "Duplicate tiles don't have a candidate" );

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
			System.out.println( String.format("%d tiles out of %d tiles are retained", retainedTiles.size(), tiles.length ) );
			TileInfoJSONProvider.saveTilesConfiguration( retainedTiles.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix( args[ 0 ], "_retained" ) );
		}

		ComparableTuple< Long > tileDimensions = null;
		boolean sameDimensionsForAllTiles = true;
		for ( final TileInfo tile : retainedTiles )
		{
			final ComparableTuple< Long > key = tileToDimensions.get( tile.getIndex() );
			if ( tileDimensions == null )
				tileDimensions = key;
			else if ( tileDimensions.compareTo( key ) != 0 )
			{
				sameDimensionsForAllTiles = false;
				break;
			}
		}
		System.out.println( sameDimensionsForAllTiles ? "OK: dimensions are the same for all tiles" : "Dimensions are different!" );
	}
}
