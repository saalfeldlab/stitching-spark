package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import net.imglib2.util.ComparablePair;

public class FilterTileDuplicates
{
	public static void main( final String[] args ) throws Exception
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		System.out.println( "Total number of tiles = " + tiles.length );

		// build a map of tile dimensions
		final TreeMap< ComparablePair< Long, ComparablePair< Long, Long > >, List< TileInfo > > dimensions = new TreeMap<>();
		for ( final TileInfo tile : tiles )
		{
			final ComparablePair< Long, ComparablePair< Long, Long > > key = new ComparablePair<>( tile.getSize( 0 ), new ComparablePair<>( tile.getSize( 1 ), tile.getSize( 2 ) ) );
			if ( !dimensions.containsKey( key ) )
				dimensions.put( key, new ArrayList<>() );
			dimensions.get( key ).add( tile );
		}

		// sort the tile dimensions by the number of tiles
		final TreeMap< Integer, ComparablePair< Long, ComparablePair< Long, Long > > > tilesCountToDimensions = new TreeMap<>();
		for ( final Entry< ComparablePair< Long, ComparablePair< Long, Long > >, List< TileInfo > > entry : dimensions.entrySet() )
			tilesCountToDimensions.put( entry.getValue().size(), entry.getKey() );

		System.out.println( "Tiles count to dimensions:" );
		for ( final Entry< Integer, ComparablePair< Long, ComparablePair< Long, Long > > > entry : tilesCountToDimensions.descendingMap().entrySet() )
			System.out.println( String.format( "  %s: %s",
					entry.getKey().toString(),
					Arrays.toString( new long[] { entry.getValue().getA(), entry.getValue().getB().getA(), entry.getValue().getB().getB() } ) ) );


		// build a map of tile coordinates to find duplicates
		final TreeMap< ComparablePair< Integer, ComparablePair< Integer, Integer > >, List< TileInfo > > coordinatesToTiles = new TreeMap<>();
		for ( final TileInfo tile : tiles )
		{
			final int[] coordinates = Utils.getTileCoordinates( tile );
			final ComparablePair< Integer, ComparablePair< Integer, Integer > > key = new ComparablePair<>( coordinates[ 0 ], new ComparablePair<>( coordinates[ 1 ], coordinates[ 2 ] ) );
			if ( !coordinatesToTiles.containsKey( key ) )
				coordinatesToTiles.put( key, new ArrayList<>() );
			coordinatesToTiles.get( key ).add( tile );
		}

		// build a helper structure for having the inverse mapping: tile -> dimensions
		final TreeMap< Integer, ComparablePair< Long, ComparablePair< Long, Long > > > tileToDimensions = new TreeMap<>();
		for ( final Entry< ComparablePair< Long, ComparablePair< Long, Long > >, List< TileInfo > > entry : dimensions.entrySet() )
			for ( final TileInfo tile : entry.getValue() )
				tileToDimensions.put( tile.getIndex(), entry.getKey() );

		// loop over the coordinate groups
		final Set< TileInfo > retainedTilesSet = new HashSet<>(), removedTilesSet = new HashSet<>();
		for ( final Entry< ComparablePair< Integer, ComparablePair< Integer, Integer > >, List< TileInfo > > entry : coordinatesToTiles.entrySet() )
		{
			// check if there are any duplicates for the particular stage position
			if ( entry.getValue().size() > 1 )
			{
				System.out.println( String.format( "%d tiles at %s",
						entry.getValue().size(),
						Arrays.toString( new int[] { entry.getKey().getA(), entry.getKey().getB().getA(), entry.getKey().getB().getB() } ) ) );

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

		ComparablePair< Long, ComparablePair< Long, Long > > tileDimensions = null;
		boolean sameDimensionsForAllTiles = true;
		for ( final TileInfo tile : retainedTiles )
		{
			final ComparablePair< Long, ComparablePair< Long, Long > > key = tileToDimensions.get( tile.getIndex() );
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
