package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;
import org.janelia.util.ComparableTuple;
import org.janelia.util.Conversions;

import net.imglib2.util.IntervalsNullable;

public class StageOverlapsBetweenNeighboringTilesInGrid
{
	public static void main( final String[] args ) throws Exception
	{
		final TileInfo[] tiles = DataProviderFactory.createFSDataProvider().loadTiles( args[ 0 ] );

		final Map< ComparableTuple< Integer >, TileInfo > gridPositionsToTiles = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			gridPositionsToTiles.put( new ComparableTuple<>( Conversions.toBoxedArray( Utils.getTileCoordinates( tile ) ) ), tile );

		final List< TilePair > nonOverlappingTilePairs = new ArrayList<>();

		for ( final Entry< ComparableTuple< Integer >, TileInfo > gridPositionToTile: gridPositionsToTiles.entrySet() )
		{
			final TileInfo tile = gridPositionToTile.getValue();

			final int[] tileGridCoordinates = new int[ gridPositionToTile.getValue().numDimensions() ];
			for ( int d = 0; d < tileGridCoordinates.length; ++d )
				tileGridCoordinates[ d ] = gridPositionToTile.getKey().getValue( d );

			for ( int d = 0; d < gridPositionToTile.getValue().numDimensions(); ++d )
			{
				for ( final int s : new int[] { -1, 1 } )
				{
					final int[] neighboringGridCoordinates = tileGridCoordinates.clone();
					neighboringGridCoordinates[ d ] += s;
					final ComparableTuple< Integer > neighboringGridPosition = new ComparableTuple<>( Conversions.toBoxedArray( neighboringGridCoordinates ) );
					final TileInfo neighboringTile = gridPositionsToTiles.get( neighboringGridPosition );

					// skip neighboring cells that do not exist
					if ( neighboringTile == null )
						continue;

					// check overlaps for each pair only once by ordering their indices
					if ( tile.getIndex().intValue() < neighboringTile.getIndex().intValue() )
					{
						if ( IntervalsNullable.intersectReal( tile, neighboringTile ) == null )
							nonOverlappingTilePairs.add( new TilePair( tile, neighboringTile ) );
					}
				}
			}
		}

		if ( nonOverlappingTilePairs.isEmpty() )
		{
			System.out.println( "OK" );
		}
		else
		{
			System.out.println( "Non-overlapping neighboring pairs:" );
			for ( final TilePair tilePair : nonOverlappingTilePairs )
				System.out.println( "  " + tilePair );
		}
	}
}
