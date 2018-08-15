package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;

public class InspectPairwiseMatchesBetweenColumns
{
	public static void main( final String[] args ) throws Exception
	{
		final String stitchedTilesConfiguration = args[ 0 ], usedPairwiseConfiguration = args[ 1 ];
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final TileInfo[] stitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( stitchedTilesConfiguration ) ) );
		final List< SerializablePairWiseStitchingResult > usedPairwiseMatches = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( usedPairwiseConfiguration ) ) );

		final Map< Long, Set< TileInfo > > columnTileGroups = new TreeMap<>();
		for ( final TileInfo tile : stitchedTiles )
		{
			final long columnPos = getColumnPosition( tile );
			if ( !columnTileGroups.containsKey( columnPos ) )
				columnTileGroups.put( columnPos, new HashSet<>() );
			columnTileGroups.get( columnPos ).add( tile );
		}

		final Map< Long, Map< Long, Integer > > columnMatchesCountMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult pairwiseMatch : usedPairwiseMatches )
		{
			if ( !pairwiseMatch.getIsValidOverlap() )
				throw new RuntimeException();

			final TileInfo[] fullTilePair = pairwiseMatch.getSubTilePair().getFullTilePair().toArray();
			final long columnPosFirst = getColumnPosition( fullTilePair[ 0 ] ), columnPosSecond = getColumnPosition( fullTilePair[ 1 ] );

			if ( !columnMatchesCountMap.containsKey( columnPosFirst ) )
				columnMatchesCountMap.put( columnPosFirst, new TreeMap<>() );

			final int currentMatchesCount = columnMatchesCountMap.get( columnPosFirst ).getOrDefault( columnPosSecond, 0 );
			columnMatchesCountMap.get( columnPosFirst ).put( columnPosSecond, currentMatchesCount + 1 );
		}

		final Map< Long, Integer > columnPosToIndexMap = new TreeMap<>();
		for ( final Long columnPos : columnTileGroups.keySet() )
			columnPosToIndexMap.put( columnPos, columnPosToIndexMap.size() + 1 );

		System.out.println( "Total number of used pairwise matches: " + usedPairwiseMatches.size() );
		System.out.println( "Total number of columns: " + columnTileGroups.size() );

		System.out.println( System.lineSeparator() + "Inner column matches:" );
		for ( final Entry< Long, Map< Long, Integer > > columnToColumnsEntry : columnMatchesCountMap.entrySet() )
			System.out.println( "  " + columnPosToIndexMap.get( columnToColumnsEntry.getKey() ) + ": " + columnToColumnsEntry.getValue().get( columnToColumnsEntry.getKey() ) );

		System.out.println( System.lineSeparator() + "Outer column matches:" );
		for ( final Entry< Long, Map< Long, Integer > > columnToColumnsEntry : columnMatchesCountMap.entrySet() )
			for ( final Entry< Long, Integer > columnToCountEntry : columnToColumnsEntry.getValue().entrySet() )
				if ( !columnToCountEntry.getKey().equals( columnToColumnsEntry.getKey() ) )
					System.out.println( "  " + columnPosToIndexMap.get( columnToColumnsEntry.getKey() ) + "->" + columnPosToIndexMap.get( columnToCountEntry.getKey() ) + ": " + columnToCountEntry.getValue() );
	}

	private static long getColumnPosition( final TileInfo tile )
	{
		return Math.round( tile.getStagePosition( 0 ) );
	}
}
