package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;

public class PrintDiffBetweenPairwiseShifts
{
	public static void main( final String[] args ) throws Exception
	{
		final String[] pairwisePath = new String[] { args[ 0 ], args[ 1 ] };
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final List< SerializablePairWiseStitchingResult >[] pairwiseShifts = new List[ 2 ];
		for ( int i = 0; i < 2; ++i )
			pairwiseShifts[ i ] = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwisePath[ i ] ) ) );

		final Map< Integer, Map< Integer, Boolean > > fullTilePairwiseMap = new TreeMap<>();

		for ( final SerializablePairWiseStitchingResult shift : pairwiseShifts[ 0 ] )
		{
			final TileInfo[] tilePair = shift.getSubTilePair().getFullTilePair().toArray();
			final int ind1 = Math.min( tilePair[ 0 ].getIndex(), tilePair[ 1 ].getIndex() );
			final int ind2 = Math.max( tilePair[ 0 ].getIndex(), tilePair[ 1 ].getIndex() );

			if ( !fullTilePairwiseMap.containsKey( ind1 ) )
				fullTilePairwiseMap.put( ind1, new TreeMap<>() );
			fullTilePairwiseMap.get( ind1 ).put( ind2, false );
		}

		final Map< Integer, Integer > missingInA = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : pairwiseShifts[ 1 ] )
		{
			final TileInfo[] tilePair = shift.getSubTilePair().getFullTilePair().toArray();
			final int ind1 = Math.min( tilePair[ 0 ].getIndex(), tilePair[ 1 ].getIndex() );
			final int ind2 = Math.max( tilePair[ 0 ].getIndex(), tilePair[ 1 ].getIndex() );

			if ( !fullTilePairwiseMap.containsKey( ind1 ) || !fullTilePairwiseMap.get( ind1 ).containsKey( ind2 ) )
				missingInA.put( ind1, ind2 );
			else
				fullTilePairwiseMap.get( ind1 ).put( ind2, true );
		}
		System.out.println( "Missing in A:" );
		for ( final Entry< Integer, Integer > entry : missingInA.entrySet() )
			System.out.println( " (" + entry.getKey() + "," + entry.getValue() + ")" );
		System.out.println();

		System.out.println( "Missing in B:" );
		for ( final Entry< Integer, Map< Integer, Boolean > > entry : fullTilePairwiseMap.entrySet() )
			for ( final Entry< Integer, Boolean > entryForKey : entry.getValue().entrySet() )
				if ( !entryForKey.getValue() )
					System.out.println( " (" + entry.getKey() + "," + entryForKey.getKey() + ")" );
	}
}
