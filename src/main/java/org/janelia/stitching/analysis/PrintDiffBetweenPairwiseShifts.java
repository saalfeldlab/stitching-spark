package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.SubTile;
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

		for ( final List< SerializablePairWiseStitchingResult > pairwiseShiftList : pairwiseShifts )
			for ( final SerializablePairWiseStitchingResult pairwiseShift : pairwiseShiftList )
				if ( !pairwiseShift.getIsValidOverlap() )
					throw new RuntimeException( "contains invalid elements" );

		System.out.println( "Size of A: " + pairwiseShifts[ 0 ].size() );
		System.out.println( "Size of B: " + pairwiseShifts[ 1 ].size() );
		System.out.println();

		final Map< Integer, Map< Integer, SerializablePairWiseStitchingResult > > subTilePairwiseMap = new TreeMap<>();

		for ( final SerializablePairWiseStitchingResult shift : pairwiseShifts[ 0 ] )
		{
			final SubTile[] subTilePair = shift.getSubTilePair().toArray();
			final int ind1 = Math.min( subTilePair[ 0 ].getIndex(), subTilePair[ 1 ].getIndex() );
			final int ind2 = Math.max( subTilePair[ 0 ].getIndex(), subTilePair[ 1 ].getIndex() );

			if ( !subTilePairwiseMap.containsKey( ind1 ) )
				subTilePairwiseMap.put( ind1, new TreeMap<>() );
			subTilePairwiseMap.get( ind1 ).put( ind2, shift );
		}

		final List< SerializablePairWiseStitchingResult > missingInA = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : pairwiseShifts[ 1 ] )
		{
			final SubTile[] subTilePair = shift.getSubTilePair().toArray();
			final int ind1 = Math.min( subTilePair[ 0 ].getIndex(), subTilePair[ 1 ].getIndex() );
			final int ind2 = Math.max( subTilePair[ 0 ].getIndex(), subTilePair[ 1 ].getIndex() );

			if ( !subTilePairwiseMap.containsKey( ind1 ) || !subTilePairwiseMap.get( ind1 ).containsKey( ind2 ) )
			{
				missingInA.add( shift );
			}
			else
			{
				subTilePairwiseMap.get( ind1 ).get( ind2 ).setIsValidOverlap( false );
			}
		}
		System.out.println( "Missing in A:" );
		for ( final SerializablePairWiseStitchingResult pairwiseShift : missingInA )
			System.out.println( "  " + pairwiseShift.getSubTilePair() );
		System.out.println();

		System.out.println( "Missing in B:" );
		for ( final Map< Integer, SerializablePairWiseStitchingResult > values : subTilePairwiseMap.values() )
			for ( final SerializablePairWiseStitchingResult pairwiseShift : values.values() )
				if ( pairwiseShift.getIsValidOverlap() )
					System.out.println( "  " + pairwiseShift.getSubTilePair() );
	}
}
