package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;

/**
 * Evaluates the quality of pairwise matching by finding incorrect shifts.
 *
 * If some of the pairs end up being non-overlapped, it indicates that the phase correlation has failed to identify good shifts for some of the tiles.
 * In this case you may want to increase the number of phase correlation peaks that should be investigated by the pairwise stitching algorithm.
 *
 * @author Igor Pisarev
 */

public class NonOverlappingShifts
{
	public static void main( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args[ 0 ] );

		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( !shift.getIsValidOverlap() )
				throw new Exception( "Invalid overlap" );

		double cross = 0;
		final List< SerializablePairWiseStitchingResult> badShifts = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			final TileInfo t1 = shift.getTilePair().first().clone();
			final TileInfo t2 = shift.getTilePair().second().clone();

			if ( TileOperations.getOverlappingRegion( t1, t2 ) == null )
				throw new Exception( "impossible" );

			for ( int d = 0; d < shift.getNumDimensions(); d++ )
				t2.setPosition( d, t1.getPosition( d ) + shift.getOffset( d ) );

			if ( TileOperations.getOverlappingRegion( t1, t2 ) == null)
			{
				badShifts.add( shift );
				cross += shift.getCrossCorrelation();
			}
		}

		System.out.println( "There are " + badShifts.size() + " pairs out of " + shifts.size() + " which end up being non-overlapped" );
		System.out.println( "Total cross corr of them=" + cross);
		System.out.println( "---------------" );

		final TreeMap< Integer, Integer > tilesToMistakes = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult badShift : badShifts )
		{
			for ( final TileInfo tile : badShift.getTilePair().toArray() )
			{
				if ( !tilesToMistakes.containsKey( tile.getIndex() ) )
					tilesToMistakes.put( tile.getIndex(), 0 );
				tilesToMistakes.put( tile.getIndex(), tilesToMistakes.get( tile.getIndex() ) + 1 );
			}
		}
		System.out.println( "tilesToMistakes size=" + tilesToMistakes.size() );
		final TreeMap< Integer, Integer > mistakesToCount = new TreeMap<>();
		for ( final Entry< Integer, Integer > entry : tilesToMistakes.entrySet() )
		{
			final int mistakesForTile = entry.getValue();
			if ( !mistakesToCount.containsKey( mistakesForTile ) )
				mistakesToCount.put( mistakesForTile, 0 );
			mistakesToCount.put( mistakesForTile, mistakesToCount.get( mistakesForTile ) + 1 );
		}
		System.out.println( "mistakesToCount = " + mistakesToCount.descendingMap() );
	}
}
