package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;

/**
 * Evaluates the quality of pairwise matching by finding incorrect shifts.
 *
 * If some of the pairs end up being non-overlapped, it indicates that the phase correlation has failed to identify good shifts for some of the tiles.
 * In this case you may want to:
 * 1. Increase the number of phase correlation peaks that should be investigated by the pairwise stitching algorithm.
 * 2. Preapply gaussian blur to tile images
 *
 * @author Igor Pisarev
 */

@Deprecated
public class NonOverlappingShifts
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );

		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( !shift.getIsValidOverlap() )
				throw new Exception( "Invalid overlap" );

		double cross = 0;
		final List< SerializablePairWiseStitchingResult> badShifts = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			final TileInfo t1 = shift.getTileBoxPair().getOriginalTilePair().getA().clone();
			final TileInfo t2 = shift.getTileBoxPair().getOriginalTilePair().getB().clone();

			if ( !TileOperations.overlap( t1, t2 ) )
				throw new Exception( "impossible" );

			for ( int d = 0; d < shift.getNumDimensions(); d++ )
				t2.setPosition( d, t1.getPosition( d ) + shift.getOffset( d ) );

			if ( !TileOperations.overlap( t1, t2 ) )
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
			for ( final TileInfo tile : badShift.getTileBoxPair().getOriginalTilePair().toArray() )
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
