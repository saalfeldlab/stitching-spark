package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;

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
			final TileInfo t1 = shift.getPairOfTiles()._1.clone();
			final TileInfo t2 = shift.getPairOfTiles()._2.clone();

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
			for ( final TileInfo tile : new TileInfo[] { badShift.getPairOfTiles()._1, badShift.getPairOfTiles()._2 } )
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
		System.out.println( "---------------" );



		// Remove tiles that lead to these mistakes
		/*final String initialConfig = Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args[0], "_pairwise" ), "_full" );
		final TileInfo[] initialTiles = TileInfoJSONProvider.loadTilesConfiguration( initialConfig );
		final ArrayList< TileInfo > newTiles = new ArrayList<>();
		for ( final TileInfo tile : initialTiles )
			if ( !tilesToMistakes.containsKey( tile.getIndex() ) )
				newTiles.add( tile );
		TileInfoJSONProvider.saveTilesConfiguration( newTiles.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args[0], "_pairwise" ), "_reduced" ) );*/
	}
}
