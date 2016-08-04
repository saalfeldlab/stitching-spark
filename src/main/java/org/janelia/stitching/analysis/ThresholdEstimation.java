package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;

public class ThresholdEstimation
{

	static class CrossCorrelationComparatorDesc implements Comparator< SerializablePairWiseStitchingResult >
	{
		@Override
		public int compare( final SerializablePairWiseStitchingResult s1, final SerializablePairWiseStitchingResult s2 )
		{
			return -Double.compare( s1.getCrossCorrelation(), s2.getCrossCorrelation() );
		}
	}

	public static void main( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args[ 0 ] );

		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( !shift.getIsValidOverlap() )
				throw new Exception( "Invalid overlap" );

		shifts.sort( new CrossCorrelationComparatorDesc() );

		final TreeSet< Integer > uncoveredTiles = new TreeSet<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : new TileInfo[] { shift.getPairOfTiles()._1, shift.getPairOfTiles()._2 } )
				uncoveredTiles.add( tile.getIndex() );

		double threshold = 1.;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			for ( final TileInfo tile : new TileInfo[] { shift.getPairOfTiles()._1, shift.getPairOfTiles()._2 } )
				uncoveredTiles.remove( tile.getIndex() );

			if ( uncoveredTiles.isEmpty() )
			{
				threshold = shift.getCrossCorrelation();
				break;
			}
		}

		System.out.println( "Optimal threshold value: " + threshold );

		System.out.println( "-----------" );
		final ArrayList< SerializablePairWiseStitchingResult> nonOverlappingShifts = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			if ( shift.getCrossCorrelation() < threshold )
				continue;

			final TileInfo t1 = shift.getPairOfTiles()._1.clone();
			final TileInfo t2 = shift.getPairOfTiles()._2.clone();

			if ( TileOperations.getOverlappingRegion( t1, t2 ) == null )
				throw new Exception( "impossible" );

			for ( int d = 0; d < shift.getNumDimensions(); d++ )
				t2.setPosition( d, t1.getPosition( d ) + shift.getOffset( d ) );

			if ( TileOperations.getOverlappingRegion( t1, t2 ) == null)
				nonOverlappingShifts.add( shift );
		}
		System.out.println( "There are " + nonOverlappingShifts.size() + " non-overlapping shifts after thresholding" );
	}




	/*public static void mainTest( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args[ 0 ] );

		final double threshold = 0.1;


		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( !shift.getIsValidOverlap() )
				throw new Exception( "Invalid overlap" );


		final TreeMap< Integer, TileInfo > allTiles = new TreeMap<>(), goodTiles = new TreeMap<>();
		int badShifts = 0;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			final boolean isBadShift = ( shift.getCrossCorrelation() < threshold );
			if (isBadShift )
				badShifts++;

			for ( final TileInfo tile : new TileInfo[] { shift.getPairOfTiles()._1, shift.getPairOfTiles()._2 } )
			{
				allTiles.put( tile.getIndex(), tile );
				if ( !isBadShift)
					goodTiles.put( tile.getIndex(), tile );
			}
		}

		System.out.println( "Covered " + goodTiles.size() + " tiles out of " + allTiles.size() );

		System.out.println( "There are " + badShifts + " bad pairs out of " + shifts.size() );

		final ArrayList< Integer > badTiles = new ArrayList<>();
		for ( final Integer tileIndex : allTiles.keySet() )
			if ( !goodTiles.containsKey( tileIndex ) )
				badTiles.add( tileIndex );
		System.out.println( "There are " + badTiles.size() + " bad tiles: " + badTiles );

		//		for ( final Integer tileIndex : badTiles )
		//			System.out.println( tileIndex + ": " + allTiles.get( tileIndex ).getFile() );





		System.out.println( "-----------" );
		final ArrayList< SerializablePairWiseStitchingResult> nonOverlappingShifts = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			if ( shift.getCrossCorrelation() < threshold )
				continue;

			final TileInfo t1 = shift.getPairOfTiles()._1.clone();
			final TileInfo t2 = shift.getPairOfTiles()._2.clone();

			if ( TileOperations.getOverlappingRegion( t1, t2 ) == null )
				throw new Exception( "impossible" );

			for ( int d = 0; d < shift.getNumDimensions(); d++ )
				t2.setPosition( d, t1.getPosition( d ) + shift.getOffset( d ) );

			if ( TileOperations.getOverlappingRegion( t1, t2 ) == null)
				nonOverlappingShifts.add( shift );
		}
		System.out.println( "There are " + nonOverlappingShifts.size() + " non-overlapping shifts after thresholding" );
	}*/
}
