package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.util.ComparablePair;

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


	public static double findOptimalThreshold( final List< SerializablePairWiseStitchingResult > shifts )
	{
		shifts.sort( new CrossCorrelationComparatorDesc() );

		final TreeSet< Integer > uncoveredTiles = new TreeSet<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTilePair().toArray() )
				uncoveredTiles.add( tile.getIndex() );

		double threshold = 1.;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			for ( final TileInfo tile : shift.getTilePair().toArray() )
				uncoveredTiles.remove( tile.getIndex() );

			if ( uncoveredTiles.isEmpty() )
			{
				threshold = shift.getCrossCorrelation();
				break;
			}
		}

		return threshold;
	}


	public static TreeMap< Integer, SerializablePairWiseStitchingResult > getTilesHighestCorrelationWithPair( final List< SerializablePairWiseStitchingResult > shifts )
	{
		// shifts are already sorted
		final TreeMap< Integer, SerializablePairWiseStitchingResult > ret = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTilePair().toArray() )
				if ( !ret.containsKey( tile.getIndex() ) )
					ret.put( tile.getIndex(), shift );
		return ret;
	}

	private static void printTilesHighestCorrelationWithPair( final List< SerializablePairWiseStitchingResult > shifts )
	{
		final TreeMap< Integer, SerializablePairWiseStitchingResult > tilesCorr = getTilesHighestCorrelationWithPair( shifts );

		final TreeMap< Double, Integer > tilesCorrMap = new TreeMap<>();
		for ( final Entry< Integer, SerializablePairWiseStitchingResult > entry : tilesCorr.entrySet() )
			tilesCorrMap.put( ( double ) entry.getValue().getCrossCorrelation(), entry.getKey() );

		System.out.println( "-----------" );
		System.out.println( "Highest correlation value for every tile (with corresponding pair):" );
		for ( final Entry< Double, Integer > entry : tilesCorrMap.descendingMap().entrySet() )
			System.out.println( entry.getKey() + ": " + entry.getValue() + " (" + tilesCorr.get( entry.getValue() ).getTilePair().first().getIndex() + "," + tilesCorr.get( entry.getValue() ).getTilePair().second().getIndex() +")" );
		System.out.println( "-----------" );
	}




	public static TreeMap< Integer, Double > getTilesHighestCorrelation( final List< SerializablePairWiseStitchingResult > shifts )
	{
		// shifts are already sorted
		final TreeMap< Integer, Double > ret = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTilePair().toArray() )
				if ( !ret.containsKey( tile.getIndex() ) )
					ret.put( tile.getIndex(), ( double ) shift.getCrossCorrelation() );
		return ret;
	}

	private static void printTilesHighestCorrelation( final List< SerializablePairWiseStitchingResult > shifts )
	{
		final TreeMap< Integer, Double > tilesCorr = getTilesHighestCorrelation( shifts );

		final ArrayList< ComparablePair< Double, Integer > > tilesCorrList = new ArrayList<>();
		for ( final Entry< Integer, Double > entry : tilesCorr.entrySet() )
			tilesCorrList.add( new ComparablePair<>( entry.getValue(), entry.getKey() ) );

		Collections.sort( tilesCorrList );
		Collections.reverse( tilesCorrList );

		System.out.println( "-----------" );
		System.out.println( "Highest correlation value for every tile:" );
		for ( final ComparablePair< Double, Integer > pair : tilesCorrList )
			System.out.println( pair.first + ": " + pair.second );
		System.out.println( "-----------" );
	}





	public static void main( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args[ 0 ] );



		// -----------------------
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( !shift.getIsValidOverlap() )
				throw new Exception( "Invalid overlap" );

		double minCrossCorrelation = Double.MAX_VALUE;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			minCrossCorrelation = Math.min( shift.getCrossCorrelation(), minCrossCorrelation );
		System.out.println( "Min cross correlation = " + minCrossCorrelation );
		if ( minCrossCorrelation < 0 )
			throw new Exception( "Negative cross correlation" );
		// -----------------------




		final double threshold = findOptimalThreshold( shifts );
		System.out.println( "Optimal threshold value: " + threshold );

		printTilesHighestCorrelationWithPair( shifts );


		final ArrayList< SerializablePairWiseStitchingResult> nonOverlappingShifts = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			if ( shift.getCrossCorrelation() < threshold )
				continue;

			final TileInfo t1 = shift.getTilePair().first().clone();
			final TileInfo t2 = shift.getTilePair().second().clone();

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
