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
import org.janelia.util.ComparablePair;

/**
 * Evaluates the quality of pairwise matching by two different approaches:
 *
 * 1. Finds the highest cross correlation threshold value such that every tile is included in the resulting set.
 * The intuition would be that if we raise this threshold value a bit higher, at least one tile ends up uncovered by the resulting set
 * so it will not be passed to optimization procedure and will stay at its initial position.
 *
 * 2. Sorts the tiles by the highest cross correlation value of the pairwise shifts where they appear.
 *
 * For both approaches, low values indicate that the phase correlation has failed to identify good shifts for some of the tiles.
 * In this case you may want to:
 * 1. Increase the number of phase correlation peaks that should be investigated by the pairwise stitching algorithm.
 * 2. Preapply gaussian blur to tile images
 *
 * @author Igor Pisarev
 */

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
	}
}
