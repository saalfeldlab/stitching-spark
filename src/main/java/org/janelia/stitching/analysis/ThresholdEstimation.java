package org.janelia.stitching.analysis;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;

import net.imglib2.util.ComparablePair;

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

@Deprecated
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


	@Deprecated
	public static double findOptimalThreshold( final List< SerializablePairWiseStitchingResult > shifts )
	{
		shifts.sort( new CrossCorrelationComparatorDesc() );

		final TreeSet< Integer > uncoveredTiles = new TreeSet<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( shift.getIsValidOverlap() )
				for ( final TileInfo tile : shift.getTileBoxPair().getOriginalTilePair().toArray() )
					uncoveredTiles.add( tile.getIndex() );

		double threshold = 1.;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			if ( !shift.getIsValidOverlap() )
				continue;

			for ( final TileInfo tile : shift.getTileBoxPair().getOriginalTilePair().toArray() )
				uncoveredTiles.remove( tile.getIndex() );

			if ( uncoveredTiles.isEmpty() )
			{
				threshold = shift.getCrossCorrelation();
				break;
			}
		}

		return threshold;
	}


	@Deprecated
	public static ComparablePair< Double, Integer > findOptimalThresholdWithIndex( final List< SerializablePairWiseStitchingResult > shifts )
	{
		shifts.sort( new CrossCorrelationComparatorDesc() );

		final TreeSet< Integer > uncoveredTiles = new TreeSet<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTileBoxPair().getOriginalTilePair().toArray() )
				uncoveredTiles.add( tile.getIndex() );

		double threshold = 1.;
		int index = shifts.size();
		for ( int i = 0; i < shifts.size(); i++ )
		{
			for ( final TileInfo tile : shifts.get( i ).getTileBoxPair().getOriginalTilePair().toArray() )
				uncoveredTiles.remove( tile.getIndex() );

			if ( uncoveredTiles.isEmpty() )
			{
				threshold = shifts.get( i ).getCrossCorrelation();
				index = i;
				break;
			}
		}

		try
		{
			final PrintWriter writer = new PrintWriter("pairwise_correlations(threshold=" + threshold + ".txt", "UTF-8");
			for ( final SerializablePairWiseStitchingResult shift : shifts )
				writer.println( shift.getCrossCorrelation() );
			writer.close();
		}
		catch ( FileNotFoundException | UnsupportedEncodingException e )
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



		return new ComparablePair< >( threshold, index );
	}


	@Deprecated
	public static TreeMap< Integer, SerializablePairWiseStitchingResult > getTilesHighestCorrelationWithPair( final List< SerializablePairWiseStitchingResult > shifts )
	{
		// shifts are already sorted
		final TreeMap< Integer, SerializablePairWiseStitchingResult > ret = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTileBoxPair().getOriginalTilePair().toArray() )
				if ( !ret.containsKey( tile.getIndex() ) )
					ret.put( tile.getIndex(), shift );
		return ret;
	}

	private static void printTilesHighestCorrelationWithPair( final List< SerializablePairWiseStitchingResult > shifts )
	{
		final TreeMap< Integer, SerializablePairWiseStitchingResult > tilesCorr = getTilesHighestCorrelationWithPair( shifts );
		final List< ComparablePair< Double, Integer > > tilesCorrSorted = new ArrayList<>();
		for ( final Entry< Integer, SerializablePairWiseStitchingResult > entry : tilesCorr.entrySet() )
			tilesCorrSorted.add( new ComparablePair<>( ( double ) entry.getValue().getCrossCorrelation(), entry.getKey() ) );

		Collections.sort( tilesCorrSorted );
		Collections.reverse( tilesCorrSorted );

		/*System.out.println( "-----------" );
		System.out.println( "Highest correlation value for every tile (with corresponding pair):" );
		for ( final ComparablePair< Double, Integer > entry : tilesCorrSorted )
			System.out.println( entry.a + ": " + entry.b + " (" + tilesCorr.get( entry.b ).getTilePair().getA().getIndex() + "," + tilesCorr.get( entry.b ).getTilePair().getB().getIndex() +")" );
		System.out.println( "-----------" );*/



		try
		{
			final PrintWriter writer = new PrintWriter("highest_tile_correlation.txt", "UTF-8");
			for ( final Entry< Integer, SerializablePairWiseStitchingResult > entry : tilesCorr.entrySet() )
				writer.println( entry.getKey() + " " + entry.getValue().getCrossCorrelation() );
			writer.close();
		}
		catch ( FileNotFoundException | UnsupportedEncodingException e )
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



		/*final Map< Integer, TileInfo > tilesMap = new HashMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTilePair().toArray() )
				tilesMap.put( tile.getIndex(), tile );
		int badShifts = 0;
		for ( final Entry< Integer, SerializablePairWiseStitchingResult > entry : tilesCorr.entrySet() )
		{
			if ( entry.getValue().getCrossCorrelation() < 0.85 )
			{
				badShifts++;
				final String before = tilesMap.containsKey( entry.getKey()-1 ) ? "before="+Paths.get( tilesMap.get( entry.getKey()-1 ).getFilePath() ).getFileName() : "";
				final String after = tilesMap.containsKey( entry.getKey()+1 ) ? "after="+Paths.get( tilesMap.get( entry.getKey()+1 ).getFilePath() ).getFileName() : "";
				final String curr = "CURR="+Paths.get( tilesMap.get( entry.getKey() ).getFilePath() ).getFileName();
				System.out.println( entry.getKey() + ":   " + before + "    " + curr + "    "+ after );
			}
		}
		System.out.println( "BAD SHIFTS = " + badShifts );*/
	}




	@Deprecated
	public static TreeMap< Integer, Double > getTilesHighestCorrelation( final List< SerializablePairWiseStitchingResult > shifts )
	{
		// shifts are already sorted
		final TreeMap< Integer, Double > ret = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTileBoxPair().getOriginalTilePair().toArray() )
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
			System.out.println( pair.a + ": " + pair.b );
		System.out.println( "-----------" );
	}





	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );

		final List< SerializablePairWiseStitchingResult > validShifts = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( shift.getIsValidOverlap() )
				validShifts.add( shift );
		System.out.println( "There are " + validShifts.size() + " valid shifts" );

		// use only valid ones
		shifts.clear();
		shifts.addAll( validShifts );

		double minCrossCorrelation = Double.MAX_VALUE;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			minCrossCorrelation = Math.min( shift.getCrossCorrelation(), minCrossCorrelation );
		System.out.println( "Min cross correlation = " + minCrossCorrelation );
		if ( minCrossCorrelation < 0 )
			throw new Exception( "Negative cross correlation" );

		final ComparablePair< Double, Integer > thresholdWithIndex = findOptimalThresholdWithIndex( shifts );
		System.out.println( "Optimal threshold value: " + thresholdWithIndex.a );
		System.out.println( "Covers " + thresholdWithIndex.b + " pairs out of " + shifts.size() );

		printTilesHighestCorrelationWithPair( shifts );
	}
}
