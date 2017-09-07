package org.janelia.stitching.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.stitching.PipelineExecutionException;
import org.janelia.stitching.SearchRadius;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.TileSearchRadiusEstimator;
import org.janelia.stitching.Utils;

public class FindPairwiseChanges
{
	private static final double SEARCH_RADIUS_MULTIPLIER = 3;

	public static void main( final String[] args ) throws IOException, PipelineExecutionException
	{
		final TileInfo[] stageTiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final TileInfo[] stitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 1 ] );

		final List< TilePair > adjacentPairs = FilterAdjacentShifts.filterAdjacentPairs( TileOperations.findOverlappingTiles( stageTiles ) );
		final List< TilePair > newAdjacentPairs = getPairsWithPrediction( stageTiles, stitchedTiles, 5, true );

		// find adjacent pairs that are formed between a tile from the stitched set and a tile from the missing set
		final Map< Integer, TileInfo > stitchedTilesMap = Utils.createTilesMap( stitchedTiles );
		final List< TilePair > stitchedAndMissingAdjacentPairs = new ArrayList<>();
		final Map< Integer, Map< Integer, TilePair > > adjacentPairsMap = new TreeMap<>();
		for ( final TilePair adjacentPair : adjacentPairs )
		{
			if ( stitchedTilesMap.containsKey( adjacentPair.getA().getIndex() ) != stitchedTilesMap.containsKey( adjacentPair.getB().getIndex() ) )
				stitchedAndMissingAdjacentPairs.add( adjacentPair );

			final int ind1 = Math.min( adjacentPair.getA().getIndex(), adjacentPair.getB().getIndex() );
			final int ind2 = Math.max( adjacentPair.getA().getIndex(), adjacentPair.getB().getIndex() );
			if ( !adjacentPairsMap.containsKey( ind1 ) )
				adjacentPairsMap.put( ind1, new TreeMap<>() );
			adjacentPairsMap.get( ind1 ).put( ind2, adjacentPair );
		}

		int removedPairsCount = adjacentPairs.size(), addedPairsCount = 0;
		for ( final TilePair newAdjacentPair : newAdjacentPairs )
		{
			final int ind1 = Math.min( newAdjacentPair.getA().getIndex(), newAdjacentPair.getB().getIndex() );
			final int ind2 = Math.max( newAdjacentPair.getA().getIndex(), newAdjacentPair.getB().getIndex() );
			if ( adjacentPairsMap.containsKey( ind1 ) && adjacentPairsMap.get( ind1 ).containsKey( ind2 ) )
				--removedPairsCount;
			else
				++addedPairsCount;
		}
		System.out.println( "Stage tiles: " + stageTiles.length + ", stitched tiles: " + stitchedTilesMap.size() );
		System.out.println( "old adjacent pairs: " + adjacentPairs.size() + ", new adjacent pairs: " + newAdjacentPairs.size() );
		System.out.println( "Added pairs: " + addedPairsCount );
		System.out.println( "Removed pairs: " + removedPairsCount );
	}


	public static List< TilePair > getPairsWithPrediction(
			final TileInfo[] stageTiles,
			final TileInfo[] stitchedTiles,
			final int minNumNeighbors,
			final boolean adjacentOnly ) throws PipelineExecutionException
	{
		final TileSearchRadiusEstimator predictor = new TileSearchRadiusEstimator( stageTiles, stitchedTiles, SEARCH_RADIUS_MULTIPLIER );
		final List< TileInfo > predictedTiles = new ArrayList<>();
		for ( final TileInfo stageTile : stageTiles )
		{
			final SearchRadius prediction = predictor.getSearchRadiusTreeWithinEstimationWindow( stageTile );
			if ( prediction.getUsedPointsIndexes().size() < minNumNeighbors )
				continue;

			final TileInfo predictedTile = stageTile.clone();
			// put the tile on its predicted location
			predictedTile.setPosition( prediction.getEllipseCenter().clone() );
			// shift it to the stitched space
			for ( int d = 0; d < predictedTile.numDimensions(); ++d )
				predictedTile.setPosition( d, predictedTile.getPosition( d ) + predictor.getStitchedTilesOffset()[ d ] );
			predictedTiles.add( predictedTile );
		}

		// filter new pairs in the predicted space
		final List< TilePair > predictedPairs = TileOperations.findOverlappingTiles( predictedTiles.toArray( new TileInfo[ 0 ] ) );

		// retain only adjacent pairs if requested
		if ( adjacentOnly )
		{
			final List< TilePair > predictedAdjacentPairs = FilterAdjacentShifts.filterAdjacentPairs( predictedPairs );
			predictedPairs.clear();
			predictedPairs.addAll( predictedAdjacentPairs );
		}

		// move them back to the stage space
		final List< TilePair > newStageAdjacentPairs = new ArrayList<>();
		final Map< Integer, TileInfo > stageTilesMap = Utils.createTilesMap( stageTiles );
		for ( final TilePair predictedPair : predictedPairs )
			newStageAdjacentPairs.add( new TilePair( stageTilesMap.get( predictedPair.getA().getIndex() ), stageTilesMap.get( predictedPair.getB().getIndex() ) ) );

		return newStageAdjacentPairs;
	}
}
