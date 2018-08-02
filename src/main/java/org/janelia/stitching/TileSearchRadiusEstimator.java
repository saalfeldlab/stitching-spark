package org.janelia.stitching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.imglib2.Dimensions;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.util.Intervals;

public class TileSearchRadiusEstimator implements Serializable
{
	private static final long serialVersionUID = 3966655006478467424L;

	private static final double ESTIMATION_WINDOW_SIZE_TIMES = 3;

	private final Map< Integer, TileInfo > stageTilesMap, stitchedTilesMap;
	private final Map< Integer, double[] > tileOffsets;
	private final SearchRadiusEstimator estimator;
	private final double[] stitchedTilesOffset;

	public TileSearchRadiusEstimator( final TileInfo[] stageTiles, final TileInfo[] stitchedTiles )
	{
		this( stageTiles, stitchedTiles, null );
	}

	public TileSearchRadiusEstimator( final TileInfo[] stageTiles, final TileInfo[] stitchedTiles, final double[] estimationWindowSizeNullable )
	{
		stageTilesMap = Utils.createTilesMap( stageTiles );
		stitchedTilesMap = Utils.createTilesMap( stitchedTiles );

		stitchedTilesOffset = getStitchedTilesOffset( stageTilesMap, stitchedTilesMap );
		final double[] estimationWindowSize = ( estimationWindowSizeNullable == null ? getEstimationWindowSize( stageTiles[ 0 ].getSize() ) : estimationWindowSizeNullable );

		final Map< Integer, double[] > stageValues = new HashMap<>(), stitchedValues = new HashMap<>();
		for ( final Entry< Integer, TileInfo > stageEntry : stageTilesMap.entrySet() )
			stageValues.put( stageEntry.getKey(), stageEntry.getValue().getPosition().clone() );

		for ( final Entry< Integer, TileInfo > stitchedEntry : stitchedTilesMap.entrySet() )
			stitchedValues.put( stitchedEntry.getKey(), getOffsetCoordinates( stitchedEntry.getValue().getPosition(), stitchedTilesOffset ) );

		estimator = new SearchRadiusEstimator( stageValues, stitchedValues, estimationWindowSize );

		tileOffsets = new HashMap<>();
		for ( final Entry< Integer, double[] > stitchedEntry : stitchedValues.entrySet() )
		{
			final double[] stitchedPos = stitchedEntry.getValue(), stagePos = stageValues.get( stitchedEntry.getKey() );
			final double[] offset = new double[ stitchedPos.length ];
			for ( int d = 0; d < offset.length; ++d )
				offset[ d ] = stitchedPos[ d ] - stagePos[ d ];
			tileOffsets.put( stitchedEntry.getKey(), offset );
		}


		// print stage bounding box
		final Boundaries stageBoundingBox = TileOperations.getCollectionBoundaries( stageTiles );
		System.out.println( "Stage bounding box: min=" + Arrays.toString( Intervals.minAsIntArray( stageBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( stageBoundingBox ) ) );

		// print stage subset bounding box (only tiles that are contained in the stitched solution)
		final List< TileInfo > stageTilesSubset = new ArrayList<>();
		for ( final TileInfo stitchedTile : stitchedTiles )
			stageTilesSubset.add( stageTilesMap.get( stitchedTile.getIndex() ) );
		final Boundaries stageSubsetBoundingBox = TileOperations.getCollectionBoundaries( stageTilesSubset.toArray( new TileInfo[ 0 ] ) );
		System.out.println( "Stage subset bounding box: min=" + Arrays.toString( Intervals.minAsIntArray( stageSubsetBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( stageSubsetBoundingBox ) ) );
	}


	@Deprecated
	public SearchRadius getSearchRadiusWithinEstimationWindow( final TileInfo tile ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusWithinEstimationWindow( getStagePosition( tile ) );
	}
	@Deprecated
	public SearchRadius getSearchRadiusWithinInterval( final Interval neighborSearchInterval ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusWithinInterval( neighborSearchInterval );
	}

	public SearchRadius getSearchRadiusTreeWithinEstimationWindow( final TileInfo tile ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusTreeWithinEstimationWindow( getStagePosition( tile ) );
	}
	public SearchRadius getSearchRadiusTreeWithinInterval( final Interval neighborSearchInterval ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusTreeWithinInterval( neighborSearchInterval );
	}
	public SearchRadius getSearchRadiusTreeUsingKNearestNeighbors( final TileInfo tile, final int numNearestNeighbors ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusTreeUsingKNearestNeighbors( getStagePosition( tile ), numNearestNeighbors );
	}

	@Deprecated
	public ErrorEllipse getCombinedErrorEllipse( final SearchRadius fixedSearchRadius, final SearchRadius movingSearchRadius )
	{
		return estimator.getCombinedErrorEllipse( fixedSearchRadius, movingSearchRadius );
	}

	public SearchRadius getCombinedCovariancesSearchRadius( final SearchRadius fixedSearchRadius, final SearchRadius movingSearchRadius ) throws PipelineExecutionException
	{
		return estimator.getCombinedCovariancesSearchRadius( fixedSearchRadius, movingSearchRadius );
	}

	public double[] getStitchedTilesOffset()
	{
		return stitchedTilesOffset;
	}

	public Map< Integer, double[] > getTileOffsets()
	{
		return tileOffsets;
	}

	public int getNumPoints()
	{
		return tileOffsets.size();
	}

	public double[] getStagePosition( final TileInfo tile )
	{
		return getStagePosition( tile.getIndex() );
	}
	public double[] getStagePosition( final Integer tileIndex )
	{
		return stageTilesMap.get( tileIndex ).getPosition();
	}

	public Dimensions getEstimationWindowSize()
	{
		return Intervals.smallestContainingInterval(
				new FinalRealInterval(
						new double[ estimator.getEstimationWindowSize().length ],
						estimator.getEstimationWindowSize() ) );
	}

	private static double[] getEstimationWindowSize( final long[] tileSize )
	{
		final double[] estimationWindowSize = new double[ tileSize.length ];
		for ( int d = 0; d < tileSize.length; ++d )
			estimationWindowSize[ d ] = tileSize[ d ] * ESTIMATION_WINDOW_SIZE_TIMES;
		return estimationWindowSize;
	}

	/**
	 * Maps the earliest tile of the stitched set to the same tile in the stage set
	 *
	 * @param stageTilesMap
	 * @param stitchedTilesMap
	 * @return
	 */
	public static double[] getStitchedTilesOffset( final Map< Integer, TileInfo > stageTilesMap, final Map< Integer, TileInfo > stitchedTilesMap )
	{
		final TileInfo referenceStitchedTile = stitchedTilesMap.values().iterator().next();
		final TileInfo correspondingStageTile = stageTilesMap.get( referenceStitchedTile.getIndex() );
		final double[] stitchedTilesOffset = new double[ referenceStitchedTile.numDimensions() ];
		for ( int d = 0; d < stitchedTilesOffset.length; ++d )
			stitchedTilesOffset[ d ] = referenceStitchedTile.getPosition( d ) - correspondingStageTile.getPosition( d );
		return stitchedTilesOffset;
	}

	public static double[] getOffsetCoordinates( final double[] coords, final double[] offset )
	{
		final double[] offsetCoords = new double[ coords.length ];
		for ( int d = 0; d < coords.length; ++d )
			offsetCoords[ d ] = coords[ d ] - offset[ d ];
		return offsetCoords;
	}
}
