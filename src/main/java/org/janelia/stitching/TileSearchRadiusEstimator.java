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

/**
 * Wrapper for {@link SearchRadiusEstimator} that simplifies estimating a search radius for a tile.
 * Takes stage position of the given tile and delegates the call to the underlying {@link SearchRadiusEstimator}.
 */
public class TileSearchRadiusEstimator implements Serializable
{
	private static final long serialVersionUID = 3966655006478467424L;

	private final Map< Integer, TileInfo > stageTilesMap, stitchedTilesMap;
	private final Map< Integer, double[] > tileOffsets;
	private final SearchRadiusEstimator estimator;
	private final double[] stitchedTilesOffset;

	public TileSearchRadiusEstimator(
			final TileInfo[] stageTiles,
			final TileInfo[] stitchedTiles,
			final double searchRadiusMultiplier,
			final int[] statsWindowTileSize )
	{
		this(
				stageTiles,
				stitchedTiles,
				getEstimationWindowSize( stageTiles[ 0 ].getSize(), statsWindowTileSize ),
				searchRadiusMultiplier
			);
	}

	public TileSearchRadiusEstimator(
			final TileInfo[] stageTiles,
			final TileInfo[] stitchedTiles,
			final double[] estimationWindowSize,
			final double searchRadiusMultiplier )
	{
		stageTilesMap = Utils.createTilesMap( stageTiles );
		stitchedTilesMap = Utils.createTilesMap( stitchedTiles );

		final Map< Integer, double[] > stageValues = new HashMap<>(), stitchedValues = new HashMap<>();
		for ( final Entry< Integer, TileInfo > stageEntry : stageTilesMap.entrySet() )
			stageValues.put( stageEntry.getKey(), stageEntry.getValue().getPosition().clone() );

		for ( final Entry< Integer, TileInfo > stitchedEntry : stitchedTilesMap.entrySet() )
		{
			final int[] noSplitParts = new int[ stitchedEntry.getValue().numDimensions() ];
			Arrays.fill( noSplitParts, 1 );
			final SubdividedTileBox tileBox = SplitTileOperations.splitTilesIntoBoxes( new TileInfo[] { stitchedEntry.getValue() }, noSplitParts ).iterator().next();
			final Interval transformedTileBox = SplitTileOperations.transformTileBox( tileBox );
//			stitchedValues.put( stitchedEntry.getKey(), getOffsetCoordinates( stitchedEntry.getValue().getPosition(), stitchedTilesOffset ) );
			stitchedValues.put( stitchedEntry.getKey(), Intervals.minAsDoubleArray( transformedTileBox ) );
		}

		// find offset
		final Integer referenceTileIndex = getReferenceTileIndex( stageTilesMap, stitchedTilesMap );
		stitchedTilesOffset = new double[ stageTilesMap.get( referenceTileIndex ).numDimensions() ];
		for ( int d = 0; d < stitchedTilesOffset.length; ++d )
			stitchedTilesOffset[ d ] = stitchedValues.get( referenceTileIndex )[ d ] - stageValues.get( referenceTileIndex )[ d ];

		// apply offset
		for ( final Entry< Integer, double[] > stitchedEntry : stitchedValues.entrySet() )
			stitchedEntry.setValue( getOffsetCoordinates( stitchedEntry.getValue(), stitchedTilesOffset ) );

		estimator = new SearchRadiusEstimator( stageValues, stitchedValues, estimationWindowSize, searchRadiusMultiplier );

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
		return estimator.getSearchRadiusWithinEstimationWindow( getTileStagePosition( tile ) );
	}
	@Deprecated
	public SearchRadius getSearchRadiusWithinInterval( final Interval neighborSearchInterval ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusWithinInterval( neighborSearchInterval );
	}

	public SearchRadius getSearchRadiusTreeWithinEstimationWindow( final SubdividedTileBox tileBox ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusTreeWithinEstimationWindow( getTileBoxMiddlePointStagePosition( tileBox ) );
	}
	public SearchRadius getSearchRadiusTreeWithinEstimationWindow( final TileInfo tile ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusTreeWithinEstimationWindow( getTileStagePosition( tile ) );
	}

	public SearchRadius getSearchRadiusTreeWithinInterval( final Interval neighborSearchInterval ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusTreeWithinInterval( neighborSearchInterval );
	}

	public SearchRadius getSearchRadiusTreeUsingKNearestNeighbors( final SubdividedTileBox tileBox, final int numNearestNeighbors ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusTreeUsingKNearestNeighbors( getTileBoxMiddlePointStagePosition( tileBox ), numNearestNeighbors );
	}
	public SearchRadius getSearchRadiusTreeUsingKNearestNeighbors( final TileInfo tile, final int numNearestNeighbors ) throws PipelineExecutionException
	{
		return estimator.getSearchRadiusTreeUsingKNearestNeighbors( getTileStagePosition( tile ), numNearestNeighbors );
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

	public double[] getTileBoxMiddlePointStagePosition( final SubdividedTileBox tileBox )
	{
		final double[] tileStagePosition = getTileStagePosition( tileBox.getFullTile() );
		final double[] tileBoxMiddlePoint = SplitTileOperations.getTileBoxMiddlePoint( tileBox );
		final double[] tileBoxStagePosition = new double[ tileBox.numDimensions() ];
		for ( int d = 0; d < tileBoxStagePosition.length; ++d )
			tileBoxStagePosition[ d ] = tileStagePosition[ d ] + tileBoxMiddlePoint[ d ];
		return tileBoxStagePosition;
	}
	public double[] getTileStagePosition( final TileInfo tile )
	{
		return stageTilesMap.get( tile.getIndex() ).getPosition();
	}

	public Dimensions getEstimationWindowSize()
	{
		return Intervals.smallestContainingInterval(
				new FinalRealInterval(
						new double[ estimator.getEstimationWindowSize().length ],
						estimator.getEstimationWindowSize() ) );
	}

	private static double[] getEstimationWindowSize( final long[] tileSize, final int[] statsWindowTileSize )
	{
		final double[] estimationWindowSize = new double[ tileSize.length ];
		for ( int d = 0; d < tileSize.length; ++d )
			estimationWindowSize[ d ] = tileSize[ d ] * statsWindowTileSize[ d ];
		return estimationWindowSize;
	}

	/**
	 * Maps the earliest tile of the stitched set to the same tile in the stage set
	 *
	 * @param stageTilesMap
	 * @param stitchedTilesMap
	 * @return
	 */
	public static Integer getReferenceTileIndex( final Map< Integer, TileInfo > stageTilesMap, final Map< Integer, TileInfo > stitchedTilesMap )
	{
		long minTimestampStitched = Long.MAX_VALUE;
		TileInfo minTimestampStitchedTile = null;
		for ( final TileInfo stitchedTile : stitchedTilesMap.values() )
		{
			try
			{
				final long timestampStitched = Utils.getTileTimestamp( stitchedTile );
				if ( minTimestampStitched > timestampStitched )
				{
					minTimestampStitched = timestampStitched;
					minTimestampStitchedTile = stitchedTile;
				}
			}
			catch ( final Exception e )
			{
				System.out.println( "Cannot get timestamp:" );
				e.printStackTrace();
			}
		}

		return minTimestampStitchedTile.getIndex();
	}

	public static double[] getOffsetCoordinates( final double[] coords, final double[] offset )
	{
		final double[] offsetCoords = new double[ coords.length ];
		for ( int d = 0; d < coords.length; ++d )
			offsetCoords[ d ] = coords[ d ] - offset[ d ];
		return offsetCoords;
	}
}
