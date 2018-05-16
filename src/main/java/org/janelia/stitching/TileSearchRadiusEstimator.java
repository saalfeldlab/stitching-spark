package org.janelia.stitching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.KDTree;
import net.imglib2.RealLocalizable;
import net.imglib2.RealPoint;
import net.imglib2.neighborsearch.IntervalNeighborSearchOnKDTree;
import net.imglib2.neighborsearch.KNearestNeighborSearchOnKDTree;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

/**
 * Wrapper for {@link SearchRadiusEstimator} that simplifies estimating a search radius for a tile.
 * Takes stage position of the given tile and delegates the call to the underlying {@link SearchRadiusEstimator}.
 */
public class TileSearchRadiusEstimator implements Serializable
{
	public static class EstimatedTileBoxSearchRadius
	{
		public final ErrorEllipse errorEllipse;
		public final SubdividedTileBox tileBox;
		public final Set< TileInfo > neighboringTiles;

		public EstimatedTileBoxSearchRadius(
				final ErrorEllipse errorEllipse,
				final SubdividedTileBox tileBox,
				final Set< TileInfo > neighboringTiles )
		{
			this.errorEllipse = errorEllipse;
			this.tileBox = tileBox;
			this.neighboringTiles = neighboringTiles;
		}
	}

	private static final long serialVersionUID = 3966655006478467424L;

	final double[] estimationWindowSize;
	final double searchRadiusMultiplier;

	final double[] worldOffset;

	private final KDTree< TileInfo > kdTree;

	public TileSearchRadiusEstimator(
			final TileInfo[] tiles,
			final double searchRadiusMultiplier,
			final int[] statsWindowTileSize )
	{
		this(
				tiles,
				getEstimationWindowSize( tiles[ 0 ].getSize(), statsWindowTileSize ),
				searchRadiusMultiplier
			);
	}

	public TileSearchRadiusEstimator(
			final TileInfo[] tiles,
			final double[] estimationWindowSize,
			final double searchRadiusMultiplier )
	{
		this.estimationWindowSize = estimationWindowSize;
		this.searchRadiusMultiplier = searchRadiusMultiplier;

		final List< TileInfo > tilesWithStitchedTransform = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( tile.getTransform() != null )
				tilesWithStitchedTransform.add( tile );

		final List< RealLocalizable > stageSubsetPositions = new ArrayList<>();
		for ( final TileInfo tileWithStitchedTransform : tilesWithStitchedTransform )
			stageSubsetPositions.add( new RealPoint( tileWithStitchedTransform.getPosition() ) );

		// calculate world offset as an average difference between world and stage coordinates
		worldOffset = getWorldOffset( tilesWithStitchedTransform );

		// build a search tree to be able to look up stage positions and corresponding stitched positions
		// in the neighborhood for any given stage position
		kdTree = new KDTree<>( tilesWithStitchedTransform, stageSubsetPositions );
	}

	public EstimatedTileBoxSearchRadius estimateSearchRadiusWithinWindow( final SubdividedTileBox tileBox ) throws PipelineExecutionException
	{
		return estimateSearchRadiusWithinWindow( tileBox, getEstimationWindow( tileBox ) );
	}

	public EstimatedTileBoxSearchRadius estimateSearchRadiusWithinWindow( final SubdividedTileBox tileBox, final Interval estimationWindow ) throws PipelineExecutionException
	{
		final IntervalNeighborSearchOnKDTree< TileInfo > intervalSearch = new IntervalNeighborSearchOnKDTree<>( kdTree );
		final Set< TileInfo > neighboringTiles = new HashSet<>( intervalSearch.search( estimationWindow ) );
		return estimateSearchRadius( tileBox, neighboringTiles );
	}

	public EstimatedTileBoxSearchRadius estimateSearchRadiusKNearestNeighbors( final SubdividedTileBox tileBox, final int numNearestNeighbors ) throws PipelineExecutionException
	{
		final KNearestNeighborSearchOnKDTree< TileInfo > neighborsSearch = new KNearestNeighborSearchOnKDTree<>( kdTree, numNearestNeighbors );
		final double[] tileBoxMiddlePointStagePosition = SplitTileOperations.getTileBoxMiddlePointStagePosition( tileBox );
		neighborsSearch.search( new RealPoint( tileBoxMiddlePointStagePosition ) );
		final Set< TileInfo > neighboringTiles = new HashSet<>();
		for ( int i = 0; i < neighborsSearch.getK(); ++i )
			neighboringTiles.add( neighborsSearch.getSampler( i ).get() );
		return estimateSearchRadius( tileBox, neighboringTiles );
	}

	private EstimatedTileBoxSearchRadius estimateSearchRadius( final SubdividedTileBox tileBox, final Set< TileInfo > neighboringTiles ) throws PipelineExecutionException
	{
		final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates = getStageAndWorldCoordinates( tileBox, neighboringTiles, worldOffset );

		final double[] meanValues = new double[ tileBox.numDimensions() ];
		for ( int d = 0; d < meanValues.length; ++d )
		{
			double dOffsetSum = 0;
			for ( final Pair< RealPoint, RealPoint > stageAndWorld : stageAndWorldCoordinates )
				dOffsetSum += stageAndWorld.getB().getDoublePosition( d ) - stageAndWorld.getA().getDoublePosition( d );

			meanValues[ d ] = dOffsetSum / stageAndWorldCoordinates.size();
		}

		final double[][] covarianceMatrix = new double[ meanValues.length ][ meanValues.length ];
		for ( int dRow = 0; dRow < covarianceMatrix.length; ++dRow )
		{
			for ( int dCol = dRow; dCol < covarianceMatrix[ dRow ].length; ++dCol )
			{
				double dRowColOffsetSumProduct = 0;
				for ( final Pair< RealPoint, RealPoint > stageAndWorld : stageAndWorldCoordinates )
				{
					final double dRowOffset = stageAndWorld.getB().getDoublePosition( dRow ) - stageAndWorld.getA().getDoublePosition( dRow );
					final double dColOffset = stageAndWorld.getB().getDoublePosition( dCol ) - stageAndWorld.getA().getDoublePosition( dCol );
					dRowColOffsetSumProduct += dRowOffset * dColOffset;
				}
				final double covariance = dRowColOffsetSumProduct / stageAndWorldCoordinates.size() - meanValues[ dRow ] * meanValues[ dCol ];
				covarianceMatrix[ dRow ][ dCol ] = covarianceMatrix[ dCol ][ dRow ]  = covariance;
			}
		}

		final ErrorEllipse searchRadius = new ErrorEllipse( searchRadiusMultiplier, meanValues, covarianceMatrix );
		return new EstimatedTileBoxSearchRadius( searchRadius, tileBox, neighboringTiles );
	}

	static double[] getWorldOffset( final Collection< TileInfo > tilesWithStitchedTransform )
	{
		final int dim = tilesWithStitchedTransform.iterator().next().numDimensions();
		final double[] worldOffset = new double[ dim ];

		// helper for transforming the middle point of the tile instead of the min point
		final int[] noSplitParts = new int[ dim ];
		Arrays.fill( noSplitParts, 1 );
		final List< SubdividedTileBox > fullTileBoxes = SplitTileOperations.splitTilesIntoBoxes( tilesWithStitchedTransform.toArray( new TileInfo[ 0 ] ), noSplitParts );

		for ( final SubdividedTileBox fullTileBox : fullTileBoxes )
		{
			final double[] tileMiddlePointStagePosition = SplitTileOperations.getTileBoxMiddlePointStagePosition( fullTileBox );
			final double[] tileMiddlePointWorldPosition = SplitTileOperations.transformTileBoxMiddlePoint( fullTileBox );
			for ( int d = 0; d < dim; ++d )
				worldOffset[ d ] += tileMiddlePointWorldPosition[ d ] - tileMiddlePointStagePosition[ d ];
		}

		for ( int d = 0; d < dim; ++d )
			worldOffset[ d ] /= tilesWithStitchedTransform.size();

		return worldOffset;
	}

	static double[] getEstimationWindowSize( final long[] tileSize, final int[] statsWindowTileSize )
	{
		final double[] estimationWindowSize = new double[ tileSize.length ];
		for ( int d = 0; d < tileSize.length; ++d )
			estimationWindowSize[ d ] = tileSize[ d ] * statsWindowTileSize[ d ];
		return estimationWindowSize;
	}

	private static List< Pair< RealPoint, RealPoint > > getStageAndWorldCoordinates(
			final SubdividedTileBox tileBox,
			final Set< TileInfo > neighboringTiles,
			final double[] worldOffset )
	{
		final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates = new ArrayList<>();
		for ( final TileInfo neighboringTile : neighboringTiles )
		{
			final int[] splitParts = new int[ neighboringTile.numDimensions() ];
			Arrays.fill( splitParts, 2 );
			final List< SubdividedTileBox > neighboringTileBoxes = SplitTileOperations.splitTilesIntoBoxes( new TileInfo[] { neighboringTile }, splitParts );
			for ( final SubdividedTileBox neighboringTileBox : neighboringTileBoxes )
			{
				if ( tileBox.getTag().equals( neighboringTileBox.getTag() ) )
				{
					final double[] neighboringTileBoxMiddlePointStagePosition = SplitTileOperations.getTileBoxMiddlePointStagePosition( neighboringTileBox );
					final double[] neighboringTileBoxMiddlePointWorldPosition = SplitTileOperations.transformTileBoxMiddlePoint( neighboringTileBox );

					for ( int d = 0; d < worldOffset.length; ++d )
						neighboringTileBoxMiddlePointWorldPosition[ d ] -= worldOffset[ d ];

					stageAndWorldCoordinates.add( new ValuePair<>(
							new RealPoint( neighboringTileBoxMiddlePointStagePosition ),
							new RealPoint( neighboringTileBoxMiddlePointWorldPosition )
						) );
				}
			}
		}
		return stageAndWorldCoordinates;
	}

	/**
	 * When estimating a pairwise shift vector between a pair of tiles, both of them have
	 * an expected offset and a confidence interval where they can be possibly shifted.
	 *
	 * For pairwise matching, one of the tiles is 'fixed' and the other one is 'moving'.
	 * This function combines these confidence intervals of the two tiles to just one interval that represents
	 * variability of the new offset between them vs. stage offset between them.
	 *
	 * @param fixedSearchRadius
	 * @param movingSearchRadius
	 * @return
	 * @throws PipelineExecutionException
	 */
	public EstimatedTileBoxSearchRadius getCombinedCovariancesSearchRadius(
			final EstimatedTileBoxSearchRadius fixedSearchRadius,
			final EstimatedTileBoxSearchRadius movingSearchRadius ) throws PipelineExecutionException
	{
		return new EstimatedTileBoxSearchRadius(
				new ErrorEllipse(
						searchRadiusMultiplier,
						getCombinedMeanOffset( fixedSearchRadius, movingSearchRadius ),
						getCombinedCovariances( fixedSearchRadius, movingSearchRadius )
					),
				movingSearchRadius.tileBox,
				getCombinedNeighboringTiles( fixedSearchRadius, movingSearchRadius )
			);
	}

	static double[][] getCombinedCovariances( final EstimatedTileBoxSearchRadius fixedSearchRadius, final EstimatedTileBoxSearchRadius movingSearchRadius )
	{
		final int dim = Math.max( fixedSearchRadius.errorEllipse.numDimensions(), movingSearchRadius.errorEllipse.numDimensions() );
		final double[][] combinedOffsetsCovarianceMatrix = new double[ dim ][ dim ];
		for ( int dRow = 0; dRow < dim; ++dRow )
			for ( int dCol = 0; dCol < dim; ++dCol )
				combinedOffsetsCovarianceMatrix[ dRow ][ dCol ] = fixedSearchRadius.errorEllipse.getOffsetsCovarianceMatrix()[ dRow ][ dCol ] + movingSearchRadius.errorEllipse.getOffsetsCovarianceMatrix()[ dRow ][ dCol ];
		return combinedOffsetsCovarianceMatrix;
	}

	static double[] getCombinedMeanOffset( final EstimatedTileBoxSearchRadius fixedSearchRadius, final EstimatedTileBoxSearchRadius movingSearchRadius )
	{
		final int dim = Math.max( fixedSearchRadius.errorEllipse.numDimensions(), movingSearchRadius.errorEllipse.numDimensions() );
		final double[] combinedOffsetsMeanValues = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			combinedOffsetsMeanValues[ d ] = movingSearchRadius.errorEllipse.getOffsetsMeanValues()[ d ] - fixedSearchRadius.errorEllipse.getOffsetsMeanValues()[ d ];
		return combinedOffsetsMeanValues;
	}

	static Set< TileInfo > getCombinedNeighboringTiles( final EstimatedTileBoxSearchRadius fixedSearchRadius, final EstimatedTileBoxSearchRadius movingSearchRadius )
	{
		final Set< TileInfo > combinedNeighboringTilesSet = new HashSet<>();
		combinedNeighboringTilesSet.addAll( Optional.ofNullable( fixedSearchRadius.neighboringTiles ).orElse( Collections.emptySet() ) );
		combinedNeighboringTilesSet.addAll( Optional.ofNullable( movingSearchRadius.neighboringTiles ).orElse( Collections.emptySet() ) );
		return combinedNeighboringTilesSet;
	}

	Interval getEstimationWindow( final SubdividedTileBox tileBox )
	{
		final double[] adjustedStageWindowPosition = new double[ tileBox.numDimensions() ];
		for ( int d = 0; d < tileBox.numDimensions(); ++d )
			adjustedStageWindowPosition[ d ] = tileBox.getBoundaries().min( d ) == 0 ? tileBox.getFullTile().realMin( d ) : tileBox.getFullTile().realMax( d );

		final long[] estimationWindowMin = new long[ estimationWindowSize.length ], estimationWindowMax = new long[ estimationWindowSize.length ];
		for ( int d = 0; d < estimationWindowSize.length; ++d )
		{
			estimationWindowMin[ d ] = ( long ) Math.floor( adjustedStageWindowPosition[ d ] - estimationWindowSize[ d ] / 2 );
			estimationWindowMax[ d ] = ( long ) Math.ceil ( adjustedStageWindowPosition[ d ] + estimationWindowSize[ d ] / 2 );
		}

		return new FinalInterval( estimationWindowMin, estimationWindowMax );
	}
}
