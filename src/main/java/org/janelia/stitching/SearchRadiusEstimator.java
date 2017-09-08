package org.janelia.stitching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.KDTree;
import net.imglib2.RealLocalizable;
import net.imglib2.RealPoint;
import net.imglib2.neighborsearch.IntervalNeighborSearchOnKDTree;
import net.imglib2.neighborsearch.KNearestNeighborSearchOnKDTree;

public class SearchRadiusEstimator implements Serializable
{
	private static final long serialVersionUID = -1337122618818271339L;

	private final Map< Integer, double[] > stageValues, stitchedValues;
	private final double[] estimationWindowSize;
	private final double searchRadiusMultiplier;

	private final KDTree< Integer > tree;

	public SearchRadiusEstimator(
			final Map< Integer, double[] > stageValues,
			final Map< Integer, double[] > stitchedValues,
			final double[] estimationWindowSize,
			final double searchRadiusMultiplier )
	{
		this.stageValues = stageValues;
		this.stitchedValues = stitchedValues;
		this.estimationWindowSize = estimationWindowSize;
		this.searchRadiusMultiplier = searchRadiusMultiplier;

		final List< Integer > stageSubsetIndexes = new ArrayList<>();
		final List< RealLocalizable > stageSubsetPositions = new ArrayList<>();
		for ( final Entry< Integer, double[] > stitchedEntry : stitchedValues.entrySet() )
		{
			final double[] correspondingStageVal = stageValues.get( stitchedEntry.getKey() );
			stageSubsetIndexes.add( stitchedEntry.getKey() );
			stageSubsetPositions.add( new RealPoint( correspondingStageVal ) );
		}

		// build a search tree
		tree = new KDTree<>( stageSubsetIndexes, stageSubsetPositions );
	}

	public double[] getEstimationWindowSize()
	{
		return estimationWindowSize;
	}

	@Deprecated
	/*
	 * Uses exhaustive search over a subset of stage tiles within the estimation window
	 */
	public SearchRadius getSearchRadiusWithinEstimationWindow( final double[] stagePosition ) throws PipelineExecutionException
	{
		return getSearchRadiusWithinInterval( getEstimationWindow( stagePosition ), stagePosition );
	}
	@Deprecated
	/*
	 * Uses exhaustive search over a subset of stage tiles within the specified interval
	 */
	public SearchRadius getSearchRadiusWithinInterval( final Interval neighborSearchInterval ) throws PipelineExecutionException
	{
		return getSearchRadiusWithinInterval( neighborSearchInterval, null );
	}
	@Deprecated
	/*
	 * Uses exhaustive search over a subset of stage tiles within the specified interval
	 */
	public SearchRadius getSearchRadiusWithinInterval( final Interval neighborSearchInterval, final double[] stagePosition ) throws PipelineExecutionException
	{
		final List< Integer > indexesWithinWindow = new ArrayList<>();
		for ( final Entry< Integer, double[] > stitchedEntry : stitchedValues.entrySet() )
		{
			final double[] correspondingStageVal = stageValues.get( stitchedEntry.getKey() );

			boolean withinWindow = true;
			for ( int d = 0; d < neighborSearchInterval.numDimensions(); ++d )
				withinWindow &= correspondingStageVal[ d ] >= neighborSearchInterval.min( d ) && correspondingStageVal[ d ] <= neighborSearchInterval.max( d );

			if ( withinWindow )
				indexesWithinWindow.add( stitchedEntry.getKey() );
		}
		return getSearchRadius( indexesWithinWindow, stagePosition );
	}

	/*
	 * Uses optimized KD-tree within the estimation window
	 */
	public SearchRadius getSearchRadiusTreeWithinEstimationWindow( final double[] stagePosition ) throws PipelineExecutionException
	{
		return getSearchRadiusTreeWithinInterval( getEstimationWindow( stagePosition ), stagePosition );
	}

	/*
	 * Uses optimized KD-tree within the specified interval
	 */
	public SearchRadius getSearchRadiusTreeWithinInterval( final Interval neighborSearchInterval ) throws PipelineExecutionException
	{
		final double[] neighborSearchIntervalMiddlePoint = new double[ neighborSearchInterval.numDimensions() ];
		for ( int d = 0; d < neighborSearchInterval.numDimensions(); ++d )
			neighborSearchIntervalMiddlePoint[ d ] = ( neighborSearchInterval.realMin( d ) + neighborSearchInterval.realMax( d ) ) / 2;
		return getSearchRadiusTreeWithinInterval( neighborSearchInterval, neighborSearchIntervalMiddlePoint );
	}

	/*
	 * Uses optimized KD-tree within the specified interval
	 */
	public SearchRadius getSearchRadiusTreeWithinInterval( final Interval neighborSearchInterval, final double[] stagePosition ) throws PipelineExecutionException
	{
		final IntervalNeighborSearchOnKDTree< Integer > intervalSearch = new IntervalNeighborSearchOnKDTree<>( tree );
		final List< Integer > indexesWithinWindow = intervalSearch.search( neighborSearchInterval );
		return getSearchRadius( indexesWithinWindow, stagePosition );
	}

	/*
	 * Uses optimized KD-tree with a constraint on number of nearest neighbors
	 */
	public SearchRadius getSearchRadiusTreeUsingKNearestNeighbors( final double[] stagePosition, final int numNearestNeighbors ) throws PipelineExecutionException
	{
		final KNearestNeighborSearchOnKDTree< Integer > neighborsSearch = new KNearestNeighborSearchOnKDTree<>( tree, numNearestNeighbors );
		neighborsSearch.search( new RealPoint( stagePosition ) );
		final List< Integer > pointIndexes = new ArrayList<>();
		for ( int i = 0; i < neighborsSearch.getK(); ++i )
			pointIndexes.add( neighborsSearch.getSampler( i ).get() );
		return getSearchRadius( pointIndexes, stagePosition );
	}

	private SearchRadius getSearchRadius( final List< Integer > pointIndexes, final double[] stagePosition ) throws PipelineExecutionException
	{
		final double[] meanValues = new double[ numDimensions() ];
		for ( int d = 0; d < meanValues.length; ++d )
		{
			double dOffsetSum = 0;
			for ( final int pointIndex : pointIndexes )
				dOffsetSum += stitchedValues.get( pointIndex )[ d ] - stageValues.get( pointIndex )[ d ];

			meanValues[ d ] = dOffsetSum / pointIndexes.size();
		}

		final double[][] covarianceMatrix = new double[ numDimensions() ][ numDimensions() ];
		for ( int dRow = 0; dRow < covarianceMatrix.length; ++dRow )
		{
			for ( int dCol = dRow; dCol < covarianceMatrix[ dRow ].length; ++dCol )
			{
				double dRowColOffsetSumProduct = 0;
				for ( final int pointIndex : pointIndexes )
				{
					final double dRowOffset = stitchedValues.get( pointIndex )[ dRow ] - stageValues.get( pointIndex )[ dRow ];
					final double dColOffset = stitchedValues.get( pointIndex )[ dCol ] - stageValues.get( pointIndex )[ dCol ];
					dRowColOffsetSumProduct += dRowOffset * dColOffset;
				}
				final double covariance = dRowColOffsetSumProduct / pointIndexes.size() - meanValues[ dRow ] * meanValues[ dCol ];
				covarianceMatrix[ dRow ][ dCol ] = covarianceMatrix[ dCol ][ dRow ]  = covariance;
			}
		}

		System.out.println( "Using searchRadiusMultiplier=" + searchRadiusMultiplier + " to stretch the error ellipse with respect to standard deviation" );

		return new SearchRadius( searchRadiusMultiplier, meanValues, covarianceMatrix, pointIndexes, stagePosition );
	}

	private Interval getEstimationWindow( final double[] stagePosition )
	{
		final long[] estimationWindowMin = new long[ estimationWindowSize.length ], estimationWindowMax = new long[ estimationWindowSize.length ];
		for ( int d = 0; d < estimationWindowSize.length; ++d )
		{
			estimationWindowMin[ d ] = ( long ) Math.floor( stagePosition[ d ] - estimationWindowSize[ d ] / 2 );
			estimationWindowMax[ d ] = ( long ) Math.ceil ( stagePosition[ d ] + estimationWindowSize[ d ] / 2 );
		}
		return new FinalInterval( estimationWindowMin, estimationWindowMax );
	}

	private int numDimensions()
	{
		return estimationWindowSize.length;
	}

	public SearchRadius getCombinedCovariancesSearchRadius( final SearchRadius fixedSearchRadius, final SearchRadius movingSearchRadius ) throws PipelineExecutionException
	{
		final double[][] combinedOffsetsCovarianceMatrix = new double[ numDimensions() ][ numDimensions() ];
		for ( int dRow = 0; dRow < numDimensions(); ++dRow )
			for ( int dCol = 0; dCol < numDimensions(); ++dCol )
				combinedOffsetsCovarianceMatrix[ dRow ][ dCol ] = fixedSearchRadius.getOffsetsCovarianceMatrix()[ dRow ][ dCol ] + movingSearchRadius.getOffsetsCovarianceMatrix()[ dRow ][ dCol ];

		final double[] combinedOffsetsMeanValues = new double[ numDimensions() ];
		for ( int d = 0; d < numDimensions(); ++d )
			combinedOffsetsMeanValues[ d ] = movingSearchRadius.getOffsetsMeanValues()[ d ] - fixedSearchRadius.getOffsetsMeanValues()[ d ];

		final Set< Integer > combinedPointIndexesSet = new HashSet<>();
		combinedPointIndexesSet.addAll( fixedSearchRadius.getUsedPointsIndexes() );
		combinedPointIndexesSet.addAll( movingSearchRadius.getUsedPointsIndexes() );
		final List< Integer > combinedPointIndexes = new ArrayList<>( combinedPointIndexesSet );

		return new SearchRadius( searchRadiusMultiplier, combinedOffsetsMeanValues, combinedOffsetsCovarianceMatrix, combinedPointIndexes, movingSearchRadius.getStagePosition() );
	}
}
