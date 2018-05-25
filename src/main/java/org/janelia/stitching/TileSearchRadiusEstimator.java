package org.janelia.stitching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.Covariance;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.KDTree;
import net.imglib2.RealLocalizable;
import net.imglib2.RealPoint;
import net.imglib2.concatenate.Concatenable;
import net.imglib2.concatenate.PreConcatenable;
import net.imglib2.neighborsearch.IntervalNeighborSearchOnKDTree;
import net.imglib2.neighborsearch.KNearestNeighborSearchOnKDTree;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;

public class TileSearchRadiusEstimator implements Serializable
{
	public static class EstimatedWorldSearchRadius
	{
		public final ErrorEllipse errorEllipse;
		public final TileInfo tile;
		public final Set< TileInfo > neighboringTiles;
		public final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates;

		public EstimatedWorldSearchRadius(
				final ErrorEllipse errorEllipse,
				final TileInfo tile,
				final Set< TileInfo > neighboringTiles,
				final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates )
		{
			this.errorEllipse = errorEllipse;
			this.tile = tile;
			this.neighboringTiles = neighboringTiles;
			this.stageAndWorldCoordinates = stageAndWorldCoordinates;
		}
	}

	public static class EstimatedRelativeSearchRadius
	{
		public final ErrorEllipse combinedErrorEllipse;
		public final EstimatedWorldSearchRadius[] worldSearchRadiusStats;

		public EstimatedRelativeSearchRadius(
				final ErrorEllipse combinedErrorEllipse,
				final EstimatedWorldSearchRadius[] worldSearchRadiusStats )
		{
			this.combinedErrorEllipse = combinedErrorEllipse;
			this.worldSearchRadiusStats = worldSearchRadiusStats;

			for ( int d = 0; d < combinedErrorEllipse.numDimensions(); ++d )
				if ( !Util.isApproxEqual( combinedErrorEllipse.getEllipseCenter()[ d ], 0, 1e-10 ) )
					throw new IllegalArgumentException( "A combined error ellipse is expected to be defined in relative coordinate space and be zero-centered" );
		}
	}

	private static final long serialVersionUID = 3966655006478467424L;

	final double[] estimationWindowSize;
	final double searchRadiusMultiplier;

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
			stageSubsetPositions.add( new RealPoint( tileWithStitchedTransform.getStagePosition() ) );

		// build a search tree to be able to look up stage positions and corresponding stitched positions
		// in the neighborhood for any given stage position
		kdTree = new KDTree<>( tilesWithStitchedTransform, stageSubsetPositions );
	}

	public EstimatedWorldSearchRadius estimateSearchRadiusWithinWindow( final TileInfo tile ) throws PipelineExecutionException
	{
		return estimateSearchRadiusWithinWindow( tile, getEstimationWindow( tile ) );
	}

	public EstimatedWorldSearchRadius estimateSearchRadiusWithinWindow( final TileInfo tile, final Interval estimationWindow ) throws PipelineExecutionException
	{
		return estimateSearchRadius( tile, findTilesWithinWindow( estimationWindow ) );
	}

	public EstimatedWorldSearchRadius estimateSearchRadiusKNearestNeighbors( final TileInfo tile, final int numNearestNeighbors ) throws PipelineExecutionException
	{
		return estimateSearchRadius(
				tile,
				findNearestTiles(
						new RealPoint( tile.getStagePosition() ),
						numNearestNeighbors
					)
			);
	}

	public Set< TileInfo > findTilesWithinWindow( final Interval estimationWindow )
	{
		final IntervalNeighborSearchOnKDTree< TileInfo > intervalSearch = new IntervalNeighborSearchOnKDTree<>( kdTree );
		final Set< TileInfo > neighboringTiles = new HashSet<>( intervalSearch.search( estimationWindow ) );
		return neighboringTiles;
	}

	public Set< TileInfo > findNearestTiles( final RealPoint point, final int numNearestNeighbors )
	{
		final KNearestNeighborSearchOnKDTree< TileInfo > neighborsSearch = new KNearestNeighborSearchOnKDTree<>( kdTree, numNearestNeighbors );
		neighborsSearch.search( point );
		final Set< TileInfo > neighboringTiles = new HashSet<>();
		for ( int i = 0; i < neighborsSearch.getK(); ++i )
			neighboringTiles.add( neighborsSearch.getSampler( i ).get() );
		return neighboringTiles;
	}

	private EstimatedWorldSearchRadius estimateSearchRadius( final TileInfo tile, final Set< TileInfo > neighboringTiles ) throws PipelineExecutionException
	{
		final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates = getStageAndWorldCoordinates( neighboringTiles );

		final List< double[] > dimOffsets = new ArrayList<>();
		for ( final Pair< RealPoint, RealPoint > stageAndWorld : stageAndWorldCoordinates )
		{
			final double[] dimOffset = new double[ tile.numDimensions() ];
			for ( int d = 0; d < dimOffset.length; ++d )
				dimOffset[ d ] = stageAndWorld.getB().getDoublePosition( d ) - stageAndWorld.getA().getDoublePosition( d );
			dimOffsets.add( dimOffset );
		}

		final double[] dimOffsetsMeanValues = new double[ tile.numDimensions() ];
		for ( int d = 0; d < dimOffsetsMeanValues.length; ++d )
		{
			for ( final double[] dimOffset : dimOffsets )
				dimOffsetsMeanValues[ d ] += dimOffset[ d ];
			dimOffsetsMeanValues[ d ] /= stageAndWorldCoordinates.size();
		}

		final RealMatrix dimOffsetsMatrix = MatrixUtils.createRealMatrix( dimOffsets.toArray( new double[ 0 ][] ) );
		final RealMatrix dimOffsetsCovarianceMatrix = new Covariance( dimOffsetsMatrix, false ).getCovarianceMatrix();

		final ErrorEllipse searchRadius = new ErrorEllipse( searchRadiusMultiplier, dimOffsetsMeanValues, dimOffsetsCovarianceMatrix.getData() );
		return new EstimatedWorldSearchRadius( searchRadius, tile, neighboringTiles, stageAndWorldCoordinates );
	}

	static double[] getEstimationWindowSize( final long[] tileSize, final int[] statsWindowTileSize )
	{
		final double[] estimationWindowSize = new double[ tileSize.length ];
		for ( int d = 0; d < tileSize.length; ++d )
			estimationWindowSize[ d ] = tileSize[ d ] * statsWindowTileSize[ d ];
		return estimationWindowSize;
	}

	static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > List< Pair< RealPoint, RealPoint > > getStageAndWorldCoordinates(
			final Set< TileInfo > neighboringTiles )
	{
		final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates = new ArrayList<>();
		for ( final TileInfo neighboringTile : neighboringTiles )
		{
			// invert the linear part of the affine transformation
			final AffineGet neighboringTileTransform = TransformedTileOperations.getTileTransform( neighboringTile );
			final RealTransform neighboringTileLocalToOffsetTransform = TransformUtils.undoLinearComponent( neighboringTileTransform );

			final double[] stagePosition = neighboringTile.getStagePosition();
			final double[] transformedPosition = new double[ neighboringTile.numDimensions() ];
			neighboringTileLocalToOffsetTransform.apply( transformedPosition, transformedPosition );

			stageAndWorldCoordinates.add( new ValuePair<>( new RealPoint( stagePosition ), new RealPoint( transformedPosition ) ) );
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
	 * @param worldSearchRadiusStats
	 * @return
	 * @throws PipelineExecutionException
	 */
	public EstimatedRelativeSearchRadius getCombinedCovariancesSearchRadius( final EstimatedWorldSearchRadius[] worldSearchRadiusStats ) throws PipelineExecutionException
	{
		final int dim = worldSearchRadiusStats[ 0 ].errorEllipse.numDimensions();
		final double[][] combinedOffsetsCovarianceMatrix = new double[ dim ][ dim ];
		for ( final EstimatedWorldSearchRadius worldSearchRadius : worldSearchRadiusStats )
			for ( int dRow = 0; dRow < dim; ++dRow )
				for ( int dCol = 0; dCol < dim; ++dCol )
					combinedOffsetsCovarianceMatrix[ dRow ][ dCol ] += worldSearchRadius.errorEllipse.getOffsetsCovarianceMatrix()[ dRow ][ dCol ];

		return new EstimatedRelativeSearchRadius(
				new ErrorEllipse(
						searchRadiusMultiplier,
						new double[ dim ], // zero-centered
						combinedOffsetsCovarianceMatrix
					),
				worldSearchRadiusStats
			);
	}

	Interval getEstimationWindow( final TileInfo tile )
	{
		return getEstimationWindow( new RealPoint( tile.getStagePosition() ) );
	}

	Interval getEstimationWindow( final RealPoint point )
	{
		final long[] estimationWindowMin = new long[ estimationWindowSize.length ], estimationWindowMax = new long[ estimationWindowSize.length ];
		for ( int d = 0; d < estimationWindowSize.length; ++d )
		{
			estimationWindowMin[ d ] = ( long ) Math.floor( point.getDoublePosition( d ) - estimationWindowSize[ d ] / 2 );
			estimationWindowMax[ d ] = ( long ) Math.ceil ( point.getDoublePosition( d ) + estimationWindowSize[ d ] / 2 );
		}
		return new FinalInterval( estimationWindowMin, estimationWindowMax );
	}
}
