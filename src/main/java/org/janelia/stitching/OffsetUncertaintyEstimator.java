package org.janelia.stitching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.janelia.stitching.math.WeightedCovariance;

import net.imglib2.Interval;
import net.imglib2.RealPoint;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;

public class OffsetUncertaintyEstimator implements Serializable
{
	public static class EstimatedWorldSearchRadius
	{
		public final ErrorEllipse errorEllipse;
		public final SubTile subTile;
		public final Set< TileInfo > neighboringTiles;
		public final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates;

		public EstimatedWorldSearchRadius(
				final ErrorEllipse errorEllipse,
				final SubTile subTile,
				final Set< TileInfo > neighboringTiles,
				final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates )
		{
			this.errorEllipse = errorEllipse;
			this.subTile = subTile;
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

	public static class NotEnoughNeighboringTilesException extends Exception
	{
		private static final long serialVersionUID = -6612788597298487220L;

		public final Set< TileInfo > neighboringTiles;
		public final int numNeighboringTilesRequested;

		public NotEnoughNeighboringTilesException( final Set< TileInfo > neighboringTiles, final int numNeighboringTilesRequested )
		{
			super( "Requested " + numNeighboringTilesRequested + " neighboring tiles, found only " + neighboringTiles.size() );
			this.neighboringTiles = neighboringTiles;
			this.numNeighboringTilesRequested = numNeighboringTilesRequested;
		}
	}

	private static final long serialVersionUID = 3966655006478467424L;

	private final NeighboringTilesLocator neighboringTilesLocator;
	private final SampleWeightCalculator sampleWeightCalculator;
	private final double searchRadiusMultiplier;
	private final int minNumNeighboringTiles;

	public OffsetUncertaintyEstimator(
			final NeighboringTilesLocator neighboringTilesLocator,
			final SampleWeightCalculator sampleWeightCalculator,
			final double searchRadiusMultiplier,
			final int minNumNeighboringTiles )
	{
		this.neighboringTilesLocator = neighboringTilesLocator;
		this.sampleWeightCalculator = sampleWeightCalculator;
		this.searchRadiusMultiplier = searchRadiusMultiplier;
		this.minNumNeighboringTiles = minNumNeighboringTiles;
	}

	public NeighboringTilesLocator getNeighboringTilesLocator()
	{
		return neighboringTilesLocator;
	}

	public SampleWeightCalculator getSampleWeightCalculator()
	{
		return sampleWeightCalculator;
	}

	public double getSearchRadiusMultiplier()
	{
		return searchRadiusMultiplier;
	}

	public int getMinNumNeighboringTiles()
	{
		return minNumNeighboringTiles;
	}

	public EstimatedWorldSearchRadius estimateSearchRadiusWithinWindow( final SubTile subTile ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		return estimateSearchRadiusWithinWindow( subTile, neighboringTilesLocator.getSearchWindowInterval( subTile.getFullTile() ) );
	}

	public EstimatedWorldSearchRadius estimateSearchRadiusWithinWindow( final SubTile subTile, final Interval estimationWindow ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		return estimateSearchRadius( subTile, neighboringTilesLocator.findTilesWithinWindow( estimationWindow ) );
	}

	/*public EstimatedWorldSearchRadius estimateSearchRadiusKNearestNeighbors( final TileInfo tile, final int numNearestNeighbors ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		return estimateSearchRadius(
				tile,
				neighboringTilesLocator.findNearestTiles(
						new RealPoint( tile.getStagePosition() ),
						numNearestNeighbors
					)
			);
	}*/

	private EstimatedWorldSearchRadius estimateSearchRadius( final SubTile subTile, final Set< TileInfo > neighboringTiles ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		// do not use the tile in its offset statistics for prediction
		neighboringTiles.removeIf( t -> t.getIndex().equals( subTile.getFullTile().getIndex() ) );

		if ( neighboringTiles.size() < minNumNeighboringTiles )
			throw new NotEnoughNeighboringTilesException( neighboringTiles, minNumNeighboringTiles );

		final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates = getStageAndWorldCoordinates( subTile, neighboringTiles );

		final List< RealPoint > stageCoordinates = new ArrayList<>();
		for ( final Pair< RealPoint, RealPoint > stageAndWorld : stageAndWorldCoordinates )
			stageCoordinates.add( stageAndWorld.getA() );
		final double[] sampleWeights = sampleWeightCalculator.calculateSampleWeights(
				new RealPoint( SubTileOperations.getSubTileMiddlePointStagePosition( subTile ) ),
				stageCoordinates
			);

		final List< double[] > offsetSamples = new ArrayList<>();
		for ( final Pair< RealPoint, RealPoint > stageAndWorld : stageAndWorldCoordinates )
		{
			final double[] offsetSample = new double[ subTile.numDimensions() ];
			for ( int d = 0; d < offsetSample.length; ++d )
				offsetSample[ d ] = stageAndWorld.getB().getDoublePosition( d ) - stageAndWorld.getA().getDoublePosition( d );
			offsetSamples.add( offsetSample );
		}

		final WeightedCovariance weightedCovariance = new WeightedCovariance( offsetSamples, sampleWeights );
		final ErrorEllipse searchRadius = new ErrorEllipse( searchRadiusMultiplier, weightedCovariance.getWeightedMean(), weightedCovariance.getWeightedCovarianceMatrix() );
		return new EstimatedWorldSearchRadius( searchRadius, subTile, neighboringTiles, stageAndWorldCoordinates );
	}

	public static double[] getUncorrelatedErrorEllipseRadius( final long[] tileSize, final double errorEllipseRadiusAsTileSizeRatio )
	{
		final double[] radius = new double[ tileSize.length ];
		for ( int d = 0; d < radius.length; ++d )
			radius[ d ] = tileSize[ d ] * errorEllipseRadiusAsTileSizeRatio;
		return radius;
	}

	public static ErrorEllipse getUncorrelatedErrorEllipse( final double[] radius ) throws PipelineExecutionException
	{
		final double[] zeroMeanValues = new double[ radius.length ];
		final double[][] uncorrelatedCovarianceMatrix = new double[ radius.length ][ radius.length ];
		for ( int d = 0; d < radius.length; ++d )
			uncorrelatedCovarianceMatrix[ d ][ d ] = radius[ d ] * radius[ d ];
		return new ErrorEllipse( 1.0, zeroMeanValues, uncorrelatedCovarianceMatrix );
	}

	static List< Pair< RealPoint, RealPoint > > getStageAndWorldCoordinates( final SubTile subTile, final Set< TileInfo > neighboringTiles )
	{
		final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates = new ArrayList<>();
		for ( final TileInfo neighboringTile : neighboringTiles )
		{
			// find corresponding subtile in the neighboring tile
			final int[] subdivisionGridSize = new int[ neighboringTile.numDimensions() ];
			Arrays.fill( subdivisionGridSize, 2 );
			final List< SubTile > neighboringSubTiles = SubTileOperations.subdivideTiles( new TileInfo[] { neighboringTile }, subdivisionGridSize );

			SubTile correspondingNeighboringSubTile = null;
			for ( final SubTile neighboringSubTile : neighboringSubTiles )
			{
				if ( Intervals.equals( neighboringSubTile, subTile ) )
				{
					correspondingNeighboringSubTile = neighboringSubTile;
					break;
				}
			}

			final double[] middlePointStagePosition = SubTileOperations.getSubTileMiddlePointStagePosition( correspondingNeighboringSubTile );
			final double[] middlePointWorldPosition = TransformedTileOperations.transformSubTileMiddlePoint( correspondingNeighboringSubTile, true );

			stageAndWorldCoordinates.add( new ValuePair<>( new RealPoint( middlePointStagePosition ), new RealPoint( middlePointWorldPosition ) ) );
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
}
