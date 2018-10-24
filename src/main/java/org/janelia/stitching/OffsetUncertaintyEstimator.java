package org.janelia.stitching;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import net.imglib2.Interval;
import net.imglib2.RealPoint;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;

public class OffsetUncertaintyEstimator implements Serializable
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

	public EstimatedWorldSearchRadius estimateSearchRadiusWithinWindow( final TileInfo tile ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		return estimateSearchRadiusWithinWindow( tile, neighboringTilesLocator.getSearchWindowInterval( tile ) );
	}

	public EstimatedWorldSearchRadius estimateSearchRadiusWithinWindow( final TileInfo tile, final Interval estimationWindow ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		return estimateSearchRadius( tile, neighboringTilesLocator.findTilesWithinWindow( estimationWindow ) );
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

	private EstimatedWorldSearchRadius estimateSearchRadius( final TileInfo tile, final Set< TileInfo > neighboringTiles ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		// do not use the tile in its offset statistics for prediction
		neighboringTiles.removeIf( t -> t.getIndex().equals( tile.getIndex() ) );

		if ( neighboringTiles.size() < minNumNeighboringTiles )
			throw new NotEnoughNeighboringTilesException( neighboringTiles, minNumNeighboringTiles );

		final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates = getStageAndWorldCoordinates( neighboringTiles );

		final List< RealPoint > stageCoordinates = new ArrayList<>();
		for ( final Pair< RealPoint, RealPoint > stageAndWorld : stageAndWorldCoordinates )
			stageCoordinates.add( stageAndWorld.getA() );
		final double[] sampleWeights = sampleWeightCalculator.calculateSampleWeights( new RealPoint( tile.getStagePosition() ), stageCoordinates );

		final List< double[] > offsetSamples = new ArrayList<>();
		for ( final Pair< RealPoint, RealPoint > stageAndWorld : stageAndWorldCoordinates )
		{
			final double[] offsetSample = new double[ tile.numDimensions() ];
			for ( int d = 0; d < offsetSample.length; ++d )
				offsetSample[ d ] = stageAndWorld.getB().getDoublePosition( d ) - stageAndWorld.getA().getDoublePosition( d );
			offsetSamples.add( offsetSample );
		}

		final double[] meanOffset = new double[ tile.numDimensions() ];
		for ( int i = 0; i < offsetSamples.size(); ++i )
		{
			final double[] offsetSample = offsetSamples.get( i );
			for ( int d = 0; d < meanOffset.length; ++d )
				meanOffset[ d ] += offsetSample[ d ] * sampleWeights[ i ];
		}

		final double[][] covarianceMatrix = new double[ meanOffset.length ][ meanOffset.length ];
		double covarianceDenomCoeff = 0;
		for ( final double sampleWeight : sampleWeights )
			covarianceDenomCoeff += Math.pow( sampleWeight, 2 );
		covarianceDenomCoeff = 1 - covarianceDenomCoeff;
		for ( int dRow = 0; dRow < covarianceMatrix.length; ++dRow )
		{
			for ( int dCol = dRow; dCol < covarianceMatrix[ dRow ].length; ++dCol )
			{
				double dRowColOffsetSumProduct = 0;
				for ( int i = 0; i < offsetSamples.size(); ++i )
				{
					final double[] offsetSample = offsetSamples.get( i );
					dRowColOffsetSumProduct += ( offsetSample[ dRow ] - meanOffset[ dRow ] ) * ( offsetSample[ dCol ] - meanOffset[ dCol ] ) * sampleWeights[ i ];
				}
				final double covariance = dRowColOffsetSumProduct / covarianceDenomCoeff;
				covarianceMatrix[ dRow ][ dCol ] = covarianceMatrix[ dCol ][ dRow ] = covariance;
			}
		}

		final ErrorEllipse searchRadius = new ErrorEllipse( searchRadiusMultiplier, meanOffset, covarianceMatrix );
		return new EstimatedWorldSearchRadius( searchRadius, tile, neighboringTiles, stageAndWorldCoordinates );
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

	static List< Pair< RealPoint, RealPoint > > getStageAndWorldCoordinates( final Set< TileInfo > neighboringTiles )
	{
		final List< Pair< RealPoint, RealPoint > > stageAndWorldCoordinates = new ArrayList<>();
		for ( final TileInfo neighboringTile : neighboringTiles )
		{
			// invert the linear part of the affine transformation
			final AffineGet neighboringTileTransform = TransformedTileOperations.getTileTransform( neighboringTile, true );
//			final RealTransform neighboringTileLocalToOffsetTransform = TransformUtils.undoLinearComponent( neighboringTileTransform );

			final double[] stagePosition = neighboringTile.getStagePosition();
			final double[] transformedPosition = new double[ neighboringTile.numDimensions() ];
			/*neighboringTileLocalToOffsetTransform*/neighboringTileTransform.apply( new double[ neighboringTile.numDimensions() ], transformedPosition ); // (0,0,0) -> transformed

			if ( neighboringTileTransform != null )
				throw new RuntimeException( "FIXME: check implementation" );

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
}
