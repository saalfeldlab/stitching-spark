package org.janelia.stitching;

import org.janelia.stitching.OffsetUncertaintyEstimator.EstimatedRelativeSearchRadius;
import org.janelia.stitching.OffsetUncertaintyEstimator.EstimatedWorldSearchRadius;
import org.janelia.stitching.OffsetUncertaintyEstimator.NotEnoughNeighboringTilesException;

import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.realtransform.Translation;
import net.imglib2.util.Intervals;

public class PairwiseTileOperations
{
	private static final int fixedIndex = 0;
	private static final int movingIndex = 1;

	private static void validateInputParametersLength( final Object[]... args )
	{
		for ( final Object[] arg : args )
			if ( arg.length != 2 )
				throw new IllegalArgumentException( "Incorrect number of input parameters" );
	}

	/**
	 * Returns new error ellipse that is a combination of error ellipses for the fixed subtile and the moving subtile.
	 *
	 * @param offsetUncertaintyEstimator
	 * @param subTiles
	 * @return
	 * @throws PipelineExecutionException
	 * @throws NotEnoughNeighboringTilesException
	 */
	public static EstimatedRelativeSearchRadius getCombinedSearchRadiusForMovingSubTile(
			final SubTile[] subTiles,
			final OffsetUncertaintyEstimator offsetUncertaintyEstimator ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		validateInputParametersLength( subTiles );

		final EstimatedWorldSearchRadius[] searchRadiusStats = new EstimatedWorldSearchRadius[ subTiles.length ];
		for ( int i = 0; i < subTiles.length; ++i )
			searchRadiusStats[ i ] = offsetUncertaintyEstimator.estimateSearchRadiusWithinWindow( subTiles[ i ] );

		return offsetUncertaintyEstimator.getCombinedCovariancesSearchRadius( searchRadiusStats );
	}

	/**
	 * Returns the transformation to map the moving tile into the coordinate space of the fixed tile.
	 *
	 * @param tileTransforms
	 * @return
	 */
	public static InvertibleRealTransform getMovingTileToFixedTileTransform( final AffineGet[] tileTransforms )
	{
		validateInputParametersLength( tileTransforms );

		final InvertibleRealTransformSequence movingTileToFixedTileTransform = new InvertibleRealTransformSequence();
		movingTileToFixedTileTransform.add( tileTransforms[ movingIndex ] ); // moving tile -> world
		movingTileToFixedTileTransform.add( tileTransforms[ fixedIndex ].inverse() ); // world -> fixed tile
		return movingTileToFixedTileTransform;
	}

	/**
	 * Returns the transformation to map the moving tile into the coordinate space of the fixed subtile.
	 *
	 * @param subTiles
	 * @param tileTransforms
	 * @return
	 */
	public static InvertibleRealTransform getMovingTileToFixedSubTileTransform(
			final SubTile[] subTiles,
			final AffineGet[] tileTransforms )
	{
		validateInputParametersLength( subTiles, tileTransforms );

		return getFixedSubTileTransform(
				subTiles,
				getMovingTileToFixedTileTransform( tileTransforms )
			);
	}

	/**
	 * Maps the given transformation in the fixed tile space into the fixed subtile space.
	 *
	 * @param subTiles
	 * @param transformToFixedTileSpace
	 * @return
	 */
	public static InvertibleRealTransform getFixedSubTileTransform(
			final SubTile[] subTiles,
			final InvertibleRealTransform transformToFixedTileSpace )
	{
		validateInputParametersLength( subTiles );

		final InvertibleRealTransformSequence fixedTileToFixedSubTileTransform = new InvertibleRealTransformSequence();
		fixedTileToFixedSubTileTransform.add( transformToFixedTileSpace ); // ... -> fixed tile
		fixedTileToFixedSubTileTransform.add( new Translation( Intervals.minAsDoubleArray( subTiles[ fixedIndex ] ) ).inverse() ); // fixed tile -> fixed subtile
		return fixedTileToFixedSubTileTransform;
	}

	/**
	 * Returns the offset between zero-min of the transformed moving subtile and its bounding box.
	 *
	 * @param subTiles
	 * @param tileTransforms
	 * @return
	 */
	public static double[] getTransformedMovingSubTileToBoundingBoxOffset(
			final SubTile[] subTiles,
			final AffineGet[] tileTransforms )
	{
		validateInputParametersLength( subTiles, tileTransforms );

		return getTransformedMovingSubTileToBoundingBoxOffset(
				subTiles,
				getMovingTileToFixedSubTileTransform( subTiles, tileTransforms )
			);
	}

	/**
	 * Returns the offset between zero-min of the transformed moving subtile and its bounding box.
	 *
	 * @param subTiles
	 * @param movingTileToFixedSubTileTransform
	 * @return
	 */
	public static double[] getTransformedMovingSubTileToBoundingBoxOffset(
			final SubTile[] subTiles,
			final InvertibleRealTransform movingTileToFixedSubTileTransform )
	{
		validateInputParametersLength( subTiles );

		final double[] transformedMovingSubTilePosition = new double[ subTiles[ movingIndex ].numDimensions() ];
		movingTileToFixedSubTileTransform.apply( Intervals.minAsDoubleArray( subTiles[ movingIndex ] ), transformedMovingSubTilePosition );

		final RealInterval transformedMovingSubTileInterval = TransformedTileOperations.getTransformedBoundingBoxReal(
				subTiles[ movingIndex ],
				movingTileToFixedSubTileTransform
			);

		final double[] transformedMovingSubTileToBoundingBoxOffset = new double[ subTiles[ movingIndex ].numDimensions() ];
		for ( int d = 0; d < transformedMovingSubTileToBoundingBoxOffset.length; ++d )
			transformedMovingSubTileToBoundingBoxOffset[ d ] = transformedMovingSubTilePosition[ d ] - transformedMovingSubTileInterval.realMin( d );

		return transformedMovingSubTileToBoundingBoxOffset;
	}

	/**
	 * Builds the transformation for the error ellipse to map it into the coordinate space of the fixed subtile.
	 *
	 * @param subTiles
	 * @param tileTransforms
	 * @return
	 */
	public static InvertibleRealTransform getErrorEllipseTransform(
			final SubTile[] subTiles,
			final AffineGet[] tileTransforms )
	{
		validateInputParametersLength( subTiles, tileTransforms );

		final InvertibleRealTransform movingTileToFixedSubTileTransform = getMovingTileToFixedSubTileTransform( subTiles, tileTransforms );
		final double[] transformedMovingSubTileToBoundingBoxOffset = getTransformedMovingSubTileToBoundingBoxOffset( subTiles, movingTileToFixedSubTileTransform );

		final InvertibleRealTransformSequence errorEllipseTransform = new InvertibleRealTransformSequence();
		errorEllipseTransform.add( new Translation( Intervals.minAsDoubleArray( subTiles[ movingIndex ] ) ) ); // moving subtile -> moving tile
		errorEllipseTransform.add( movingTileToFixedSubTileTransform ); // moving tile -> fixed subtile
		errorEllipseTransform.add( new Translation( transformedMovingSubTileToBoundingBoxOffset ).inverse() ); // transformed subtile top-left -> bounding box top-left

		return errorEllipseTransform;
	}

	/**
	 * Map the middle point of the moving subtile into the coordinate space of the full fixed tile (useful for point matching).
	 *
	 * @param subTiles
	 * @param fullTileTransforms
	 * @param subTilesOffset
	 * @return
	 */
	public static double[] mapMovingSubTileMiddlePointIntoFixedTile(
			final SubTile[] subTiles,
			final AffineGet[] fullTileTransforms,
			final double[] subTilesOffset )
	{
		// build the transform to convert from the 'moving' tile into the 'fixed' tile
		final InvertibleRealTransform movingTileToFixedTileTransform = PairwiseTileOperations.getMovingTileToFixedTileTransform( fullTileTransforms );

		// transform the 'moving' subtile into the coordinate space of the 'fixed' tile and find its bounding box
		final Interval transformedMovingSubTileBoundingBox = TransformedTileOperations.getTransformedBoundingBox( subTiles[ movingIndex ], movingTileToFixedTileTransform );

		// calculate the new middle point position of the 'moving' subtile in the coordinate space of the 'fixed' tile
		final double[] newTransformedMovingSubTileMiddlePointPosition = new double[ transformedMovingSubTileBoundingBox.numDimensions() ];
		for ( int d = 0; d < transformedMovingSubTileBoundingBox.numDimensions(); ++d )
			newTransformedMovingSubTileMiddlePointPosition[ d ] = subTiles[ fixedIndex ].realMin( d ) + subTilesOffset[ d ] + transformedMovingSubTileBoundingBox.dimension( d ) / 2.;

		return newTransformedMovingSubTileMiddlePointPosition;
	}
}
