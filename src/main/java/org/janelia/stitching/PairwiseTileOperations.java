package org.janelia.stitching;

import org.janelia.stitching.TileSearchRadiusEstimator.EstimatedTileBoxRelativeSearchRadius;
import org.janelia.stitching.TileSearchRadiusEstimator.EstimatedTileBoxWorldSearchRadius;

import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.realtransform.RealTransformSequence;
import net.imglib2.realtransform.Translation;

public class PairwiseTileOperations
{
	/**
	 * Returns new error ellipse that is a combination of error ellipses for the fixed box and the moving box.
	 *
	 * @param searchRadiusEstimator
	 * @param tileBoxes
	 * @return
	 * @throws PipelineExecutionException
	 */
	public static EstimatedTileBoxRelativeSearchRadius getCombinedSearchRadiusForMovingBox(
			final TileSearchRadiusEstimator searchRadiusEstimator,
			final SubdividedTileBox[] tileBoxes ) throws PipelineExecutionException
	{
		final EstimatedTileBoxWorldSearchRadius[] searchRadiusStats = new EstimatedTileBoxWorldSearchRadius[ tileBoxes.length ];
		for ( int i = 0; i < tileBoxes.length; ++i )
			searchRadiusStats[ i ] = searchRadiusEstimator.estimateSearchRadiusWithinWindow( tileBoxes[ i ] );

		final EstimatedTileBoxRelativeSearchRadius combinedSearchRadiusForMovingBox = searchRadiusEstimator.getCombinedCovariancesSearchRadius(
				searchRadiusStats[ 0 ],
				searchRadiusStats[ 1 ]
			);

		return combinedSearchRadiusForMovingBox;
	}

	/**
	 * Returns the transformation to map the moving tile into the coordinate space of the fixed tile.
	 *
	 * @param tileTransforms
	 * @return
	 */
	public static InvertibleRealTransform getMovingTileToFixedTileTransform( final AffineGet[] tileTransforms )
	{
		final InvertibleRealTransformSequence movingTileToFixedTileTransform = new InvertibleRealTransformSequence();
		movingTileToFixedTileTransform.add( tileTransforms[ 1 ] );           // moving tile -> world
		movingTileToFixedTileTransform.add( tileTransforms[ 0 ].inverse() ); // world -> fixed tile
		return movingTileToFixedTileTransform;

	}

	/**
	 * Returns the transformation to map the moving tile into the coordinate space of the fixed box.
	 *
	 * @param tileBoxPair
	 * @param tileTransforms
	 * @return
	 */
	public static InvertibleRealTransform getMovingTileToFixedBoxTransform(
			final SubdividedTileBoxPair tileBoxPair,
			final AffineGet[] tileTransforms )
	{
		return getFixedBoxTransform( tileBoxPair, getMovingTileToFixedTileTransform( tileTransforms ) );
	}

	/**
	 * Maps the given transformation in the fixed tile space into the fixed box space.
	 *
	 * @param tileBoxPair
	 * @param transformToFixedTileSpace
	 * @return
	 */
	public static InvertibleRealTransform getFixedBoxTransform(
			final SubdividedTileBoxPair tileBoxPair,
			final InvertibleRealTransform transformToFixedTileSpace )
	{
		final InvertibleRealTransformSequence fixedTileToFixedBoxTransform = new InvertibleRealTransformSequence();
		fixedTileToFixedBoxTransform.add( transformToFixedTileSpace ); // ... -> fixed tile
		fixedTileToFixedBoxTransform.add( new Translation( tileBoxPair.getA().getPosition() ).inverse() ); // fixed tile -> fixed box
		return fixedTileToFixedBoxTransform;
	}

	/**
	 * Returns the offset between zero-min of the transformed moving box and its bounding box.
	 *
	 * @param tileBoxPair
	 * @param tileTransforms
	 * @return
	 */
	public static double[] getTransformedMovingBoxToBoundingBoxOffset(
			final SubdividedTileBoxPair tileBoxPair,
			final AffineGet[] tileTransforms )
	{
		return getTransformedMovingBoxToBoundingBoxOffset(
				tileBoxPair,
				getMovingTileToFixedBoxTransform( tileBoxPair, tileTransforms )
			);
	}

	/**
	 * Returns the offset between zero-min of the transformed moving box and its bounding box.
	 *
	 * @param tileBoxPair
	 * @param movingTileToFixedBoxTransform
	 * @return
	 */
	public static double[] getTransformedMovingBoxToBoundingBoxOffset(
			final SubdividedTileBoxPair tileBoxPair,
			final InvertibleRealTransform movingTileToFixedBoxTransform )
	{
		final double[] transformedMovingTileBoxPosition = new double[ tileBoxPair.getB().numDimensions() ];
		movingTileToFixedBoxTransform.apply( tileBoxPair.getB().getPosition(), transformedMovingTileBoxPosition );

		final RealInterval transformedMovingBoxInterval = TransformedTileOperations.getTransformedBoundingBoxReal(
				tileBoxPair.getB(),
				movingTileToFixedBoxTransform
			);

		final double[] transformedMovingTileBoxToBoundingBoxOffset = new double[ tileBoxPair.getB().numDimensions() ];
		for ( int d = 0; d < transformedMovingTileBoxToBoundingBoxOffset.length; ++d )
			transformedMovingTileBoxToBoundingBoxOffset[ d ] = transformedMovingTileBoxPosition[ d ] - transformedMovingBoxInterval.realMin( d );

		return transformedMovingTileBoxToBoundingBoxOffset;
	}

	/**
	 * Builds the transformation for the error ellipse to map it into the coordinate space of the fixed box.
	 *
	 * @param tileBoxPair
	 * @param tileTransforms
	 * @return
	 */
	public static InvertibleRealTransform getErrorEllipseTransform(
			final SubdividedTileBoxPair tileBoxPair,
			final AffineGet[] tileTransforms )
	{
		final InvertibleRealTransform movingTileToFixedBoxTransform = getMovingTileToFixedBoxTransform( tileBoxPair, tileTransforms );
		final double[] transformedMovingTileBoxToBoundingBoxOffset = getTransformedMovingBoxToBoundingBoxOffset( tileBoxPair, movingTileToFixedBoxTransform );

		final InvertibleRealTransformSequence errorEllipseTransform = new InvertibleRealTransformSequence();
		errorEllipseTransform.add( movingTileToFixedBoxTransform ); // moving tile -> fixed box
		errorEllipseTransform.add( new Translation( transformedMovingTileBoxToBoundingBoxOffset ).inverse() ); // transformed box top-left -> bounding box top-left

		return errorEllipseTransform;
	}

	/**
	 * Builds the new transformation for the moving tile based on the new offset of its tile box.
	 *
	 * @param tileBoxPair
	 * @param tileTransforms
	 * @param movingBoundingBoxOffset
	 * @return
	 */
	public static AffineGet getNewMovingTileTransform(
			final SubdividedTileBoxPair tileBoxPair,
			final AffineGet[] tileTransforms,
			final double[] movingBoundingBoxOffset )
	{
		final int dim = movingBoundingBoxOffset.length;

		// Resulting offset is between the moving bounding box in the fixed box space.
		// Convert it to the offset between the moving and fixed boxes in the global translated space (where the linear affine component of each tile has been undone).
		final RealTransformSequence offsetToWorldTransform = new RealTransformSequence();
		offsetToWorldTransform.add( new Translation( getTransformedMovingBoxToBoundingBoxOffset( tileBoxPair, tileTransforms ) ) ); // bounding box top-left in fixed box space -> transformed top-left in fixed box space
		offsetToWorldTransform.add( new Translation( tileBoxPair.getA().getPosition() ) ); // fixed box -> tixed tile
		offsetToWorldTransform.add( tileTransforms[ 0 ] ); // fixed tile -> world

		final double[] newMovingBoxWorldPosition = new double[ dim ];
		offsetToWorldTransform.apply( movingBoundingBoxOffset, newMovingBoxWorldPosition );

		final double[] movingBoxWorldPosition = new double[ dim ];
		tileTransforms[ 1 ].apply( tileBoxPair.getB().getPosition(), movingBoxWorldPosition );

		final double[] newMovingBoxWorldOffset = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			newMovingBoxWorldOffset[ d ] = newMovingBoxWorldPosition[ d ] - movingBoxWorldPosition[ d ];

		// new translation component
		final AffineGet movingTileTransformTranslationComponent = TransformUtils.getTranslationalComponent( tileTransforms[ 1 ] );
		final double[] newMovingTileTransformTranslationComponent = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			newMovingTileTransformTranslationComponent[ d ] = newMovingBoxWorldOffset[ d ] + movingTileTransformTranslationComponent.get( d, dim );

		// linear component stays the same
		final AffineGet movingTileTransformLinearComponent = TransformUtils.getLinearComponent( tileTransforms[ 1 ] );

		// new affine matrix
		final double[][] newMovingTileTransformAffineMatrix = new double[ dim ][ dim + 1 ];
		for ( int dRow = 0; dRow < dim; ++dRow )
		{
			newMovingTileTransformAffineMatrix[ dRow ][ dim ] = newMovingTileTransformTranslationComponent[ dRow ];
			for ( int dCol = 0; dCol < dim; ++dCol )
				newMovingTileTransformAffineMatrix[ dRow ][ dCol ] = movingTileTransformLinearComponent.get( dRow, dCol );
		}

		return TransformUtils.createTransform( newMovingTileTransformAffineMatrix );
	}

	/**
	 * Returns transformed offset between the fixed box and the moving box at its new position.
	 *
	 * @param tileBoxPair
	 * @param tileTransforms
	 * @param movingBoundingBoxOffset
	 * @return
	 */
	public static double[] getNewStitchedOffset(
			final SubdividedTileBoxPair tileBoxPair,
			final AffineGet[] tileTransforms,
			final double[] movingBoundingBoxOffset )
	{
		final int dim = movingBoundingBoxOffset.length;

		final double[] fixedBoxTranslatedPosition = new double[ dim ];
		TransformUtils.undoLinearComponent( tileTransforms[ 0 ] ).apply( tileBoxPair.getA().getPosition(), fixedBoxTranslatedPosition );

		final double[] newMovingBoxTranslatedPosition = new double[ dim ];
		final AffineGet newMovingTileTransform = getNewMovingTileTransform( tileBoxPair, tileTransforms, movingBoundingBoxOffset );
		TransformUtils.undoLinearComponent( newMovingTileTransform ).apply( tileBoxPair.getB().getPosition(), newMovingBoxTranslatedPosition );

		final double[] stitchedOffset = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			stitchedOffset[ d ] = newMovingBoxTranslatedPosition[ d ] - fixedBoxTranslatedPosition[ d ];
		return stitchedOffset;
	}
}
