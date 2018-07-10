package org.janelia.stitching;

import org.janelia.stitching.TileSearchRadiusEstimator.EstimatedRelativeSearchRadius;
import org.janelia.stitching.TileSearchRadiusEstimator.EstimatedWorldSearchRadius;
import org.janelia.stitching.TileSearchRadiusEstimator.NotEnoughNeighboringTilesException;

import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.realtransform.RealTransformSequence;
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
	 * Returns new error ellipse that is a combination of error ellipses for the fixed box and the moving box.
	 *
	 * @param searchRadiusEstimator
	 * @param tileBoxes
	 * @return
	 * @throws PipelineExecutionException
	 * @throws NotEnoughNeighboringTilesException
	 */
	public static EstimatedRelativeSearchRadius getCombinedSearchRadiusForMovingBox(
			final SubdividedTileBox[] tileBoxes,
			final TileSearchRadiusEstimator searchRadiusEstimator ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		validateInputParametersLength( tileBoxes );

		final EstimatedWorldSearchRadius[] searchRadiusStats = new EstimatedWorldSearchRadius[ tileBoxes.length ];
		for ( int i = 0; i < tileBoxes.length; ++i )
			searchRadiusStats[ i ] = searchRadiusEstimator.estimateSearchRadiusWithinWindow( tileBoxes[ i ].getFullTile() );

		return searchRadiusEstimator.getCombinedCovariancesSearchRadius( searchRadiusStats );
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
		movingTileToFixedTileTransform.add( tileTransforms[ movingIndex ] );           // moving tile -> world
		movingTileToFixedTileTransform.add( tileTransforms[ fixedIndex ].inverse() ); // world -> fixed tile
		return movingTileToFixedTileTransform;

	}

	/**
	 * Returns the transformation to map the moving tile into the coordinate space of the fixed box.
	 *
	 * @param tileBoxes
	 * @param tileTransforms
	 * @return
	 */
	public static InvertibleRealTransform getMovingTileToFixedBoxTransform(
			final SubdividedTileBox[] tileBoxes,
			final AffineGet[] tileTransforms )
	{
		validateInputParametersLength( tileBoxes, tileTransforms );

		return getFixedBoxTransform(
				tileBoxes,
				getMovingTileToFixedTileTransform( tileTransforms )
			);
	}

	/**
	 * Maps the given transformation in the fixed tile space into the fixed box space.
	 *
	 * @param tileBoxes
	 * @param transformToFixedTileSpace
	 * @return
	 */
	public static InvertibleRealTransform getFixedBoxTransform(
			final SubdividedTileBox[] tileBoxes,
			final InvertibleRealTransform transformToFixedTileSpace )
	{
		validateInputParametersLength( tileBoxes );

		final InvertibleRealTransformSequence fixedTileToFixedBoxTransform = new InvertibleRealTransformSequence();
		fixedTileToFixedBoxTransform.add( transformToFixedTileSpace ); // ... -> fixed tile
		fixedTileToFixedBoxTransform.add( new Translation( Intervals.minAsDoubleArray( tileBoxes[ fixedIndex ] ) ).inverse() ); // fixed tile -> fixed box
		return fixedTileToFixedBoxTransform;
	}

	/**
	 * Returns the offset between zero-min of the transformed moving box and its bounding box.
	 *
	 * @param tileBoxes
	 * @param tileTransforms
	 * @return
	 */
	public static double[] getTransformedMovingBoxToBoundingBoxOffset(
			final SubdividedTileBox[] tileBoxes,
			final AffineGet[] tileTransforms )
	{
		validateInputParametersLength( tileBoxes, tileTransforms );

		return getTransformedMovingBoxToBoundingBoxOffset(
				tileBoxes,
				getMovingTileToFixedBoxTransform( tileBoxes, tileTransforms )
			);
	}

	/**
	 * Returns the offset between zero-min of the transformed moving box and its bounding box.
	 *
	 * @param tileBoxes
	 * @param movingTileToFixedBoxTransform
	 * @return
	 */
	public static double[] getTransformedMovingBoxToBoundingBoxOffset(
			final SubdividedTileBox[] tileBoxes,
			final InvertibleRealTransform movingTileToFixedBoxTransform )
	{
		validateInputParametersLength( tileBoxes );

		final double[] transformedMovingTileBoxPosition = new double[ tileBoxes[ movingIndex ].numDimensions() ];
		movingTileToFixedBoxTransform.apply( Intervals.minAsDoubleArray( tileBoxes[ movingIndex ] ), transformedMovingTileBoxPosition );

		final RealInterval transformedMovingBoxInterval = TransformedTileOperations.getTransformedBoundingBoxReal(
				tileBoxes[ movingIndex ],
				movingTileToFixedBoxTransform
			);

		final double[] transformedMovingTileBoxToBoundingBoxOffset = new double[ tileBoxes[ movingIndex ].numDimensions() ];
		for ( int d = 0; d < transformedMovingTileBoxToBoundingBoxOffset.length; ++d )
			transformedMovingTileBoxToBoundingBoxOffset[ d ] = transformedMovingTileBoxPosition[ d ] - transformedMovingBoxInterval.realMin( d );

		return transformedMovingTileBoxToBoundingBoxOffset;
	}

	/**
	 * Builds the transformation for the error ellipse to map it into the coordinate space of the fixed box.
	 *
	 * @param tileBoxes
	 * @param tileTransforms
	 * @return
	 */
	public static InvertibleRealTransform getErrorEllipseTransform(
			final SubdividedTileBox[] tileBoxes,
			final AffineGet[] tileTransforms )
	{
		validateInputParametersLength( tileBoxes, tileTransforms );

		final InvertibleRealTransform movingTileToFixedBoxTransform = getMovingTileToFixedBoxTransform( tileBoxes, tileTransforms );
		final double[] transformedMovingTileBoxToBoundingBoxOffset = getTransformedMovingBoxToBoundingBoxOffset( tileBoxes, movingTileToFixedBoxTransform );

		final InvertibleRealTransformSequence errorEllipseTransform = new InvertibleRealTransformSequence();
		errorEllipseTransform.add( new Translation( Intervals.minAsDoubleArray( tileBoxes[ movingIndex ] ) ) ); // moving box -> moving tile
		errorEllipseTransform.add( movingTileToFixedBoxTransform ); // moving tile -> fixed box
		errorEllipseTransform.add( new Translation( transformedMovingTileBoxToBoundingBoxOffset ).inverse() ); // transformed box top-left -> bounding box top-left

		return errorEllipseTransform;
	}

	/**
	 * Builds the new transformation for the moving tile based on the new offset of its tile box.
	 *
	 * @param tileBoxes
	 * @param tileTransforms
	 * @param movingBoundingBoxOffset
	 * @return
	 */
	public static AffineGet getNewMovingTileTransform(
			final SubdividedTileBox[] tileBoxes,
			final AffineGet[] tileTransforms,
			final double[] movingBoundingBoxOffset )
	{
		validateInputParametersLength( tileBoxes, tileTransforms );

		final int dim = movingBoundingBoxOffset.length;

		// Resulting offset is between the moving bounding box in the fixed box space.
		// Convert it to the offset between the moving and fixed boxes in the global translated space (where the linear affine component of each tile has been undone).
		final RealTransformSequence offsetToWorldTransform = new RealTransformSequence();
		offsetToWorldTransform.add( new Translation( getTransformedMovingBoxToBoundingBoxOffset( tileBoxes, tileTransforms ) ) ); // bounding box top-left in fixed box space -> transformed top-left in fixed box space
		offsetToWorldTransform.add( new Translation( Intervals.minAsDoubleArray( tileBoxes[ fixedIndex ] ) ) ); // fixed box -> fixed tile
		offsetToWorldTransform.add( tileTransforms[ fixedIndex ] ); // fixed tile -> world

		final double[] newMovingBoxWorldPosition = new double[ dim ];
		offsetToWorldTransform.apply( movingBoundingBoxOffset, newMovingBoxWorldPosition );

		final double[] movingBoxWorldPosition = new double[ dim ];
		tileTransforms[ movingIndex ].apply( Intervals.minAsDoubleArray( tileBoxes[ movingIndex ] ), movingBoxWorldPosition );

		final double[] newMovingBoxWorldOffset = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			newMovingBoxWorldOffset[ d ] = newMovingBoxWorldPosition[ d ] - movingBoxWorldPosition[ d ];

		// new translation component
		final AffineGet movingTileTransformTranslationComponent = TransformUtils.getTranslationalComponent( tileTransforms[ movingIndex ] );
		final double[] newMovingTileTransformTranslationComponent = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			newMovingTileTransformTranslationComponent[ d ] = newMovingBoxWorldOffset[ d ] + movingTileTransformTranslationComponent.get( d, dim );

		// linear component stays the same
		final AffineGet movingTileTransformLinearComponent = TransformUtils.getLinearComponent( tileTransforms[ movingIndex ] );

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
	 * @param tileBoxes
	 * @param tileTransforms
	 * @param movingBoundingBoxOffset
	 * @return
	 */
	public static double[] getNewStitchedOffset(
			final SubdividedTileBox[] tileBoxes,
			final AffineGet[] tileTransforms,
			final double[] movingBoundingBoxOffset )
	{
		validateInputParametersLength( tileBoxes, tileTransforms );

		final int dim = movingBoundingBoxOffset.length;

		final double[] fixedBoxTranslatedPosition = new double[ dim ];
		TransformUtils.undoLinearComponent( tileTransforms[ fixedIndex ] ).apply( Intervals.minAsDoubleArray( tileBoxes[ fixedIndex ] ), fixedBoxTranslatedPosition );

		final double[] newMovingBoxTranslatedPosition = new double[ dim ];
		final AffineGet newMovingTileTransform = getNewMovingTileTransform( tileBoxes, tileTransforms, movingBoundingBoxOffset );
		TransformUtils.undoLinearComponent( newMovingTileTransform ).apply( Intervals.minAsDoubleArray( tileBoxes[ movingIndex ] ), newMovingBoxTranslatedPosition );

		final double[] stitchedOffset = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			stitchedOffset[ d ] = newMovingBoxTranslatedPosition[ d ] - fixedBoxTranslatedPosition[ d ];
		return stitchedOffset;
	}
}
