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
	 * Returns new error ellipse that is a combination of error ellipses for the fixed subtile and the moving subtile.
	 *
	 * @param searchRadiusEstimator
	 * @param subTiles
	 * @return
	 * @throws PipelineExecutionException
	 * @throws NotEnoughNeighboringTilesException
	 */
	public static EstimatedRelativeSearchRadius getCombinedSearchRadiusForMovingSubTile(
			final SubTile[] subTiles,
			final TileSearchRadiusEstimator searchRadiusEstimator ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		validateInputParametersLength( subTiles );

		final EstimatedWorldSearchRadius[] searchRadiusStats = new EstimatedWorldSearchRadius[ subTiles.length ];
		for ( int i = 0; i < subTiles.length; ++i )
			searchRadiusStats[ i ] = searchRadiusEstimator.estimateSearchRadiusWithinWindow( subTiles[ i ].getFullTile() );

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
	 * Builds the new transformation for the moving tile based on the new offset of its subtile.
	 *
	 * @param subTiles
	 * @param tileTransforms
	 * @param movingBoundingBoxOffset
	 * @return
	 */
	public static AffineGet getNewMovingTileTransform(
			final SubTile[] subTiles,
			final AffineGet[] tileTransforms,
			final double[] movingBoundingBoxOffset )
	{
		validateInputParametersLength( subTiles, tileTransforms );

		final int dim = movingBoundingBoxOffset.length;

		// Resulting offset is for the moving bounding box in the fixed subtile space.
		// Convert it to the offset between the moving and fixed subtiles in the global translated space (where the linear affine component of each tile has been undone).
		final RealTransformSequence offsetToWorldTransform = new RealTransformSequence();
		offsetToWorldTransform.add( new Translation( getTransformedMovingSubTileToBoundingBoxOffset( subTiles, tileTransforms ) ) ); // bounding box top-left in fixed subtile space -> transformed top-left in fixed subtile space
		offsetToWorldTransform.add( new Translation( Intervals.minAsDoubleArray( subTiles[ fixedIndex ] ) ) ); // fixed subtile -> fixed tile
		offsetToWorldTransform.add( tileTransforms[ fixedIndex ] ); // fixed tile -> world

		final double[] newMovingSubTileWorldPosition = new double[ dim ];
		offsetToWorldTransform.apply( movingBoundingBoxOffset, newMovingSubTileWorldPosition );

		final double[] movingSubTileWorldPosition = new double[ dim ];
		tileTransforms[ movingIndex ].apply( Intervals.minAsDoubleArray( subTiles[ movingIndex ] ), movingSubTileWorldPosition );

		final double[] newMovingSubTileWorldOffset = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			newMovingSubTileWorldOffset[ d ] = newMovingSubTileWorldPosition[ d ] - movingSubTileWorldPosition[ d ];

		// new translation component
		final AffineGet movingTileTransformTranslationComponent = TransformUtils.getTranslationalComponent( tileTransforms[ movingIndex ] );
		final double[] newMovingTileTransformTranslationComponent = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			newMovingTileTransformTranslationComponent[ d ] = newMovingSubTileWorldOffset[ d ] + movingTileTransformTranslationComponent.get( d, dim );

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
	 * Returns transformed offset between the fixed subtile and the moving subtile at its new position.
	 *
	 * @param subTiles
	 * @param tileTransforms
	 * @param movingBoundingBoxOffset
	 * @return
	 */
	public static double[] getNewStitchedOffset(
			final SubTile[] subTiles,
			final AffineGet[] tileTransforms,
			final double[] movingBoundingBoxOffset )
	{
		validateInputParametersLength( subTiles, tileTransforms );

		final int dim = movingBoundingBoxOffset.length;

		final double[] fixedSubTileTranslatedPosition = new double[ dim ];
		TransformUtils.undoLinearComponent( tileTransforms[ fixedIndex ] ).apply( Intervals.minAsDoubleArray( subTiles[ fixedIndex ] ), fixedSubTileTranslatedPosition );

		final double[] newMovingSubTileTranslatedPosition = new double[ dim ];
		final AffineGet newMovingTileTransform = getNewMovingTileTransform( subTiles, tileTransforms, movingBoundingBoxOffset );
		TransformUtils.undoLinearComponent( newMovingTileTransform ).apply( Intervals.minAsDoubleArray( subTiles[ movingIndex ] ), newMovingSubTileTranslatedPosition );

		final double[] stitchedOffset = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
			stitchedOffset[ d ] = newMovingSubTileTranslatedPosition[ d ] - fixedSubTileTranslatedPosition[ d ];
		return stitchedOffset;
	}
}
