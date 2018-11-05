package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.janelia.stitching.PointMatchSpatialLayout.SpatialLayout;

import mpicbg.models.IllDefinedDataPointsException;
import mpicbg.models.Model;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import net.imglib2.RealPoint;
import net.imglib2.concatenate.Concatenable;
import net.imglib2.concatenate.PreConcatenable;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.Translation;

public class TileTransformEstimator
{
	private static final double REGULARIZER_LAMBDA = 0.1; // TODO: allow to tweak the regularizer lambda value using a cmd arg
	private static final double EPSILON = 1e-2;

	/**
	 * Estimates an expected affine transformation for a given tile in the following way:
	 * (1) Find affine transformations for subtiles by fitting it to stage->world points of neighboring subtiles
	 * (2) Find affine transformation for the given tile by fitting it to stage->transformed points of its subtiles using the transformations estimated in (1)
	 *
	 * The estimated transformation performs the following mapping: local tile coordinates -> expected world coordinates.
	 *
	 * @param tile
	 * @param neighboringTilesLocator
	 * @param sampleWeightCalculator
	 * @return
	 * @throws IllDefinedDataPointsException
	 * @throws NotEnoughDataPointsException
	 */
	public static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > AffineGet estimateAffineTransformation(
			final TileInfo tile,
			final NeighboringTilesLocator neighboringTilesLocator,
			final SampleWeightCalculator sampleWeightCalculator ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		// (1) Find affine transformations for subtiles by fitting it to stage->world points of neighboring subtiles
		final Map< SubTile, AffineGet > estimatedSubTileTransforms = estimateSubTileTransforms( tile, neighboringTilesLocator, sampleWeightCalculator );

		// (2) Find affine transformation for the given tile by fitting it to stage->transformed points of its subtiles using the transformations estimated in (1)
		return estimateTransformForTile( tile, estimatedSubTileTransforms );
	}

	/**
	 * Estimates an expected affine transformation for each subtile of the given tile by fitting a stage->world transform based on its neighboring tiles.
	 *
	 * @param tile
	 * @param neighboringTilesLocator
	 * @param sampleWeightCalculator
	 * @return
	 * @throws IllDefinedDataPointsException
	 * @throws NotEnoughDataPointsException
	 */
	@SuppressWarnings( { "unchecked", "rawtypes" } )
	static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > Map< SubTile, AffineGet > estimateSubTileTransforms(
			final TileInfo tile,
			final NeighboringTilesLocator neighboringTilesLocator,
			final SampleWeightCalculator sampleWeightCalculator ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		final int[] subTilesGridSize = new int[ tile.numDimensions() ];
		Arrays.fill( subTilesGridSize, neighboringTilesLocator.getSubdivisionGridSize() );
		final List< SubTile > subTiles = SubTileOperations.subdivideTiles( new TileInfo[] { tile }, subTilesGridSize );

		// create point matches for each subtile
		final Map< SubTile, List< PointMatch > > subTilesToPointMatches = new HashMap<>();
		for ( final SubTile subTile : subTiles )
		{
			final List< SubTile > neighboringSubTiles = new ArrayList<>( neighboringTilesLocator.findSubTilesWithinWindow( subTile ) );

			final List< RealPoint > neighboringSubTilesStageMiddlePoints = new ArrayList<>();
			for ( final SubTile neighboringSubTile : neighboringSubTiles )
				neighboringSubTilesStageMiddlePoints.add( new RealPoint( SubTileOperations.getSubTileMiddlePointStagePosition( neighboringSubTile ) ) );

			final double[] neighboringSubTilesWeights = sampleWeightCalculator.calculateSampleWeights(
					new RealPoint( SubTileOperations.getSubTileMiddlePointStagePosition( subTile ) ),
					neighboringSubTilesStageMiddlePoints
				);

			final List< PointMatch > pointMatches = new ArrayList<>();
			for ( int i = 0; i < neighboringSubTiles.size(); ++i )
			{
				final double[] neighboringSubTileMiddlePointStagePosition = SubTileOperations.getSubTileMiddlePointStagePosition( neighboringSubTiles.get( i ) );
				final double[] neighboringSubTileMiddlePointWorldPosition = TransformedTileOperations.transformSubTileMiddlePoint( neighboringSubTiles.get( i ), true );
				final PointMatch pointMatch = new PointMatch(
						new Point( neighboringSubTileMiddlePointStagePosition ),
						new Point( neighboringSubTileMiddlePointWorldPosition ),
						neighboringSubTilesWeights[ i ]
					);
				pointMatches.add( pointMatch );
			}

			subTilesToPointMatches.put( subTile, pointMatches );
		}

		// detect the spatial layout
		final List< SpatialLayout > subTilesSpatialLayouts = new ArrayList<>();
		for ( final List< PointMatch > pointMatches : subTilesToPointMatches.values() )
			subTilesSpatialLayouts.add( PointMatchSpatialLayout.determine( pointMatches ) );
		final SpatialLayout spatialLayout = PointMatchSpatialLayout.pickMostRestrictedLayout( subTilesSpatialLayouts.toArray( new SpatialLayout[ 0 ] ) );

		// create subtile models of appropriate type
		final Map< SubTile, Model< ? > > subTilesToModels = new HashMap<>();
		for ( final SubTile subTile : subTilesToPointMatches.keySet() )
		{
			final Model< ? > model;
			switch ( spatialLayout )
			{
			case Collinear: // translation
				model = TileModelFactory.createTranslationModel( tile.numDimensions() );
				break;
			case Coplanar: // similarity
				model = TileModelFactory.createInterpolatedModel(
						tile.numDimensions(),
						( Model ) TileModelFactory.createSimilarityModel( tile.numDimensions() ),
						( Model ) TileModelFactory.createRigidModel( tile.numDimensions() ),
						REGULARIZER_LAMBDA
					);
				break;
			default: // affine
				model = TileModelFactory.createInterpolatedModel(
						tile.numDimensions(),
						( Model ) TileModelFactory.createAffineModel( tile.numDimensions() ),
						( Model ) TileModelFactory.createRigidModel( tile.numDimensions() ),
						REGULARIZER_LAMBDA
					);
				break;
			}
			subTilesToModels.put( subTile, model );
		}

		// fit the models
		final Map< SubTile, AffineGet > subTilesToEstimatedTransforms = new HashMap<>();
		for ( final SubTile subTile : subTilesToPointMatches.keySet() )
		{
			final Model< ? > model = subTilesToModels.get( subTile );
			final List< PointMatch > pointMatches = subTilesToPointMatches.get( subTile );
			model.fit( pointMatches );
			subTilesToEstimatedTransforms.put( subTile, TransformUtils.getModelTransform( model ) );
		}

		return subTilesToEstimatedTransforms;
	}

	/**
	 * Estimates an expected affine transformation for a given tile with known affine transformations for each subtile.
	 * The estimated transformation performs the following mapping: local tile coordinates -> expected world coordinates.
	 *
	 * @param tile
	 * @param subTileTransforms
	 * @return
	 * @throws IllDefinedDataPointsException
	 * @throws NotEnoughDataPointsException
	 */
	@SuppressWarnings( { "unchecked", "rawtypes" } )
	static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > AffineGet estimateTransformForTile(
			final TileInfo tile,
			final Map< SubTile, AffineGet > subTileTransforms ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		final List< PointMatch > pointMatches = new ArrayList<>();
		for ( final Entry< SubTile, AffineGet > subTileAndTransform : subTileTransforms.entrySet() )
		{
			final SubTile subTile = subTileAndTransform.getKey();
			final AffineGet stageToWorldSubTileTransform = subTileAndTransform.getValue();

			final double[] subTileMiddlePointStagePosition = SubTileOperations.getSubTileMiddlePointStagePosition( subTile );
			final double[] subTileMiddlePointEstimatedWorldPosition = new double[ subTileMiddlePointStagePosition.length ];
			stageToWorldSubTileTransform.apply( subTileMiddlePointStagePosition, subTileMiddlePointEstimatedWorldPosition );

			final PointMatch pointMatch = new PointMatch(
					new Point( subTileMiddlePointStagePosition ),
					new Point( subTileMiddlePointEstimatedWorldPosition )
				);
			pointMatches.add( pointMatch );
		}

		final Model< ? > model = TileModelFactory.createInterpolatedModel(
				tile.numDimensions(),
				( Model ) TileModelFactory.createAffineModel( tile.numDimensions() ),
				( Model ) TileModelFactory.createRigidModel( tile.numDimensions() ),
				REGULARIZER_LAMBDA
			);
		model.fit( pointMatches );

		// the resulting transform does stage->world mapping, convert it to local->world
		final A estimatedLocalToWorldTileTransform = TransformUtils.createTransform( tile.numDimensions() );
		estimatedLocalToWorldTileTransform
			.preConcatenate( new Translation( tile.getStagePosition() ) ) // local->stage
			.preConcatenate( TransformUtils.getModelTransform( model ) ); // stage->world
		return estimatedLocalToWorldTileTransform;
	}
}
