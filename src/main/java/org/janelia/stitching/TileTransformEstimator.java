package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	/**
	 * Estimates an expected affine transformation for a given tile in the following way:
	 * (1) Find affine transformations for subtiles by fitting it to stage->world points of neighboring subtiles
	 * (2) Find affine transformation for the given tile by fitting it to stage->transformed points of its subtiles using the transformations estimated in (1)
	 *
	 * The estimated transformation performs the following mapping: local tile coordinates -> expected world coordinates.
	 *
	 * @param tile
	 * @param neighboringTilesLocator
	 * @return
	 */
	public static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > AffineGet estimateAffineTransformation(
			final TileInfo tile,
			final NeighboringTilesLocator neighboringTilesLocator,
			final SampleWeightCalculator sampleWeightCalculator ) throws PipelineExecutionException, NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		// (1) Find affine transformations for subtiles by fitting it to stage->world points of neighboring subtiles
		final int[] subTilesGridSize = new int[ tile.numDimensions() ];
		Arrays.fill( subTilesGridSize, neighboringTilesLocator.getSubdivisionGridSize() );
		final List< SubTile > subTiles = SubTileOperations.subdivideTiles( new TileInfo[] { tile }, subTilesGridSize );
		final Map< SubTile, AffineGet > estimatedSubTileTransforms = new HashMap<>();
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

			final Model< ? > mainModel = TileModelFactory.createAffineModel( tile.numDimensions() );
			final Model< ? > regularizer = TileModelFactory.createRigidModel( tile.numDimensions() );
			@SuppressWarnings( { "unchecked", "rawtypes" } )
			final Model< ? > model = TileModelFactory.createInterpolatedModel(
					tile.numDimensions(),
					( Model ) mainModel,
					( Model ) regularizer,
					0.1 // TODO: allow to tweak regularizer lambda value by cmd arg
				);

			model.fit( pointMatches );
			estimatedSubTileTransforms.put( subTile, TransformUtils.getModelTransform( model ) );
		}

		// (2) Find affine transformation for the given tile by fitting it to stage->transformed points of its subtiles using the transformations estimated in (1)
		final List< PointMatch > pointMatches = new ArrayList<>();
		for ( final Entry< SubTile, AffineGet > subTileWithEstimatedTransform : estimatedSubTileTransforms.entrySet() )
		{
			final SubTile subTile = subTileWithEstimatedTransform.getKey();
			final AffineGet estimatedStageToWorldSubTileTransform = subTileWithEstimatedTransform.getValue();

			final double[] subTileMiddlePointStagePosition = SubTileOperations.getSubTileMiddlePointStagePosition( subTile );
			final double[] subTileMiddlePointEstimatedWorldPosition = new double[ subTileMiddlePointStagePosition.length ];
			estimatedStageToWorldSubTileTransform.apply( subTileMiddlePointStagePosition, subTileMiddlePointEstimatedWorldPosition );

			final PointMatch pointMatch = new PointMatch(
					new Point( subTileMiddlePointStagePosition ),
					new Point( subTileMiddlePointEstimatedWorldPosition )
				);
			pointMatches.add( pointMatch );
		}

		final Model< ? > mainModel = TileModelFactory.createAffineModel( tile.numDimensions() );
		final Model< ? > regularizer = TileModelFactory.createRigidModel( tile.numDimensions() );
		@SuppressWarnings( { "unchecked", "rawtypes" } )
		final Model< ? > model = TileModelFactory.createInterpolatedModel(
				tile.numDimensions(),
				( Model ) mainModel,
				( Model ) regularizer,
				0.1 // TODO: allow to tweak regularizer lambda value by cmd arg
			);

		try
		{
			model.fit( pointMatches );
		}
		catch ( final NotEnoughDataPointsException | IllDefinedDataPointsException e )
		{
			throw new PipelineExecutionException( e );
		}

		// the resulting transform does stage->world mapping, convert it to local->world
		final A estimatedLocalToWorldTileTransform = TransformUtils.createTransform( tile.numDimensions() );
		estimatedLocalToWorldTileTransform
			.preConcatenate( new Translation( tile.getStagePosition() ) ) // local->stage
			.preConcatenate( TransformUtils.getModelTransform( model ) ); // stage->world
		return estimatedLocalToWorldTileTransform;
	}
}
