package org.janelia.stitching;

import java.util.Arrays;
import java.util.Set;

import org.janelia.stitching.TileSearchRadiusEstimator.EstimatedWorldSearchRadius;
import org.janelia.stitching.TileSearchRadiusEstimator.NotEnoughNeighboringTilesException;

import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.concatenate.Concatenable;
import net.imglib2.concatenate.PreConcatenable;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.Translation;
import net.imglib2.realtransform.TranslationGet;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class TransformedTileOperations
{
	/**
	 * Returns tile transform, that is its affine transform if not null, or translation transform to the stage position of the tile otherwise (if allowed by the boolean flag).
	 *
	 * @param tile
	 * @param worldTransformOnly
	 * @return
	 */
	public static AffineGet getTileTransform( final TileInfo tile, final boolean worldTransformOnly )
	{
		final AffineGet ret;
		if ( tile.getTransform() != null )
			ret = tile.getTransform();
		else if ( !worldTransformOnly )
			ret = new Translation( tile.getStagePosition() );
		else
			ret = null;
		return ret;
	}

	/**
	 * Estimates the bounding box of a transformed (or shifted) tile.
	 *
	 * @param tile
	 * @param worldTransformOnly
	 * @return
	 */
	public static Interval getTransformedBoundingBox( final TileInfo tile, final boolean worldTransformOnly )
	{
		final AffineGet tileTransform = getTileTransform( tile, worldTransformOnly );
		return tileTransform != null ? getTransformedBoundingBox( new FinalInterval( tile.getSize() ), tileTransform ) : null;
	}

	/**
	 * Estimates the bounding box of a transformed interval inside a tile.
	 *
	 * @param interval
	 * @param transform
	 * @return
	 */
	public static Interval getTransformedBoundingBox( final RealInterval interval, final InvertibleRealTransform transform )
	{
		return Intervals.smallestContainingInterval( getTransformedBoundingBoxReal( interval, transform ) );
	}

	/**
	 * Estimates the bounding box of a transformed interval inside a tile.
	 *
	 * @param interval
	 * @param transform
	 * @return
	 */
	public static RealInterval getTransformedBoundingBoxReal( final RealInterval interval, final InvertibleRealTransform transform )
	{
		final double[] transformedMin = new double[ interval.numDimensions() ], transformedMax = new double[ interval.numDimensions() ];
		Arrays.fill( transformedMin, Double.POSITIVE_INFINITY );
		Arrays.fill( transformedMax, Double.NEGATIVE_INFINITY );

		final double[] cornerPosition = new double[ interval.numDimensions() ], transformedCornerPosition = new double[ interval.numDimensions() ];

		final int[] cornerDimensions = new int[ interval.numDimensions() ];
		Arrays.fill( cornerDimensions, 2 );
		final IntervalIterator cornerIterator = new IntervalIterator( cornerDimensions );

		while ( cornerIterator.hasNext() )
		{
			cornerIterator.fwd();
			for ( int d = 0; d < interval.numDimensions(); ++d )
				cornerPosition[ d ] = cornerIterator.getIntPosition( d ) == 0 ? interval.realMin( d ) : interval.realMax( d );

			transform.apply( cornerPosition, transformedCornerPosition );

			for ( int d = 0; d < interval.numDimensions(); ++d )
			{
				transformedMin[ d ] = Math.min( transformedCornerPosition[ d ], transformedMin[ d ] );
				transformedMax[ d ] = Math.max( transformedCornerPosition[ d ], transformedMax[ d ] );
			}
		}

		return new FinalRealInterval( transformedMin, transformedMax );
	}

//	/**
//	 * Estimates an expected affine transformation for a given tile based on offset statistics selected from local neighborhood.
//	 * The estimated transformation performs the following mapping: local tile coordinates -> expected world coordinates.
//	 *
//	 * @param tile
//	 * @param searchRadiusEstimator
//	 * @return
//	 * @throws PipelineExecutionException
//	 * @throws NotEnoughNeighboringTilesException
//	 */
//	public static AffineGet estimateAffineTransformation(
//			final TileInfo tile,
//			final TileSearchRadiusEstimator searchRadiusEstimator ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
//	{
//		return estimateAffineTransformation(
//				estimateLinearAndTranslationAffineComponents( tile, searchRadiusEstimator )
//			);
//	}
	/**
	 * Estimates an expected affine transformation for a given tile in the following way:
	 * (1) Find affine transformations for subtiles by fitting it to local->world points of neighboring subtiles
	 * (2) Find affine transformation for the given tile by fitting it to local->transformed points of its subtiles using the transformations estimated in (1)
	 *
	 * The estimated transformation performs the following mapping: local tile coordinates -> expected world coordinates.
	 *
	 * @param tile
	 * @param searchRadiusEstimator
	 * @return
	 * @throws PipelineExecutionException
	 * @throws NotEnoughNeighboringTilesException
	 */
	public static AffineGet estimateAffineTransformation(
			final TileInfo tile,
			final TileSearchRadiusEstimator searchRadiusEstimator ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		return estimateAffineTransformation(
				estimateLinearAndTranslationAffineComponents( tile, searchRadiusEstimator )
			);
	}

	/**
	 * Estimates an expected affine transformation for a given tile based on offset statistics selected from local neighborhood.
	 * The estimated transformation performs the following mapping: local tile coordinates -> expected world coordinates.
	 *
	 * @param estimatedAffineLinearComponent
	 * @param estimatedAffineTranslationComponent
	 * @return
	 * @throws PipelineExecutionException
	 */
	public static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > AffineGet estimateAffineTransformation(
			final Pair< AffineGet, TranslationGet > estimatedAffineLinearAndTranslationComponents ) throws PipelineExecutionException
	{
		final int dim = estimatedAffineLinearAndTranslationComponents.getA().numDimensions();
		final A estimatedAffine = TransformUtils.createTransform( dim );
		// Combine the transformations in an 'inverse' way: A=LT.
		// This is because the translational component is estimated in the 'offset' space where the linear component has been undone,
		// so the resulting transformation is built by applying the linear part to the translational part
		estimatedAffine.concatenate( estimatedAffineLinearAndTranslationComponents.getA() );
		estimatedAffine.concatenate( estimatedAffineLinearAndTranslationComponents.getB() );
		return estimatedAffine;
	}

	/**
	 * Estimates linear and translation components of the expected affine transformation for a given tile
	 * based on offset statistics selected from local neighborhood.
	 *
	 * @param tile
	 * @param searchRadiusEstimator
	 * @return
	 * @throws PipelineExecutionException
	 * @throws NotEnoughNeighboringTilesException
	 */
	public static Pair< AffineGet, TranslationGet > estimateLinearAndTranslationAffineComponents(
			final TileInfo tile,
			final TileSearchRadiusEstimator searchRadiusEstimator ) throws PipelineExecutionException, NotEnoughNeighboringTilesException
	{
		final EstimatedWorldSearchRadius estimatedSearchRadius = searchRadiusEstimator.estimateSearchRadiusWithinWindow( tile );
		final Set< TileInfo > neighboringTiles = estimatedSearchRadius.neighboringTiles;

		final double[][] expectedLinearAffineMatrix = new double[ tile.numDimensions() ][ tile.numDimensions() + 1 ];
		for ( final TileInfo neighboringTile : neighboringTiles )
		{
			final AffineGet neighboringTileTransform = getTileTransform( neighboringTile, true );
			for ( int dRow = 0; dRow < tile.numDimensions(); ++dRow )
				for ( int dCol = 0; dCol < tile.numDimensions(); ++dCol )
					expectedLinearAffineMatrix[ dRow ][ dCol ] += neighboringTileTransform.get( dRow, dCol );
		}
		for ( int dRow = 0; dRow < tile.numDimensions(); ++dRow )
			for ( int dCol = 0; dCol < tile.numDimensions(); ++dCol )
				expectedLinearAffineMatrix[ dRow ][ dCol ] /= neighboringTiles.size();

		final double[] expectedTranslationComponentVector = new double[ tile.numDimensions() ];
		for ( int d = 0; d < tile.numDimensions(); ++d )
			expectedTranslationComponentVector[ d ] = tile.getStagePosition( d ) + estimatedSearchRadius.errorEllipse.getOffsetsMeanValues()[ d ];

		return new ValuePair<>(
				TransformUtils.createTransform( expectedLinearAffineMatrix ),
				new Translation( expectedTranslationComponentVector )
			);
	}

	/**
	 * Returns transformed intervals for a pair of subtiles.
	 *
	 * @param subTilePair
	 * @param worldTransformOnly
	 * @return
	 */
	public static Pair< Interval, Interval > transformSubTilePair( final SubTilePair subTilePair, final boolean worldTransformOnly )
	{
		return new ValuePair<>(
				transformSubTile( subTilePair.getA(), worldTransformOnly ),
				transformSubTile( subTilePair.getB(), worldTransformOnly )
			);
	}

	/**
	 * Returns transformed subtile interval in the global space.
	 * The center coordinate of the resulting interval is defined by transforming the middle point of the subtile.
	 *
	 * @param subTile
	 * @param worldTransformOnly
	 * @return
	 */
	public static Interval transformSubTile( final SubTile subTile, final boolean worldTransformOnly )
	{
		final AffineGet tileTransform = getTileTransform( subTile.getFullTile(), worldTransformOnly );
		return tileTransform != null ? transformSubTile( subTile, tileTransform ) : null;
	}

	/**
	 * Returns transformed subtile interval in the global space.
	 * The center coordinate of the resulting interval is defined by transforming the middle point of the subtile.
	 *
	 * @param subTile
	 * @param fullTileTransform
	 * @return
	 */
	public static Interval transformSubTile( final SubTile subTile, final RealTransform fullTileTransform )
	{
		final RealInterval transformedSubTileInterval = SubTileOperations.getSubTileInterval(
				transformSubTileMiddlePoint( subTile, fullTileTransform ),
				Intervals.dimensionsAsLongArray( subTile )
			);
		return TileOperations.roundRealInterval( transformedSubTileInterval );
	}

	/**
	 * Transforms the middle point of the given subtile.
	 *
	 * @param subTile
	 * @param worldTransformOnly
	 * @return
	 */
	public static double[] transformSubTileMiddlePoint( final SubTile subTile, final boolean worldTransformOnly )
	{
		final AffineGet tileTransform = getTileTransform( subTile.getFullTile(), worldTransformOnly );
		return tileTransform != null ? transformSubTileMiddlePoint( subTile, tileTransform ) : null;
	}

	/**
	 * Transforms middle point of a given tile box.
	 *
	 * @param subTile
	 * @param fullTileTransform
	 * @return
	 */
	public static double[] transformSubTileMiddlePoint( final SubTile subTile, final RealTransform fullTileTransform )
	{
		final double[] transformedSubTileMiddlePoint = new double[ subTile.numDimensions() ];
		fullTileTransform.apply( SubTileOperations.getSubTileMiddlePoint( subTile ), transformedSubTileMiddlePoint );
		return transformedSubTileMiddlePoint;
	}
}
