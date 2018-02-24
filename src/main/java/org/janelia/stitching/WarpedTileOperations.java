package org.janelia.stitching;

import bdv.img.TpsTransformWrapper;
import net.imglib2.Interval;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.realtransform.Scale;
import net.imglib2.realtransform.Translation;

public class WarpedTileOperations
{
	/**
	 * Returns a warping transformation for the given tile.
	 */
	public static InvertibleRealTransform getTileTransform( final TileInfo slabTile, final TileSlabMapping tileSlabMapping )
	{
		final String slab = tileSlabMapping.getSlab( slabTile );
		return getTileTransform( slabTile, tileSlabMapping.getSlabMin( slab ), tileSlabMapping.getSlabTransform( slab ) );
	}

	/**
	 * Returns a warping transformation for the given tile.
	 */
	public static InvertibleRealTransform getTileTransform( final TileInfo slabTile, final double[] slabMin, final TpsTransformWrapper slabTransform )
	{
		final double[] tileSlabPositionMicrons = new double[ slabMin.length ];
		for ( int d = 0; d < slabMin.length; ++d )
			tileSlabPositionMicrons[ d ] = ( slabTile.getPosition( d ) - slabMin[ d ] ) * slabTile.getPixelResolution( d );

		final Scale pixelsToMicrons = new Scale( slabTile.getPixelResolution() );
		final Translation slabTranslationMicrons = new Translation( tileSlabPositionMicrons );

		final InvertibleRealTransformSequence tileTransform = new InvertibleRealTransformSequence();
		tileTransform.add( pixelsToMicrons );
		tileTransform.add( slabTranslationMicrons );
		tileTransform.add( slabTransform );
		tileTransform.add( pixelsToMicrons.inverse() );

		return tileTransform;
	}

	/**
	 * Estimates the bounding box of a transformed tile.
	 */
	public static Interval estimateBoundingBox( final TileInfo slabTile, final TileSlabMapping tileSlabMapping )
	{
		final String slab = tileSlabMapping.getSlab( slabTile );
		return estimateBoundingBox( slabTile, tileSlabMapping.getSlabMin( slab ), tileSlabMapping.getSlabTransform( slab ) );
	}

	/**
	 * Estimates the bounding box of a transformed tile.
	 */
	public static Interval estimateBoundingBox( final TileInfo slabTile, final double[] slabMin, final TpsTransformWrapper slabTransform )
	{
		return WarpedTileLoader.getBoundingBox( slabMin, slabTile, slabTransform );
	}
}