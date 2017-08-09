package org.janelia.stitching;

import org.janelia.util.ImageImporter;

import bdv.img.TpsTransformWrapper;
import ij.ImagePlus;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Scale;
import net.imglib2.realtransform.TransformBoundingBox;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class WarpedTileLoader
{
	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > load( final double[] slabMin, final TileInfo slabTile, final TpsTransformWrapper transform )
	{
		final double[] tileSlabMinPhysicalUnits = new double[ slabMin.length ], tileSlabMaxPhysicalUnits = new double[ slabMin.length ];
		for ( int d = 0; d < slabMin.length; ++d )
		{
			tileSlabMinPhysicalUnits[ d ] = ( slabTile.getPosition( d ) - slabMin[ d ] ) * slabTile.getPixelResolution( d );
			tileSlabMaxPhysicalUnits[ d ] = ( slabTile.getPosition( d ) - slabMin[ d ] + slabTile.getSize( d ) - 1) * slabTile.getPixelResolution( d );
		}
		final RealInterval tileSlabPhysicalUnitsInterval = new FinalRealInterval( tileSlabMinPhysicalUnits, tileSlabMaxPhysicalUnits );

		final Scale pixelsToMicrons = new Scale( slabTile.getPixelResolution() );
		final Translation slabTranslationMicrons = new Translation( Intervals.minAsDoubleArray( tileSlabPhysicalUnitsInterval ) );

		final ImagePlus imp = ImageImporter.openImage( slabTile.getFilePath() );
		final RandomAccessibleInterval< T > rawTile = ImagePlusImgs.from( imp );
		final RandomAccessible< T > extendedTile = Views.extendZero( rawTile );
		final RealRandomAccessible< T > interpolatedTile = Views.interpolate( extendedTile, new NLinearInterpolatorFactory<>() );
		final RealRandomAccessible< T > interpolatedTileMicrons = RealViews.affineReal( interpolatedTile, pixelsToMicrons );
		final RealRandomAccessible< T > translatedInterpolatedTileMicrons = RealViews.affineReal( interpolatedTileMicrons, slabTranslationMicrons );
		final RealRandomAccessible< T > transformedTileMicrons = RealViews.transformReal( translatedInterpolatedTileMicrons, transform );
		final RealRandomAccessible< T > transformedTilePixels = RealViews.affineReal( transformedTileMicrons, pixelsToMicrons.inverse() );
		final RandomAccessible< T > rasteredTransformedTilePixels = Views.raster( transformedTilePixels );

		final RealInterval estimatedBoundingBoxPhysicalUnits = TransformBoundingBox.boundingBoxForwardCorners( tileSlabPhysicalUnitsInterval, transform );
		final long[] estimatedBoundingBoxMin = new long[ slabMin.length ], estimatedBoundingBoxMax = new long[ slabMin.length ];
		for ( int d = 0; d < slabMin.length; ++d )
		{
			estimatedBoundingBoxMin[ d ] = Math.round( estimatedBoundingBoxPhysicalUnits.realMin( d ) / slabTile.getPixelResolution( d ) );
			estimatedBoundingBoxMax[ d ] = Math.round( estimatedBoundingBoxPhysicalUnits.realMax( d ) / slabTile.getPixelResolution( d ) );
		}
		final Interval estimatedBoundingBox = new FinalInterval( estimatedBoundingBoxMin, estimatedBoundingBoxMax );

		return Views.interval( rasteredTransformedTilePixels, estimatedBoundingBox );
	}
}
