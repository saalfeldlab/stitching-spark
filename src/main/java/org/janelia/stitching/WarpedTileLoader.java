package org.janelia.stitching;

import org.janelia.util.ImageImporter;

import bdv.img.TpsTransformWrapper;
import ij.ImagePlus;
import net.imglib2.FinalDimensions;
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
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.util.ConstantUtils;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class WarpedTileLoader
{
	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > load( final double[] slabMin, final TileInfo slabTile, final TpsTransformWrapper transform )
	{
		final ImagePlus imp = ImageImporter.openImage( slabTile.getFilePath() );
		final RandomAccessibleInterval< T > rawTile = ImagePlusImgs.from( imp );
		return warp( slabMin, slabTile, rawTile, transform );
	}

	public static Interval getBoundingBox( final double[] slabMin, final TileInfo slabTile, final TpsTransformWrapper transform )
	{
		final Interval tileInterval = new FinalInterval( new FinalDimensions( slabTile.getSize() ) );
		final RandomAccessibleInterval< ByteType > fakeTileImg = ConstantUtils.constantRandomAccessibleInterval( new ByteType(), slabMin.length, tileInterval );
		return warp( slabMin, slabTile, fakeTileImg, transform );
	}

	private static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > warp( final double[] slabMin, final TileInfo slabTile, final RandomAccessibleInterval< T > img, final TpsTransformWrapper transform )
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

		final RandomAccessible< T > extendedImg = Views.extendZero( img );
		final RealRandomAccessible< T > interpolatedImg = Views.interpolate( extendedImg, new NLinearInterpolatorFactory<>() );
		final RealRandomAccessible< T > interpolatedImgMicrons = RealViews.affineReal( interpolatedImg, pixelsToMicrons );
		final RealRandomAccessible< T > translatedInterpolatedImgMicrons = RealViews.affineReal( interpolatedImgMicrons, slabTranslationMicrons );
		final RealRandomAccessible< T > transformedImgMicrons = RealViews.transformReal( translatedInterpolatedImgMicrons, transform );
		final RealRandomAccessible< T > transformedImgPixels = RealViews.affineReal( transformedImgMicrons, pixelsToMicrons.inverse() );
		final RandomAccessible< T > rasteredTransformedImgPixels = Views.raster( transformedImgPixels );

		final RealInterval estimatedBoundingBoxPhysicalUnits = TransformBoundingBox.boundingBoxForwardCorners( tileSlabPhysicalUnitsInterval, transform );
		final long[] estimatedBoundingBoxMin = new long[ slabMin.length ], estimatedBoundingBoxMax = new long[ slabMin.length ];
		for ( int d = 0; d < slabMin.length; ++d )
		{
			estimatedBoundingBoxMin[ d ] = Math.round( estimatedBoundingBoxPhysicalUnits.realMin( d ) / slabTile.getPixelResolution( d ) );
			estimatedBoundingBoxMax[ d ] = Math.round( estimatedBoundingBoxPhysicalUnits.realMax( d ) / slabTile.getPixelResolution( d ) );
		}
		final Interval estimatedBoundingBox = new FinalInterval( estimatedBoundingBoxMin, estimatedBoundingBoxMax );

		return Views.interval( rasteredTransformedImgPixels, estimatedBoundingBox );
	}
}
