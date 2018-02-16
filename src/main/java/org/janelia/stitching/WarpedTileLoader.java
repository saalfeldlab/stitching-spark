package org.janelia.stitching;

import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;
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
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealConverter;
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
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

public class WarpedTileLoader
{
	private static final long[] DEFAULT_PADDING = new long[] { 200, 200, 200 };

	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > load(
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform )
	{
		return load( slabMin, slabTile, slabTransform, DEFAULT_PADDING, null );
	}

	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > load(
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform,
			final long[] padding )
	{
		return load( slabMin, slabTile, slabTransform, padding, null );
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > load(
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform,
			final RandomAccessiblePairNullable< U, U > flatfield )
	{
		return load( slabMin, slabTile, slabTransform, DEFAULT_PADDING, flatfield );
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > load(
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform,
			final long[] padding,
			final RandomAccessiblePairNullable< U, U > flatfield )
	{
		final ImagePlus imp = ImageImporter.openImage( slabTile.getFilePath() );
		final RandomAccessibleInterval< T > rawTile = ImagePlusImgs.from( imp );

		final RandomAccessibleInterval< T > source;
		if ( flatfield != null )
		{
			final FlatfieldCorrectedRandomAccessible< T, U > flatfieldCorrectedImg = new FlatfieldCorrectedRandomAccessible<>( rawTile, flatfield.toRandomAccessiblePair() );
			final RandomAccessible< T > correctedConvertedRandomAccessible = Converters.convert( flatfieldCorrectedImg, new RealConverter<>(), Util.getTypeFromInterval( rawTile ) );
			source = Views.interval( correctedConvertedRandomAccessible, rawTile );
		}
		else
		{
			source = rawTile;
		}

		return warp( slabMin, slabTile, source, slabTransform, padding );
	}

	public static Interval getBoundingBox(
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform )
	{
		return getBoundingBox( slabMin, slabTile, slabTransform, DEFAULT_PADDING );
	}

	public static Interval getBoundingBox(
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform,
			final long[] padding )
	{
		final Interval tileInterval = new FinalInterval( new FinalDimensions( slabTile.getSize() ) );
		final RandomAccessibleInterval< ByteType > fakeTileImg = ConstantUtils.constantRandomAccessibleInterval( new ByteType(), slabMin.length, tileInterval );
		return warp( slabMin, slabTile, fakeTileImg, slabTransform, padding );
	}

	private static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > warp(
			final double[] slabMin,
			final TileInfo slabTile,
			final RandomAccessibleInterval< T > img,
			final TpsTransformWrapper slabTransform,
			final long[] padding )
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
		final RealRandomAccessible< T > transformedImgMicrons = RealViews.transformReal( translatedInterpolatedImgMicrons, slabTransform );
		final RealRandomAccessible< T > transformedImgPixels = RealViews.affineReal( transformedImgMicrons, pixelsToMicrons.inverse() );
		final RandomAccessible< T > rasteredTransformedImgPixels = Views.raster( transformedImgPixels );

		final RealInterval estimatedBoundingBoxPhysicalUnits = TransformBoundingBox.boundingBoxForwardCorners( tileSlabPhysicalUnitsInterval, slabTransform );
		final long[] paddedBoundingBoxMin = new long[ slabMin.length ], paddedBoundingBoxMax = new long[ slabMin.length ];
		for ( int d = 0; d < slabMin.length; ++d )
		{
			paddedBoundingBoxMin[ d ] = Math.round( estimatedBoundingBoxPhysicalUnits.realMin( d ) / slabTile.getPixelResolution( d ) ) - padding[ d ] / 2;
			paddedBoundingBoxMax[ d ] = Math.round( estimatedBoundingBoxPhysicalUnits.realMax( d ) / slabTile.getPixelResolution( d ) ) + padding[ d ] / 2 + padding[ d ] % 2;
		}
		final Interval paddedBoundingBox = new FinalInterval( paddedBoundingBoxMin, paddedBoundingBoxMax );

		return Views.interval( rasteredTransformedImgPixels, paddedBoundingBox );
	}
}
