package org.janelia.stitching;

import org.janelia.dataaccess.DataProvider;
import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;

import bdv.img.TpsTransformWrapper;
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
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.TransformBoundingBox;
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
	private static final long[] DEFAULT_PADDING = new long[] { 100, 100, 100 };

	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > loadTile(
			final DataProvider dataProvider,
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform )
	{
		return loadTile( dataProvider, slabMin, slabTile, slabTransform, DEFAULT_PADDING, null );
	}

	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > loadTile(
			final DataProvider dataProvider,
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform,
			final long[] padding )
	{
		return loadTile( dataProvider, slabMin, slabTile, slabTransform, padding, null );
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final DataProvider dataProvider,
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform,
			final RandomAccessiblePairNullable< U, U > flatfield )
	{
		return loadTile( dataProvider, slabMin, slabTile, slabTransform, DEFAULT_PADDING, flatfield );
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final DataProvider dataProvider,
			final double[] slabMin,
			final TileInfo slabTile,
			final TpsTransformWrapper slabTransform,
			final long[] padding,
			final RandomAccessiblePairNullable< U, U > flatfield )
	{
		final RandomAccessibleInterval< T > rawTile = TileLoader.loadTile( slabTile, dataProvider );
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
		if ( !Intervals.equalDimensions( img, new FinalInterval( new FinalDimensions( slabTile.getSize() ) ) ) )
			throw new RuntimeException( "different dimensions: img != slabTile" );

		final InvertibleRealTransform tileTransform = WarpedTileOperations.getTileTransform( slabTile, slabMin, slabTransform );

		final RandomAccessibleInterval< T > zeroMinImg = Views.zeroMin( img );
		final RandomAccessible< T > extendedZeroMinImg = Views.extendZero( zeroMinImg );
		final RealRandomAccessible< T > interpolatedZeroMinImg = Views.interpolate( extendedZeroMinImg, new NLinearInterpolatorFactory<>() );
		final RealRandomAccessible< T > transformedImg = RealViews.transformReal( interpolatedZeroMinImg, tileTransform );
		final RandomAccessible< T > rasteredTransformedImg = Views.raster( transformedImg );

		final RealInterval tileInterval = new FinalRealInterval( Intervals.minAsDoubleArray( zeroMinImg ), Intervals.maxAsDoubleArray( zeroMinImg ) );
		final RealInterval estimatedBoundingBox = TransformBoundingBox.boundingBoxForwardCorners( tileInterval, tileTransform );
		final Interval paddedBoundingBox = TileOperations.padInterval( TileOperations.roundRealInterval( estimatedBoundingBox ), padding );

		return Views.interval( rasteredTransformedImg, paddedBoundingBox );
	}
}
