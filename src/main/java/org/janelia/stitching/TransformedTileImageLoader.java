package org.janelia.stitching;

import java.io.IOException;

import org.janelia.dataaccess.DataProvider;
import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;

import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealConverter;
import net.imglib2.interpolation.randomaccess.ClampingNLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

public class TransformedTileImageLoader
{
	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider ) throws IOException
	{
		return loadTile( tile, dataProvider, null );
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final RandomAccessiblePairNullable< U, U > flatfield ) throws IOException
	{
		final RandomAccessibleInterval< T > img = TileLoader.loadTile( tile, dataProvider );

		final RandomAccessibleInterval< T > source;
		if ( flatfield != null )
		{
			final FlatfieldCorrectedRandomAccessible< T, U > flatfieldCorrectedImg = new FlatfieldCorrectedRandomAccessible<>( img, flatfield.toRandomAccessiblePair() );
			final RandomAccessible< T > correctedConvertedRandomAccessible = Converters.convert( flatfieldCorrectedImg, new RealConverter<>(), Util.getTypeFromInterval( img ) );
			source = Views.interval( correctedConvertedRandomAccessible, img );
		}
		else
		{
			source = img;
		}

		final RandomAccessible< T > extendedImg = Views.extendZero( source );
		final RealRandomAccessible< T > interpolatedImg = Views.interpolate( extendedImg, new ClampingNLinearInterpolatorFactory<>() );
		final AffineGet transform = TileOperations.getTileTransform( tile );
		final RandomAccessible< T > rasteredInterpolatedImg = Views.raster( RealViews.affine( interpolatedImg, transform ) );
		final Interval boundingBox = TileOperations.estimateBoundingBox( tile );
		final RandomAccessibleInterval< T > transformedImg = Views.interval( rasteredInterpolatedImg, boundingBox );
		return transformedImg;
	}
}
