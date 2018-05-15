package org.janelia.stitching;

import java.io.IOException;
import java.util.Optional;

import org.janelia.dataaccess.DataProvider;
import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealConverter;
import net.imglib2.interpolation.randomaccess.ClampingNLinearInterpolatorFactory;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

public class TransformedTileImageLoader
{
	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final Optional< RandomAccessiblePairNullable< U, U > > flatfield ) throws IOException
	{
		return loadTile(
				tile,
				dataProvider,
				flatfield,
				new FinalInterval( tile.getSize() )
			);
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final Optional< RandomAccessiblePairNullable< U, U > > flatfield,
			final Interval tileInterval ) throws IOException
	{
		return loadTile(
				tile,
				dataProvider,
				flatfield,
				tileInterval,
				TileOperations.getTileTransform( tile )
			);
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final Optional< RandomAccessiblePairNullable< U, U > > flatfield,
			final InvertibleRealTransform tileTransform ) throws IOException
	{
		return loadTile(
				tile,
				dataProvider,
				flatfield,
				new FinalInterval( tile.getSize() ),
				tileTransform
			);
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final Optional< RandomAccessiblePairNullable< U, U > > flatfield,
			final Interval tileInterval,
			final InvertibleRealTransform tileTransform ) throws IOException
	{
		final RandomAccessibleInterval< T > rawImg = TileLoader.loadTile( tile, dataProvider );
		final RandomAccessibleInterval< T > source;
		if ( flatfield.isPresent() )
		{
			final FlatfieldCorrectedRandomAccessible< T, U > flatfieldCorrectedImg = new FlatfieldCorrectedRandomAccessible<>( rawImg, flatfield.get().toRandomAccessiblePair() );
			final RandomAccessible< T > correctedConvertedRandomAccessible = Converters.convert( flatfieldCorrectedImg, new RealConverter<>(), Util.getTypeFromInterval( rawImg ) );
			source = Views.interval( correctedConvertedRandomAccessible, tileInterval );
		}
		else
		{
			source = Views.interval( rawImg, tileInterval );
		}

		final RandomAccessible< T > extendedSource = Views.extendZero( source );
		final RealRandomAccessible< T > interpolatedSource = Views.interpolate( extendedSource, new ClampingNLinearInterpolatorFactory<>() );
		final RandomAccessible< T > transformedSource = RealViews.transform( interpolatedSource, tileTransform );
		final Interval boundingBox = TileOperations.getTransformedBoundingBox( source, tileTransform );
		final RandomAccessibleInterval< T > transformedImg = Views.interval( transformedSource, boundingBox );

		return transformedImg;
	}
}
