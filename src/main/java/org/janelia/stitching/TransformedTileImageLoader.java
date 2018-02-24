package org.janelia.stitching;

public class TransformedTileImageLoader
{
	/*public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider )
	{
		return loadTile( tile, dataProvider, null );
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final RandomAccessiblePairNullable< U, U > flatfield )
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

		final RandomAccessible< T > extendedImg = Views.extendBorder( source );
		final RealRandomAccessible< T > interpolatedImg = Views.interpolate( extendedImg, new NLinearInterpolatorFactory<>() );
		final AffineGet transform = TileOperations.getTileTransform( tile );
		final RandomAccessible< T > rasteredInterpolatedImg = Views.raster( RealViews.affine( interpolatedImg, transform ) );
		final Interval boundingBox = TileOperations.estimateBoundingBox( tile );
		final RandomAccessibleInterval< T > transformedImg = Views.interval( rasteredInterpolatedImg, boundingBox );
		return transformedImg;
	}*/
}
