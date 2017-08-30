package org.janelia.stitching;

import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;
import org.janelia.util.ImageImporter;

import ij.ImagePlus;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealConverter;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

public class TransformedTileImageLoader
{
	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > load( final TileInfo tile )
	{
		return load( tile, null );
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > load(
			final TileInfo tile,
			final RandomAccessiblePairNullable< U, U > flatfield )
	{
		final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
		final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );

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
		final AffineGet transform = tile.getTransform() != null ? tile.getTransform() : new Translation( tile.getPosition() );
		final RandomAccessible< T > rasteredInterpolatedImg = Views.raster( RealViews.affine( interpolatedImg, transform ) );
		final Interval boundingBox = TileOperations.estimateBoundingBox( tile );
		final RandomAccessibleInterval< T > transformedImg = Views.interval( rasteredInterpolatedImg, boundingBox );
		return transformedImg;
	}
}
