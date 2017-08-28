package org.janelia.stitching;

import org.janelia.util.ImageImporter;

import ij.ImagePlus;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

public class TransformedTileImageLoader
{
	// TODO: add support for flatfield correction
	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > load( final TileInfo tile )
	{
		final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
		final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );
		final RandomAccessible< T > extendedImg = Views.extendBorder( img );
		final RealRandomAccessible< T > interpolatedImg = Views.interpolate( extendedImg, new NLinearInterpolatorFactory<>() );
		final AffineGet transform = tile.getTransform() != null ? tile.getTransform() : new Translation( tile.getPosition() );
		final RandomAccessible< T > rasteredInterpolatedImg = Views.raster( RealViews.affine( interpolatedImg, transform ) );
		final Interval boundingBox = TileOperations.estimateBoundingBox( tile );
		final RandomAccessibleInterval< T > transformedImg = Views.interval( rasteredInterpolatedImg, boundingBox );
		return transformedImg;
	}
}
