package org.janelia.flatfield;

import java.util.ArrayList;
import java.util.List;

import org.janelia.stitching.Utils;
import org.janelia.util.ImageImporter;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

public class ShiftedDownsamplingTest
{
	@SuppressWarnings("unchecked")
	public static < A extends AffineGet & AffineSet, T extends NativeType< T > & RealType< T > > void main( final String[] args )
	{
		final String filepath = args[ 0 ];

		final ImagePlus imp = ImageImporter.openImage( filepath );
		final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );

		final A downsamplingTransform;
		switch ( img.numDimensions() )
		{
		case 2:
			downsamplingTransform = ( A ) new AffineTransform2D();
			downsamplingTransform.set(
					0.5, 0, -0.5,
					0, 0.5, -0.5
				);
			break;
		case 3:
			downsamplingTransform = ( A ) new AffineTransform3D();
			downsamplingTransform.set(
					0.5, 0, 0, -0.5,
					0, 0.5, 0, -0.5,
					0, 0, 0.5, -0.5
				);
			break;
		default:
			throw new IllegalArgumentException( "Input image has " + img.numDimensions() + " dimensions (only 2d and 3d images are supported)" );
		}

		final ShiftedDownsampling< A > downsampler = new ShiftedDownsampling<>( img, downsamplingTransform );

		final int restoredScale = 0;
		final List< RandomAccessibleInterval< T > > restoredImgs = new ArrayList<>();
		try ( final ShiftedDownsampling< A >.PixelsMapping pixelsMappingRestored = downsampler.new PixelsMapping( restoredScale ) )
		{
			for ( int lowScale = 0; lowScale < downsampler.getNumScales(); ++lowScale )
			{
				System.out.println( "Processing scale " + lowScale );
				try ( final ShiftedDownsampling< A >.PixelsMapping pixelsMapping = downsampler.new PixelsMapping( lowScale ) )
				{
					final RandomAccessibleInterval< T > downsampledImg = downsampler.downsampleSolutionComponent( img, pixelsMapping );
					final RealRandomAccessible< T > upsampledImg = downsampler.upsample( downsampledImg, restoredScale );
					final RandomAccessibleInterval< T > restoredImg = Views.interval( Views.raster( upsampledImg ), new FinalInterval( pixelsMappingRestored.getDimensions() ) );
					restoredImgs.add( restoredImg );
				}
			}
		}

		final RandomAccessibleInterval< T > restoredImgsStack = Views.stack( restoredImgs );
		final ImagePlus restoredStackImp = ImageJFunctions.wrap( restoredImgsStack, "" );
		Utils.workaroundImagePlusNSlices( restoredStackImp );

		new ImageJ();
		restoredStackImp.show();
	}
}
