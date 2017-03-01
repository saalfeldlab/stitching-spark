package org.janelia.flatfield;

import org.janelia.stitching.Utils;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
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
	public static < A extends AffineGet & AffineSet, T extends NativeType< T > & RealType< T > > void main( final String[] args )
	{
		final String filepath = args[ 0 ];
		final int lowScale = Integer.parseInt( args[ 1 ] ), restoredScale = Integer.parseInt( args[ 2 ] );

		final ImagePlus imp = IJ.openImage( filepath );
		Utils.workaroundImagePlusNSlices( imp );
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

		final ShiftedDownsampling downsampler = new ShiftedDownsampling( img, downsamplingTransform );
		try ( final ShiftedDownsampling.PixelsMapping pixelsMapping = downsampler.new PixelsMapping( lowScale ) )
		{
			final RandomAccessibleInterval< T > downsampledImg = downsampler.downsampleSolutionComponent( img, pixelsMapping );
			final ImagePlus downsampledImp = ImageJFunctions.wrap( downsampledImg, "" );
			Utils.workaroundImagePlusNSlices( downsampledImp );
			IJ.saveAsTiff( downsampledImp, Utils.addFilenameSuffix( filepath, "_downsampled_" + (1./(1<<lowScale) ) ) );

			try ( final ShiftedDownsampling.PixelsMapping pixelsMappingRestored = downsampler.new PixelsMapping( restoredScale ) )
			{
				final RandomAccessibleInterval< T > referenceImg = downsampler.downsampleSolutionComponent( img, pixelsMappingRestored );
				final ImagePlus referenceImp = ImageJFunctions.wrap( referenceImg, "" );
				Utils.workaroundImagePlusNSlices( referenceImp );
				IJ.saveAsTiff( referenceImp, Utils.addFilenameSuffix( filepath, "_reference_" + (1./(1<<restoredScale) ) ) );

				final RandomAccessible< T > upsampledImg = downsampler.upsample( downsampledImg, restoredScale );
				final RandomAccessibleInterval< T > restoredImg = Views.interval( upsampledImg, new FinalInterval( pixelsMappingRestored.getDimensions() ) );
				final ImagePlus restoredImp = ImageJFunctions.wrap( restoredImg, "" );
				Utils.workaroundImagePlusNSlices( restoredImp );
				IJ.saveAsTiff( restoredImp, Utils.addFilenameSuffix( filepath, "_restored_" + (1./(1<<restoredScale) ) ) );
			}
		}
	}
}
