package org.janelia.flatfield;

public class ShiftedDownsamplingTest
{
	/*@SuppressWarnings("unchecked")
	public static < A extends AffineGet & AffineSet, T extends NativeType< T > & RealType< T > > void main( final String[] args )
	{
		final String filepath = args[ 0 ];

		final ImagePlus imp = ImageImporter.openImage( filepath );
		final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );
		final ShiftedDownsampling< A > downsampler = new ShiftedDownsampling<>( img );

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
	}*/
}
