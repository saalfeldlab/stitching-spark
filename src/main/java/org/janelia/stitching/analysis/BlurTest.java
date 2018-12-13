package org.janelia.stitching.analysis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.janelia.stitching.Boundaries;
import org.janelia.stitching.ImageType;
import org.janelia.stitching.Utils;
import org.janelia.util.ImageImporter;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.exception.ImgLibException;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class BlurTest
{
	public static void main11(final String[] args)
	{
		final ImagePlus imp = IJ.openImage( "/groups/betzig/betziglab/from_tier2/Tsungli/2016_07_29(Expansion)/Sample1_C1/Position1_LargeTile2/Scan1_AFTP_Iter_000_ch0_CAM1_stack0000_488nm_0000000msec_0017939173msecAbs_041x_017y_000z_0000t.tif" );
		Utils.workaroundImagePlusNSlices( imp );

		final Boundaries roi = new Boundaries( 3 );
		roi.setMax( 0, imp.getWidth() - 1 );
		roi.setMax( 1, imp.getHeight() - 1 );
		roi.setMax( 2, 39 );

		System.out.println( "Cropping.." );
		ImagePlus croppedImp = crop( imp, roi );
		System.out.println( "Saving to cropped.tif.." );
		IJ.saveAsTiff( croppedImp, "/groups/saalfeld/home/pisarevi/workspace/old-stitching-spark/stitching-spark-1/blur-test/cropped.tif" );
		imp.close();
		croppedImp.close();

		System.out.println( "Opening cropped.tif.." );
		croppedImp = IJ.openImage( "/groups/saalfeld/home/pisarevi/workspace/old-stitching-spark/stitching-spark-1/blur-test/cropped.tif" );

		System.out.println( "Blurring " + croppedImp.getNSlices() + " slices.." );
		//		BlurTest.< NumericType< ? > >blur( croppedImp, 3.0 );

		System.out.println( "Saving to cropped_blur.tif.." );
		IJ.saveAsTiff( croppedImp, "/groups/saalfeld/home/pisarevi/workspace/old-stitching-spark/stitching-spark-1/blur-test/cropped_blur.tif" );

		System.out.println( "Done" );
	}


	@SuppressWarnings( { "rawtypes", "unchecked" } )
	public static void main22(final String[] args) throws IncompatibleTypeException
	{
		new ImageJ();

		final ImagePlus imp = IJ.openImage( "/groups/betzig/betzigcollab/2016_09_02_DataFroStephan/Sample9_YFP_MPB_S35/Scan2_NC_Iter_000_ch0_CAM1_stack0000_560nm_0000000msec_0015064909msecAbs_020x_003y_000z_0000t.tif" );
		//final ImagePlus imp = IJ.openImage( "/nobackup/saalfeld/igor/Sample1_C1/test_tile.tif" );
		Utils.workaroundImagePlusNSlices( imp );


		final Boundaries noFirstSlice = new Boundaries( 3 );
		noFirstSlice.setMax( 0, imp.getWidth() - 1 );
		noFirstSlice.setMax( 1, imp.getHeight() - 1 );
		noFirstSlice.setMax( 2, imp.getNSlices() - 1 );
		noFirstSlice.setMin( 2, 1 );

		RandomAccessibleInterval rai = ImagePlusImgs.from( imp );
		rai = Views.interval( rai, noFirstSlice );

		final Boundaries roi = new Boundaries( 3 );
		roi.setMax( 0, imp.getWidth() - 1 );
		roi.setMax( 1, imp.getHeight() - 1 );
		roi.setMax( 2, Math.min( 39, imp.getNSlices() - 1 ) );
		roi.setMin( 2, roi.min( 2 ) + 1 );

		blur( rai, roi, new double[]{ 0.0, 0.0, 0.0 * 80.0 / 150.0 } );

		imp.show();
		//if ( args.length == 0 )
		//	return;

		System.out.println( "Cropping.." );
		final ImagePlus croppedImp = crop( imp, roi );
		imp.close();




		System.out.println( "Saving to crop_blur_onthefly.tif.." );
		IJ.saveAsTiff( croppedImp, "/groups/saalfeld/home/pisarevi/workspace/old-stitching-spark/stitching-spark-1/blur-test/crop_blur_onthefly.tif" );

		croppedImp.show();

		System.out.println( "Done" );
	}


	private static < T extends RealType< T > & NativeType< T > > ImagePlus crop( final ImagePlus inImp, final Boundaries region )
	{
		final T type = ( T ) ImageType.valueOf( inImp.getType() ).getType();

		final RandomAccessible< T > inImg = ImagePlusImgs.from( inImp );
		final ImagePlusImg< T, ? > outImg = new ImagePlusImgFactory<>( type.createVariable() ).create( region );

		final IterableInterval< T > inInterval = Views.flatIterable( Views.offsetInterval( inImg, region ) );
		final IterableInterval< T > outInterval = Views.flatIterable( outImg );

		final Cursor< T > inCursor = inInterval.cursor();
		final Cursor< T > outCursor = outInterval.cursor();

		while ( inCursor.hasNext() || outCursor.hasNext() )
			outCursor.next().set( inCursor.next() );


		ImagePlus impOut;
		try
		{
			impOut = outImg.getImagePlus();
		}
		catch ( final ImgLibException e )
		{
			impOut = ImageJFunctions.wrap( outImg, "" );
		}
		Utils.workaroundImagePlusNSlices( impOut );
		return impOut;
	}


	private static <T extends NumericType< T > >void blur(
			final RandomAccessibleInterval< T > image,
			final Interval interval,
			final double[] sigmas ) throws IncompatibleTypeException
	{
		final RandomAccessible< T > extendedImage = Views.extendMirrorSingle( image );
		final RandomAccessibleInterval< T > crop = Views.interval( extendedImage, interval );

		System.out.println( "Blurring " + crop.dimension( 2 ) + " slices.." );

		final ExecutorService service = Executors.newFixedThreadPool( 1 );
		Gauss3.gauss( sigmas, extendedImage, crop, service );
		service.shutdown();
	}






	public static void main(final String[] args) throws Exception
	{
		new ImageJ();

		final ImagePlus imp = ImageImporter.openImage( "/groups/betzig/betziglab/Shoh/BBBig/179/Mega_10x21_Z1902_BB_4.5_4colors(170).czi" );
		Utils.workaroundImagePlusNSlices( imp );

		final Interval region = new FinalInterval( new long[] { 1250, 0, 0 }, new long[] { imp.getWidth() - 1, imp.getHeight() - 1, imp.getNSlices() - 1 } );
		final Interval regionSlice = new FinalInterval( new long[] { region.min(0), region.min(1) }, new long[] { region.max(0), region.max(1) } );

		final Img< UnsignedShortType > outImg = ArrayImgs.unsignedShorts( Intervals.dimensionsAsLongArray(region) );

		final int channels = imp.getNChannels();
		final int frame = 0;
		for ( int z = 0; z < imp.getNSlices(); z++ )
		{
			if ( z % 100 == 0 )
				System.out.println( "Processing slice " + z + " out of "+ imp.getNSlices() );

			//System.out.println( "Grabbing channel data" );
			final Img< UnsignedShortType >[] data = new Img[ channels ];
			final Cursor< UnsignedShortType >[] cursors = new Cursor[ channels ];
			for ( int ch = 0; ch < channels; ch++ )
			{
				data[ ch ] = ArrayImgs.unsignedShorts( ( short[] ) imp.getStack().getProcessor( imp.getStackIndex( ch + 1, z + 1, frame + 1 ) ).getPixels(), new long[] { imp.getWidth(), imp.getHeight() } );
				cursors[ ch ] = Views.flatIterable( Views.offsetInterval( data[ ch ], regionSlice ) ).cursor();
			}

			final IntervalView< UnsignedShortType > outSlice = Views.hyperSlice( outImg, 2, z );
			final Cursor< UnsignedShortType > outSliceCursor = Views.flatIterable( outSlice ).cursor();

			while ( outSliceCursor.hasNext() )
			{
				double val = 0;
				for ( int ch = 0; ch < channels; ch++ )
					val += cursors[ch].next().getRealDouble();
				outSliceCursor.next().setReal( val / channels );
			}

			for ( int ch = 0; ch < channels; ch++ )
				if ( cursors[ch].hasNext() )
					throw new Exception( "Cursors mismatch" );
		}

		System.out.println( "Wrapping into ImagePlus" );
		final ImagePlus impOut = ImageJFunctions.wrap( outImg, null );
		Utils.workaroundImagePlusNSlices(impOut);
		impOut.show();
	}
}
