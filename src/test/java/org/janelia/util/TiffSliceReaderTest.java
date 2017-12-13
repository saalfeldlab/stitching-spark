package org.janelia.util;

import java.io.IOException;
import java.util.Random;

import org.junit.Assert;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

public class TiffSliceReaderTest
{
	private static final double EPSILON = 1e-9;

	final Random rnd = new Random();

//	@Test
	public void testUnsignedShort() throws IOException
	{
		test( "/nrs/saalfeld/igor/test-images/unsigned-short.tif" );
	}

//	@Test
	public void testFloat() throws IOException
	{
		test( "/nrs/saalfeld/igor/test-images/float.tif" );
	}

	private < T extends NativeType< T > & RealType< T > > void test( final String path ) throws IOException
	{
		System.out.println( "[TiffSliceReaderTest] Opening " + path + "..." );
		final ImagePlus impFull = ImageImporter.openImage( path );
		final int slice = rnd.nextInt( impFull.getNSlices() ) + 1;
		System.out.println( "[TiffSliceReaderTest] Reading slice " + slice );
		final ImagePlus impSlice = TiffSliceReader.readSlice( path, slice );

		final RandomAccessibleInterval< T > imgSlice = ImagePlusImgs.from( impSlice );
		final RandomAccessibleInterval< T > imgFull = ImagePlusImgs.from( impFull );
		final RandomAccessibleInterval< T > imgFullSlice = Views.hyperSlice( imgFull, 2, slice - 1 );

		final Cursor< T > imgSliceCursor = Views.flatIterable( imgSlice ).cursor();
		final Cursor< T > imgFullSliceCursor = Views.flatIterable( imgFullSlice ).cursor();

		while ( imgSliceCursor.hasNext() || imgFullSliceCursor.hasNext() )
			Assert.assertEquals( imgFullSliceCursor.next().getRealDouble(), imgSliceCursor.next().getRealDouble(), EPSILON );
	}
}
