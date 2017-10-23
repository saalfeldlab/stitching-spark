package org.janelia.util;

import java.awt.image.ColorModel;
import java.awt.image.IndexColorModel;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.s3.model.S3ObjectInputStream;

import ij.CompositeImage;
import ij.IJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.LookUpTable;
import ij.io.FileInfo;
import ij.io.FileOpener;
import ij.io.ImageReader;
import ij.io.Opener;
import ij.io.TiffDecoder;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;

/**
 * Opens the requested slice of a .tif image.
 * A lot of code borrowed from IJ's {@link Opener} {@link FileOpener}
 *
 * @author Igor Pisarev
 */

public class TiffSliceReader
{
	@FunctionalInterface
	public static interface TiffInputStreamSupplier
	{
		public InputStream get() throws IOException;
	}

	/**
	 * Opens the requested slice of a .tif image.
	 * @param path
	 * 			a path to the .tif file
	 * @param slice
	 * 			an index of the desired slice in IJ's notation (1-indexed)
	 * @return
	 * 			an ImagePlus containing the requested slice, or null if it cannot be read
	 * @throws IOException
	 */
	public static ImagePlus readSlice( final String path, final int slice ) throws IOException
	{
		return readSlice( () -> new FileInputStream( path ), slice );
	}

	/**
	 * Opens the requested slice of a .tif image.
	 * @param inSupplier
	 * 			an input stream supplier for the desired tiff file
	 * @param slice
	 * 			an index of the desired slice in IJ's notation (1-indexed)
	 * @return
	 * 			an ImagePlus containing the requested slice, or null if it cannot be read
	 * @throws IOException
	 */
	public static ImagePlus readSlice( final TiffInputStreamSupplier inSupplier, final int slice ) throws IOException
	{
		final FileInfo fileInfo;

		final FileInfo[] fileInfos;
		try ( final InputStream in = inSupplier.get() )
		{
			fileInfos = new TiffDecoder( in, "" ).getTiffInfo();
			if ( in instanceof S3ObjectInputStream )
				( ( S3ObjectInputStream ) in ).abort();
		}

		// Hack to read uncompressed float images correctly (at least in my case). Otherwise, it detects a single slice but with nImages=501
		if ( fileInfos.length == 1 && fileInfos[ 0 ].nImages > 1 && fileInfos[ 0 ].compression == FileInfo.COMPRESSION_NONE )
		{
			fileInfo = fileInfos[ 0 ];
			final int numPixels = fileInfo.width * fileInfo.height;
			fileInfo.offset += ( numPixels * bytesPerPixel( fileInfo ) + fileInfo.gapBetweenImages ) * ( slice - 1 );
		}
		else
		{
			fileInfo = fileInfos[ slice - 1 ];
		}

		final ImageReader reader = new ImageReader( fileInfo );
		final Object pixels;
		try ( final InputStream in = inSupplier.get() )
		{
			pixels = reader.readPixels( in, fileInfo.getOffset() );
			if ( in instanceof S3ObjectInputStream )
				( ( S3ObjectInputStream ) in ).abort();
		}
		if ( pixels == null )
			return null;

		final int width = fileInfo.width, height = fileInfo.height;

		final ColorModel cm = createColorModel( fileInfo );
		final ImageProcessor ip;
		switch ( fileInfo.fileType )
		{
		case FileInfo.GRAY8:
		case FileInfo.COLOR8:
		case FileInfo.BITMAP:
			ip = new ByteProcessor(width, height, (byte[])pixels, cm);
			break;
		case FileInfo.GRAY16_SIGNED:
		case FileInfo.GRAY16_UNSIGNED:
		case FileInfo.GRAY12_UNSIGNED:
    		ip = new ShortProcessor(width, height, (short[])pixels, cm);
			break;
		case FileInfo.GRAY32_INT:
		case FileInfo.GRAY32_UNSIGNED:
		case FileInfo.GRAY32_FLOAT:
		case FileInfo.GRAY24_UNSIGNED:
		case FileInfo.GRAY64_FLOAT:
    		ip = new FloatProcessor(width, height, (float[])pixels, cm);
			break;
		case FileInfo.RGB:
		case FileInfo.BGR:
		case FileInfo.ARGB:
		case FileInfo.ABGR:
		case FileInfo.BARG:
		case FileInfo.RGB_PLANAR:
		case FileInfo.CMYK:
			ip = new ColorProcessor(width, height, (int[])pixels);
			if (fileInfo.fileType==FileInfo.CMYK)
				ip.invert();
			break;

		case FileInfo.RGB48:
		case FileInfo.RGB48_PLANAR:
			// this is a special case because it effectively creates a stack
			return createRGB48Image(fileInfo, (Object[])pixels);

		default:
			return null;
		}

		final ImagePlus imp = new ImagePlus( fileInfo.fileName, ip );
		imp.setFileInfo( fileInfo );
		return imp;
	}


	/**
	 * Opens the requested slice of a .tif image.
	 * @param path
	 * 			a path to the .tif file
	 * @param slice
	 * 			an index of the desired slice in IJ's notation (1-indexed)
	 * @return
	 * 			an ImagePlus containing the requested slice, or null if it cannot be read
	 */
	/*public static ImagePlus readSlice( final String path, final int slice )
	{
		final FileInfo fileInfo;
		try
		{
			final FileInfo[] fileInfos = new TiffDecoder(
					Paths.get( path ).getParent().toString(),
					Paths.get( path ).getFileName().toString() )
				.getTiffInfo();

			// Hack to read uncompressed float images correctly (at least in my case). Otherwise, it detects a single slice but with nImages=501
			if ( fileInfos.length == 1 && fileInfos[ 0 ].nImages > 1 && fileInfos[ 0 ].compression == FileInfo.COMPRESSION_NONE )
			{
				fileInfo = fileInfos[ 0 ];
				final int numPixels = fileInfo.width * fileInfo.height;
				fileInfo.offset += ( numPixels * bytesPerPixel( fileInfo ) + fileInfo.gapBetweenImages ) * ( slice - 1 );
			}
			else
				fileInfo = fileInfos[ slice - 1 ];
		}
		catch ( final IOException e )
		{
			e.printStackTrace();
			return null;
		}

		try ( final InputStream is = new FileInputStream( path ) )
		{
			final ImageReader reader = new ImageReader( fileInfo );
			final Object pixels = reader.readPixels( is, fileInfo.getOffset() );
			if ( pixels == null )
				return null;

			final int width = fileInfo.width, height = fileInfo.height;

			final ColorModel cm = createColorModel( fileInfo );
			final ImageProcessor ip;
			switch ( fileInfo.fileType )
			{
			case FileInfo.GRAY8:
			case FileInfo.COLOR8:
			case FileInfo.BITMAP:
				ip = new ByteProcessor(width, height, (byte[])pixels, cm);
				break;
			case FileInfo.GRAY16_SIGNED:
			case FileInfo.GRAY16_UNSIGNED:
			case FileInfo.GRAY12_UNSIGNED:
	    		ip = new ShortProcessor(width, height, (short[])pixels, cm);
				break;
			case FileInfo.GRAY32_INT:
			case FileInfo.GRAY32_UNSIGNED:
			case FileInfo.GRAY32_FLOAT:
			case FileInfo.GRAY24_UNSIGNED:
			case FileInfo.GRAY64_FLOAT:
	    		ip = new FloatProcessor(width, height, (float[])pixels, cm);
				break;
			case FileInfo.RGB:
			case FileInfo.BGR:
			case FileInfo.ARGB:
			case FileInfo.ABGR:
			case FileInfo.BARG:
			case FileInfo.RGB_PLANAR:
			case FileInfo.CMYK:
				ip = new ColorProcessor(width, height, (int[])pixels);
				if (fileInfo.fileType==FileInfo.CMYK)
					ip.invert();
				break;

			case FileInfo.RGB48:
			case FileInfo.RGB48_PLANAR:
				// this is a special case because it effectively creates a stack
				return createRGB48Image(fileInfo, (Object[])pixels);

			default:
				return null;
			}

			final ImagePlus imp = new ImagePlus( fileInfo.fileName, ip );
			imp.setFileInfo( fileInfo );
			return imp;
		}
		catch ( final IOException e )
		{
			e.printStackTrace();
			return null;
		}
	}*/

	private static ColorModel createColorModel(final FileInfo fi) {
		if (fi.lutSize>0)
			return new IndexColorModel(8, fi.lutSize, fi.reds, fi.greens, fi.blues);
		else
			return LookUpTable.createGrayscaleColorModel(fi.whiteIsZero);
	}

	private static ImagePlus createRGB48Image( final FileInfo fileInfo, final Object[] pixelArray )
	{
		final boolean planar = fileInfo.fileType==FileInfo.RGB48_PLANAR;
		int nChannels = 3;
		final ImageStack stack = new ImageStack(fileInfo.width, fileInfo.width);
		stack.addSlice("Red", pixelArray[0]);
		stack.addSlice("Green", pixelArray[1]);
		stack.addSlice("Blue", pixelArray[2]);
		if (fileInfo.samplesPerPixel==4 && pixelArray.length==4) {
			stack.addSlice("Gray", pixelArray[3]);
			nChannels = 4;
		}
		ImagePlus imp = new ImagePlus(fileInfo.fileName, stack);
		imp.setDimensions(nChannels, 1, 1);
		if (planar)
			imp.getProcessor().resetMinAndMax();
		imp.setFileInfo(fileInfo);
		int mode = IJ.COMPOSITE;
		if (fileInfo.description!=null) {
			if (fileInfo.description.indexOf("mode=color")!=-1)
			mode = IJ.COLOR;
			else if (fileInfo.description.indexOf("mode=gray")!=-1)
			mode = IJ.GRAYSCALE;
		}
		imp = new CompositeImage(imp, mode);
		if (!planar && fileInfo.displayRanges==null) {
			if (nChannels==4)
				((CompositeImage)imp).resetDisplayRanges();
			else {
				for (int c=1; c<=3; c++) {
					imp.setPosition(c, 1, 1);
					//imp.setDisplayRange(minValue, maxValue);
				}
				imp.setPosition(1, 1, 1);
				}
		}
		if (fileInfo.whiteIsZero) // cmyk?
			IJ.run(imp, "Invert", "");
		return imp;
	}

	private static Integer bytesPerPixel( final FileInfo fileInfo )
	{
		switch ( fileInfo.fileType )
		{
		case FileInfo.GRAY8:
		case FileInfo.COLOR8:
			return 1;
		case FileInfo.GRAY16_SIGNED:
		case FileInfo.GRAY16_UNSIGNED:
			return 2;
		case FileInfo.GRAY32_INT:
		case FileInfo.GRAY32_UNSIGNED:
		case FileInfo.GRAY32_FLOAT:
			return 4;
		case FileInfo.GRAY64_FLOAT:
			return 8;
		case FileInfo.RGB:
		case FileInfo.BGR:
		case FileInfo.ARGB:
		case FileInfo.ABGR:
		case FileInfo.BARG:
		case FileInfo.CMYK:
			return fileInfo.getBytesPerPixel();
		case FileInfo.RGB_PLANAR:
			return 3;
		case FileInfo.BITMAP:
			return 1;
		case FileInfo.RGB48:
			return 6;
		case FileInfo.RGB48_PLANAR:
			return 2;
		default:
			return null;
		}
	}
}
