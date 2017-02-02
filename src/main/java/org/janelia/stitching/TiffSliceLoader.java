package org.janelia.stitching;

import java.awt.image.ColorModel;
import java.awt.image.IndexColorModel;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import ij.IJ;
import ij.ImagePlus;
import ij.LookUpTable;
import ij.io.FileInfo;
import ij.io.ImageReader;
import ij.io.TiffDecoder;
import ij.process.ByteProcessor;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;

public class TiffSliceLoader 
{
	public static ImagePlus loadSlice( final TileInfo tile, final int slice )
	{
		if ( tile.numDimensions() == 2 )
		{
			if ( slice == 1 )
				return IJ.openImage( tile.getFilePath() );
			else
				return null;
		}
		
		final FileInfo fileInfo;
		try 
		{
			fileInfo = new TiffDecoder( 
					Paths.get( tile.getFilePath() ).getParent().toString(), 
					Paths.get( tile.getFilePath() ).getFileName().toString() )
				.getTiffInfo()[ slice - 1 ];
		} 
		catch ( final IOException e ) 
		{
			e.printStackTrace();
			return null;
		}
		
		try ( final InputStream is = new FileInputStream( tile.getFilePath() ) )
		{
			final ImageReader reader = new ImageReader( fileInfo );
			final Object pixels = reader.readPixels( is, fileInfo.getOffset() );
			if ( pixels == null )
				return null;
			
			final int width = (int) tile.getSize( 0 ), height = (int) tile.getSize( 1 );
			final ColorModel cm = createColorModel( fileInfo );
			final ImageProcessor ip;
			switch ( tile.getType() ) 
			{
				case GRAY8:
					ip = new ByteProcessor( width, height, (byte[])pixels, cm);
					break;
				case GRAY16:
		    		ip = new ShortProcessor(width, height, (short[])pixels, cm);
					break;
				case GRAY32:
		    		ip = new FloatProcessor(width, height, (float[])pixels, cm);
					break;
				default:
					System.out.println( "Unknown image type" );
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
	}

	private static ColorModel createColorModel(final FileInfo fi) {
		if (fi.lutSize>0)
			return new IndexColorModel(8, fi.lutSize, fi.reds, fi.greens, fi.blues);
		else
			return LookUpTable.createGrayscaleColorModel(fi.whiteIsZero);
	}
}
