package org.janelia.stitching.analysis;

import java.nio.file.Paths;

import org.janelia.stitching.ImageType;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class ImageConverter 
{
	public static void main( String[] args ) throws Exception
	{
		IJ.saveAsTiff( 
				convertToUnsignedShort( IJ.openImage( args[ 0 ] ) ),
				args[ 0 ] + "_16bit.tif" );
		System.out.println("Done");
	}
	
	private static < T extends RealType< T > & NativeType< T > > ImagePlus convertToUnsignedShort( final ImagePlus imp )
	{
		final Img< T > img = ImagePlusImgs.from( imp );
		final Cursor< T > src = Views.flatIterable( img ).cursor();
		
		final Img< UnsignedShortType > outImg = ArrayImgs.unsignedShorts( Intervals.dimensionsAsLongArray( img ) );
		final Cursor< UnsignedShortType > dst = Views.flatIterable( outImg ).cursor();
		
		while ( dst.hasNext() || src.hasNext() )
			dst.next().setReal( src.next().getRealDouble() );
		
		return ImageJFunctions.wrap( outImg, null );
	}
}
