package org.janelia.flatfield;

import java.io.IOException;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.stitching.Utils;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

public class ExtractDarkfield
{
	public static void main( final String[] args ) throws Exception
	{
		run(
				args[ 0 ],
				Double.parseDouble( args[ 1 ] ),
				args.length > 2 && args[ 2 ].equals( "--2d" )
			);
	}

	@SuppressWarnings( { "rawtypes", "unchecked" } )
	private static < U extends NativeType< U > & RealType< U > > void run( final String basePath, final double pivotValue, final boolean is2d ) throws IOException, ImgLibException
	{
		final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( basePath ) );
		final RandomAccessiblePairNullable< U, U > flatfield = FlatfieldCorrection.loadCorrectionImages( dataProvider, Utils.removeFilenameSuffix( basePath, "-flatfield" ), is2d ? 2 : 3 );
		final ImagePlusImg< U, ? > darkfield = new ImagePlusImgFactory< U >().create( ( Dimensions ) flatfield.getA(), ( U ) Util.getTypeFromInterval( ( RandomAccessibleInterval) flatfield.getA() ) );
		final Cursor< U > scalingCursor = Views.flatIterable( ( RandomAccessibleInterval ) flatfield.getA() ).cursor();
		final Cursor< U > translationCursor = Views.flatIterable( ( RandomAccessibleInterval ) flatfield.getB() ).cursor();
		final Cursor< U > darkfieldCursor = Views.flatIterable( darkfield ).cursor();
		while ( scalingCursor.hasNext() || translationCursor.hasNext() || darkfieldCursor.hasNext() )
		{
			final double scalingValue = scalingCursor.next().getRealDouble();
			final double translationValue = translationCursor.next().getRealDouble();
			darkfieldCursor.next().setReal( ( translationValue - pivotValue ) / scalingValue + pivotValue );
		}
		final ImagePlus darkfieldImp = darkfield.getImagePlus();
		Utils.workaroundImagePlusNSlices( darkfieldImp );
		dataProvider.saveImage( darkfieldImp, PathResolver.get( basePath, "extracted_darkfield.tif" ) );
	}
}
