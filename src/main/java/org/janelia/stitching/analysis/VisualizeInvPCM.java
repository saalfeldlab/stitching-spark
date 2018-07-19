package org.janelia.stitching.analysis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.janelia.util.ImageImporter;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

public class VisualizeInvPCM
{
	public static void main( final String[] args ) throws IOException
	{
		run();
	}

	private static < T extends NativeType< T > & RealType< T > > void run() throws IOException
	{
		final String basePath = "/nrs/saalfeld/igor/illumination-correction/Sample1_C1/stitching/new-affine-stitching/21-22x_21-22y_10-11z/iter0/invPCM";
		final List< RandomAccessibleInterval< T > > lst = new ArrayList<>();
		try ( final Stream< Path > paths = Files.list( Paths.get( basePath ) ) )
		{
			paths.forEachOrdered( path ->
					{
						final ImagePlus imp = ImageImporter.openImage( path.toAbsolutePath().toString() );
						final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );
						lst.add( img );
					}
				);
		}

		final RandomAccessibleInterval< T > imgStack = Views.permute( Views.stack( lst ), 2, 3 );

		final ImagePlus impStack = ImageJFunctions.wrap( imgStack, "invPCM_stack_" + lst.size() );
		new ImageJ();
		impStack.show();
	}
}
