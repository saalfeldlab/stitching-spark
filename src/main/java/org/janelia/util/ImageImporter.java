package org.janelia.util;

import java.io.IOException;
import java.util.Arrays;

import org.janelia.stitching.Utils;

import ij.IJ;
import ij.ImagePlus;
import loci.formats.FormatException;
import loci.plugins.in.ImagePlusReader;
import loci.plugins.in.ImportProcess;
import loci.plugins.in.ImporterOptions;

public class ImageImporter
{
	public static ImagePlus openImage( final String path )
	{
		if ( path.endsWith( ".tif" ) || path.endsWith( ".tiff" ) )
		{
			final ImagePlus imp = IJ.openImage( path );
			Utils.workaroundImagePlusNSlices( imp );
			return imp;
		}
		else
		{
			try
			{
				return openBioformatsImage( path );
			}
			catch ( final IOException | FormatException e )
			{
				e.printStackTrace();
				return null;
			}
		}
	}

	public static ImagePlus openBioformatsImage( final String path ) throws IOException, FormatException
	{
		final ImporterOptions options = new ImporterOptions();
		options.setId( path );
		options.setVirtual( true );

		options.setGroupFiles( false );
		options.setUngroupFiles( true );

		final ImportProcess process = new ImportProcess( options );
		if ( !process.execute() )
			return null;

		final ImagePlusReader reader = new ImagePlusReader( process );

		final ImagePlus imp = reader.openImagePlus()[ 0 ];
		System.out.println( "Opened image " + imp.getTitle() + " of size " + Arrays.toString( imp.getDimensions() ) );
		return imp;

	}
}
