package org.janelia.util;

import java.io.IOException;
import java.util.Arrays;

import ij.Prefs;
import loci.formats.in.ZeissCZIReader;
import loci.plugins.util.LociPrefs;
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
			if ( imp != null )
				Utils.workaroundImagePlusNSlices( imp );
			return imp;
		}
		else
		{
			try
			{
				final ImagePlus[] imps = openBioformatsImageSeries( path );
				if ( imps != null )
				{
					if ( imps.length != 1 )
						throw new UnsupportedOperationException( "Expected single image but requested image container " + path + " contains " + imps.length + " images, use openBioformatsImageSeries() instead" );
					else
						return imps[ 0 ];
				}
				else
				{
					return null;
				}
			}
			catch ( final IOException | FormatException e )
			{
				e.printStackTrace();
				return null;
			}
		}
	}

	public static ImagePlus[] openBioformatsImageSeries( final String path ) throws IOException, FormatException
	{
		final ImporterOptions options = new ImporterOptions();
		options.setId( path );
		options.setVirtual( true );

		// do not attempt to read similarly named files that are stored in the same directory
		options.setGroupFiles( false );
		options.setUngroupFiles( true );

		// read everything if there are several images stored in a single file
		options.setOpenAllSeries( true );

		Prefs.set(LociPrefs.PREF_CZI_AUTOSTITCH, false);
		Prefs.set(LociPrefs.PREF_CZI_ATTACHMENT, false);

		final ImportProcess process = new ImportProcess( options );
		if ( !process.execute() )
			return null;

		final ImagePlusReader reader = new ImagePlusReader( process );
		final ImagePlus[] imps = reader.openImagePlus();

		System.out.println( "Opened " + imps.length + ( imps.length == 1 ? " image" : " images" ) + " (" + imps[ 0 ].getTitle() + ") of size " + Arrays.toString( imps[ 0 ].getDimensions() ) );
		return imps;
	}
}
