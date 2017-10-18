package org.janelia.dataaccess;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.util.ImageImporter;

import com.google.gson.GsonBuilder;

import ij.ImagePlus;

/**
 * Provides filesystem-based access to data stored on a local or network drive.
 *
 * @author Igor Pisarev
 */
class FSDataProvider implements DataProvider
{
	@Override
	public ImagePlus loadImage( final String path )
	{
		return ImageImporter.openImage( path );
	}

	@Override
	public Reader getJsonReader( final String path ) throws IOException
	{
		return new FileReader( path );
	}

	@Override
	public Writer getJsonWriter( final String path ) throws IOException
	{
		return new FileWriter( path );
	}

	@Override
	public N5Reader createN5Reader( final String basePath )
	{
		return N5.openFSReader( basePath );
	}

	@Override
	public N5Writer createN5Writer( final String basePath ) throws IOException
	{
		return N5.openFSWriter( basePath );
	}

	@Override
	public N5Reader createN5Reader( final String basePath, final GsonBuilder gsonBuilder )
	{
		return N5.openFSReader( basePath, gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final String basePath, final GsonBuilder gsonBuilder ) throws IOException
	{
		return N5.openFSWriter( basePath, gsonBuilder );
	}
}
