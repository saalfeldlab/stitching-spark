package org.janelia.dataaccess;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import com.google.gson.GsonBuilder;

import ij.ImagePlus;

/**
 * Provides access to data on the used storage system.
 *
 * @author Igor Pisarev
 */
public interface DataProvider
{
	public ImagePlus loadImage( final String path );

	public Reader getJsonReader( final String path ) throws IOException;
	public Writer getJsonWriter( final String path ) throws IOException;

	public N5Reader createN5Reader( final String basePath );
	public N5Writer createN5Writer( final String basePath ) throws IOException;

	public N5Reader createN5Reader( final String basePath, final GsonBuilder gsonBuilder );
	public N5Writer createN5Writer( final String basePath, final GsonBuilder gsonBuilder ) throws IOException;
}
