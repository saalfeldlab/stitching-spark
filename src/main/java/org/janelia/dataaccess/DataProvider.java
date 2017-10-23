package org.janelia.dataaccess;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;

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
	public DataProviderType getType();

	public URI getUri( final String path ) throws URISyntaxException;

	public boolean fileExists( final URI uri ) throws IOException;

	public InputStream getInputStream( final URI uri ) throws IOException;
	public OutputStream getOutputStream( final URI uri ) throws IOException;

	public ImagePlus loadImage( final URI uri ) throws IOException;
	public void saveImage( final ImagePlus imp, final URI uri ) throws IOException;

	public Reader getJsonReader( final URI uri ) throws IOException;
	public Writer getJsonWriter( final URI uri ) throws IOException;

	public N5Reader createN5Reader( final URI baseUri ) throws IOException;
	public N5Writer createN5Writer( final URI baseUri ) throws IOException;

	public N5Reader createN5Reader( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException;
	public N5Writer createN5Writer( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException;
}
