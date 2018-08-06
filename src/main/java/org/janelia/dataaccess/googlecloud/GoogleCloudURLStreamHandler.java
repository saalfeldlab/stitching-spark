package org.janelia.dataaccess.googlecloud;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import com.google.cloud.storage.Storage;

class GoogleCloudURLStreamHandler extends URLStreamHandler
{
	private final Storage storage;

	public GoogleCloudURLStreamHandler( final Storage storage )
	{
		this.storage = storage;
	}

	@Override
	protected URLConnection openConnection( final URL url ) throws IOException
	{
		return new GoogleCloudURLConnection( storage, url );
	}
}
