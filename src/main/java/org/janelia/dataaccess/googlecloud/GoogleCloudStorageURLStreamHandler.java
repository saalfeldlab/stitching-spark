package org.janelia.dataaccess.googlecloud;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import com.google.cloud.storage.Storage;

public class GoogleCloudStorageURLStreamHandler extends URLStreamHandler
{
	private final Storage storage;

	public GoogleCloudStorageURLStreamHandler( final Storage storage )
	{
		this.storage = storage;
	}

	@Override
	protected URLConnection openConnection( final URL url ) throws IOException
	{
		return new GoogleCloudStorageURLConnection( storage, url );
	}
}
