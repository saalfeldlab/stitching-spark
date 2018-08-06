package org.janelia.dataaccess.googlecloud;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

import com.google.cloud.storage.Storage;

public class GoogleCloudURLStreamHandlerFactory implements URLStreamHandlerFactory
{
	private static final String googleCloudProtocol = "gs";

	private final Storage googleCloudStorage;

	public GoogleCloudURLStreamHandlerFactory( final Storage googleCloudStorage )
	{
		this.googleCloudStorage = googleCloudStorage;
	}

	@Override
	public URLStreamHandler createURLStreamHandler( final String protocol )
	{
		if ( protocol.equalsIgnoreCase( googleCloudProtocol ) )
			return new GoogleCloudStorageURLStreamHandler( googleCloudStorage );
		else
			return null;
	}
}
