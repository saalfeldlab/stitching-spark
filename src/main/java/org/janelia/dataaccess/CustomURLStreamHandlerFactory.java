package org.janelia.dataaccess;

import java.net.URL;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

import org.janelia.dataaccess.googlecloud.GoogleCloudStorageURLStreamHandler;
import org.janelia.dataaccess.s3.S3URLStreamHandler;

import com.amazonaws.services.s3.AmazonS3;
import com.google.cloud.storage.Storage;

/**
 * Factory for all supported custom protocols.
 *
 * The core method {@link URL#setURLStreamHandlerFactory(URLStreamHandlerFactory)}
 * can be called at most once, therefore this class must handle all supported protocols.
 *
 * @author Igor Pisarev
 */
class CustomURLStreamHandlerFactory implements URLStreamHandlerFactory
{
	private static final String s3Protocol = "s3";
	private static final String googleCloudProtocol = "gs";

	private final AmazonS3 s3;
	private final Storage googleCloudStorage;

	private CustomURLStreamHandlerFactory( final AmazonS3 s3, final Storage googleCloudStorage )
	{
		this.s3 = s3;
		this.googleCloudStorage = googleCloudStorage;
	}

	public synchronized static void init( final AmazonS3 s3, final Storage googleCloudStorage )
	{
		URL.setURLStreamHandlerFactory( new CustomURLStreamHandlerFactory( s3, googleCloudStorage ) );
	}

	@Override
	public URLStreamHandler createURLStreamHandler( final String protocol )
	{
		if ( protocol.equalsIgnoreCase( s3Protocol ) )
			return new S3URLStreamHandler( s3 );

		if ( protocol.equalsIgnoreCase( googleCloudProtocol ) )
			return new GoogleCloudStorageURLStreamHandler( googleCloudStorage );

		return null;
	}
}
