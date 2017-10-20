package org.janelia.dataaccess;

import java.net.URL;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;

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

	private final AmazonS3ClientBuilder s3Builder;

	private CustomURLStreamHandlerFactory( final AmazonS3ClientBuilder s3Builder )
	{
		this.s3Builder = s3Builder;
	}

	public static void init( final AmazonS3ClientBuilder s3Builder )
	{
		URL.setURLStreamHandlerFactory( new CustomURLStreamHandlerFactory( s3Builder ) );
	}

	@Override
	public URLStreamHandler createURLStreamHandler( final String protocol )
	{
		if ( protocol.equals( s3Protocol ) )
			return new S3URLStreamHandler( s3Builder );
		return null;
	}
}
