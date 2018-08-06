package org.janelia.dataaccess.s3;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

import com.amazonaws.services.s3.AmazonS3;

public class AmazonS3URLStreamHandlerFactory implements URLStreamHandlerFactory
{
	private static final String s3Protocol = "s3";

	private final AmazonS3 s3;

	public AmazonS3URLStreamHandlerFactory( final AmazonS3 s3 )
	{
		this.s3 = s3;
	}

	@Override
	public URLStreamHandler createURLStreamHandler( final String protocol )
	{
		if ( protocol.equalsIgnoreCase( s3Protocol ) )
			return new S3URLStreamHandler( s3 );
		else
			return null;
	}
}
