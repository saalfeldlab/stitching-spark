package org.janelia.dataaccess;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

class S3URLStreamHandler extends URLStreamHandler
{
	private final AmazonS3ClientBuilder s3Builder;
	private final transient AmazonS3 s3;

	public S3URLStreamHandler( final AmazonS3ClientBuilder s3Builder )
	{
		this.s3Builder = s3Builder;
		s3 = s3Builder.build();
	}

	@Override
	protected URLConnection openConnection( final URL url ) throws IOException
	{
		return new S3URLConnection( s3, url );
	}
}
