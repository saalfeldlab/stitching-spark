package org.janelia.dataaccess.s3;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import com.amazonaws.services.s3.AmazonS3;

class AmazonS3URLStreamHandler extends URLStreamHandler
{
	private final AmazonS3 s3;

	public AmazonS3URLStreamHandler( final AmazonS3 s3 )
	{
		this.s3 = s3;
	}

	@Override
	protected URLConnection openConnection( final URL url ) throws IOException
	{
		return new AmazonS3URLConnection( s3, url );
	}
}
