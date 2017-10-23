package org.janelia.dataaccess;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;

class S3URLConnection extends URLConnection
{
	private final AmazonS3 s3;
	private S3Object s3Object;

	public S3URLConnection( final AmazonS3 s3, final URL url )
	{
		super( url );
		this.s3 = s3;
	}

	@Override
	public void connect() throws IOException
	{
		final URI uri;
		try
		{
			uri = url.toURI();
		}
		catch ( final URISyntaxException e )
		{
			throw new RuntimeException( e );
		}
		final AmazonS3URI s3Uri = AmazonS3DataProvider.decodeS3Uri( uri );
		s3Object = s3.getObject( s3Uri.getBucket(), s3Uri.getKey() );
		connected = true;
	}

	@Override
	public String getContentType()
	{
		return s3Object.getObjectMetadata().getContentType();
	}

	@Override
	public InputStream getInputStream() throws IOException
	{
		if ( !connected )
			connect();
		return s3Object.getObjectContent();
	}
}
