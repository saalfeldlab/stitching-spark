package org.janelia.dataaccess;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

class GoogleCloudStorageURLConnection extends URLConnection
{
	private final Storage storage;
	private Blob blob;

	public GoogleCloudStorageURLConnection( final Storage storage, final URL url )
	{
		super( url );
		this.storage = storage;
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
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( uri );
		blob = storage.get( BlobId.of( googleCloudUri.getBucket(), googleCloudUri.getKey() ) );
		connected = true;
	}

	@Override
	public String getContentType()
	{
		return blob.getContentType();
	}

	@Override
	public InputStream getInputStream() throws IOException
	{
		if ( !connected )
			connect();
		return new ByteArrayInputStream( blob.getContent() );
	}
}
