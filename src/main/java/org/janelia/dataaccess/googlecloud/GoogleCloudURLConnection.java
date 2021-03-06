package org.janelia.dataaccess.googlecloud;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

class GoogleCloudURLConnection extends URLConnection
{
	private final Storage storage;
	private Blob blob;

	public GoogleCloudURLConnection( final Storage storage, final URL url )
	{
		super( url );
		this.storage = storage;
	}

	@Override
	public void connect() throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( url.toString() );
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
