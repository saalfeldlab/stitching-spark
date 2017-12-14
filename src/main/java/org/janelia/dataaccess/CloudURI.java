package org.janelia.dataaccess;

import java.net.URI;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;

import com.amazonaws.services.s3.AmazonS3URI;

public class CloudURI
{
	final AmazonS3URI s3Uri;
	final GoogleCloudStorageURI googleCloudUri;

	public CloudURI( final URI uri )
	{
		// try parsing as s3 link first
		AmazonS3URI s3Uri;
		try
		{
			s3Uri = new AmazonS3URI( uri );
		}
		catch ( final Exception e )
		{
			s3Uri = null;
		}

		if ( s3Uri != null )
		{
			this.s3Uri = s3Uri;
			this.googleCloudUri = null;
		}
		else
		{
			// might be a google cloud link
			final GoogleCloudStorageURI googleCloudUri;
			try
			{
				googleCloudUri = new GoogleCloudStorageURI( uri );
			}
			catch ( final Exception e )
			{
				throw new IllegalArgumentException( "The link should point to AWS S3 or Google Cloud Storage." );
			}

			this.s3Uri = null;
			this.googleCloudUri = googleCloudUri;
		}
	}

	public DataAccessType getType()
	{
		return s3Uri != null ? DataAccessType.AMAZON_S3 : DataAccessType.GOOGLE_CLOUD;
	}

	public String getBucket()
	{
		return s3Uri != null ? s3Uri.getBucket() : googleCloudUri.getBucket();
	}

	public String getKey()
	{
		return s3Uri != null ? s3Uri.getKey() : googleCloudUri.getKey();
	}
}
