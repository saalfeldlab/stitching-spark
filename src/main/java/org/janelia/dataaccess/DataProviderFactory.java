package org.janelia.dataaccess;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.cloud.storage.Storage;
import org.apache.commons.lang.NotImplementedException;
import org.janelia.dataaccess.fs.FSDataProvider;
import org.janelia.dataaccess.googlecloud.GoogleCloudDataProvider;
import org.janelia.dataaccess.googlecloud.GoogleCloudURLStreamHandlerFactory;
import org.janelia.dataaccess.s3.AmazonS3DataProvider;
import org.janelia.dataaccess.s3.AmazonS3URLStreamHandlerFactory;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageClient;

import java.net.URI;
import java.net.URL;
import java.net.URLStreamHandlerFactory;

public abstract class DataProviderFactory
{
	private static boolean initializedCustomURLStreamHandlerFactory = false;

	/**
	 * Constructs a filesystem-based {@link DataProvider}.
	 *
	 * @return
	 */
	public static DataProvider createFSDataProvider()
	{
		initCustomURLStreamHandlerFactory( null );
		return new FSDataProvider();
	}

	/**
	 * Constructs an Amazon Web Services S3-based {@link DataProvider} using a given {@link AmazonS3} client.
	 *
	 * @param s3
	 * @return
	 */
	public static DataProvider createAmazonS3DataProvider( final AmazonS3 s3 )
	{
		initCustomURLStreamHandlerFactory( new AmazonS3URLStreamHandlerFactory( s3 ) );
		return new AmazonS3DataProvider( s3 );
	}

	/**
	 * Constructs an Amazon Web Services S3-based {@link DataProvider} using the default {@link AmazonS3} client.
	 *
	 * @return
	 */
	public static DataProvider createAmazonS3DataProvider()
	{
		return createAmazonS3DataProvider( AmazonS3ClientBuilder.standard().build() );
	}

	/**
	 * Constructs a Google Cloud Storage {@link DataProvider} using a given {@link Storage} client.
	 *
	 * @param googleCloudStorage
	 * @return
	 */
	public static DataProvider createGoogleCloudDataProvider( final Storage googleCloudStorage )
	{
		initCustomURLStreamHandlerFactory( new GoogleCloudURLStreamHandlerFactory( googleCloudStorage ) );
		return new GoogleCloudDataProvider( googleCloudStorage );
	}

	/**
	 * Constructs a Google Cloud Storage {@link DataProvider} using the default {@link Storage} client.
	 *
	 * @return
	 */
	public static DataProvider createGoogleCloudDataProvider()
	{
		return createGoogleCloudDataProvider( new GoogleCloudStorageClient().create() );
	}

	/**
	 * Constructs a {@link DataProvider} of the given {@link DataProviderType}.
	 *
	 * @return
	 */
	public static DataProvider create( final DataProviderType type )
	{
		switch ( type )
		{
		case FILESYSTEM:
			return createFSDataProvider();
		case AMAZON_S3:
			return createAmazonS3DataProvider();
		case GOOGLE_CLOUD:
			return createGoogleCloudDataProvider();
		default:
			throw new NotImplementedException( "Data provider of type " + type + " is not implemented" );
		}
	}

	public static DataProviderType detectType( final String link )
	{
		if ( CloudURI.isCloudURI( link ) )
			return new CloudURI( URI.create( link ) ).getType();
		else
			return DataProviderType.FILESYSTEM;
	}

	private synchronized static void initCustomURLStreamHandlerFactory( final URLStreamHandlerFactory urlStreamHandlerFactory )
	{
		if ( !initializedCustomURLStreamHandlerFactory )
		{
			initializedCustomURLStreamHandlerFactory = true;
			if ( urlStreamHandlerFactory != null )
				URL.setURLStreamHandlerFactory( urlStreamHandlerFactory );
		}
	}
}
