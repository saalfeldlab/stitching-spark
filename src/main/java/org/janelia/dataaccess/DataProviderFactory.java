package org.janelia.dataaccess;

import java.net.URI;

import org.apache.commons.lang.NotImplementedException;
import org.janelia.dataaccess.fs.FSDataProvider;
import org.janelia.dataaccess.googlecloud.GoogleCloudDataProvider;
import org.janelia.dataaccess.s3.AmazonS3DataProvider;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public abstract class DataProviderFactory
{
	private static final String localFileProtocol = "file";
	private static final String s3Protocol = "s3";
	private static final String googleCloudProtocol = "gs";

	private static boolean initializedCustomURLStreamHandlerFactory;

	/**
	 * Constructs a filesystem-based {@link DataProvider}.
	 *
	 * @return
	 */
	public static DataProvider createFSDataProvider()
	{
		init( null, null );
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
		init( s3, null );
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
	 * @param storage
	 * @return
	 */
	public static DataProvider createGoogleCloudDataProvider( final Storage googleCloudStorage )
	{
		init( null, googleCloudStorage );
		return new GoogleCloudDataProvider( googleCloudStorage );
	}

	/**
	 * Constructs a Google Cloud Storage {@link DataProvider} using the default {@link Storage} client.
	 *
	 * @return
	 */
	public static DataProvider createGoogleCloudDataProvider()
	{
		return createGoogleCloudDataProvider( StorageOptions.getDefaultInstance().getService() );
	}

	/**
	 * Constructs an appropriate {@link DataProvider} for the given link or path.
	 *
	 * @return
	 */
	public static DataProvider createByLink( final String link )
	{
		final DataProviderType type = CloudURI.isCloudURI( link ) ? getTypeByURI( URI.create( link ) ) : DataProviderType.FILESYSTEM;
		return createByType( type );
	}

	/**
	 * Constructs an appropriate {@link DataProvider} for the given {@link URI}.
	 *
	 * @return
	 */
	public static DataProvider createByURI( final URI uri )
	{
		return createByType( getTypeByURI( uri ) );
	}

	/**
	 * Constructs a {@link DataProvider} of the given {@link DataProviderType}.
	 *
	 * @return
	 */
	public static DataProvider createByType( final DataProviderType type )
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

	public static DataProviderType getTypeByURI( final URI uri )
	{
		final String protocol = uri.getScheme();

		if ( protocol == null || protocol.equalsIgnoreCase( localFileProtocol ) )
			return DataProviderType.FILESYSTEM;

		if ( protocol.equalsIgnoreCase( s3Protocol ) )
			return DataProviderType.AMAZON_S3;

		if ( protocol.equalsIgnoreCase( googleCloudProtocol ) )
			return DataProviderType.GOOGLE_CLOUD;

		throw new NotImplementedException( "factory for protocol " + uri.getScheme() + " is not implemented" );
	}

	public static URI createBucketUri( final DataProviderType type, final String bucketName )
	{
		final String protocol;
		switch ( type )
		{
		case AMAZON_S3:
			protocol = s3Protocol;
			break;
		case GOOGLE_CLOUD:
			protocol = googleCloudProtocol;
			break;
		case FILESYSTEM:
			throw new IllegalArgumentException( "Not supported for filesystem storage" );
		default:
			throw new NotImplementedException( "Not implemented for type " + type );
		}
		return URI.create( protocol + "://" + bucketName + "/" );
	}

	private synchronized static void init( final AmazonS3 s3, final Storage googleCloudStorage )
	{
		if ( !initializedCustomURLStreamHandlerFactory )
		{
			initializedCustomURLStreamHandlerFactory = true;
			if ( s3 != null || googleCloudStorage != null )
				CustomURLStreamHandlerFactory.init( s3, googleCloudStorage );
		}
	}
}
