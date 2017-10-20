package org.janelia.dataaccess;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public abstract class DataProviderFactory
{
	private static boolean initializedCustomURLStreamHandlerFactory;

	/**
	 * Constructs a filesystem-based data provider.
	 *
	 * @return
	 */
	public static DataProvider createFSDataProvider()
	{
		init( null );
		return new FSDataProvider();
	}

	/**
	 * Constructs an Amazon Web Services S3-based data provider
	 * using a given {@link AmazonS3ClientBuilder}.
	 *
	 * @param s3
	 * @return
	 */
	public static DataProvider createAmazonS3DataProvider( final AmazonS3ClientBuilder s3Builder )
	{
		init( s3Builder );
		return new AmazonS3DataProvider( s3Builder );
	}

	/**
	 * Constructs an Amazon Web Services S3-based data provider
	 * using the default {@link AmazonS3} client.
	 *
	 * @return
	 */
	public static DataProvider createAmazonS3DataProvider()
	{
		return createAmazonS3DataProvider( AmazonS3ClientBuilder.standard() );
	}

	private synchronized static void init( final AmazonS3ClientBuilder s3Builder )
	{
		if ( !initializedCustomURLStreamHandlerFactory )
		{
			initializedCustomURLStreamHandlerFactory = true;
			if ( s3Builder != null )
				CustomURLStreamHandlerFactory.init( s3Builder );
		}
	}
}
