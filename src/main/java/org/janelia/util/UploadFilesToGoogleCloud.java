package org.janelia.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudOAuth;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageClient;
import org.janelia.util.concurrent.MultithreadedExecutor;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

public class UploadFilesToGoogleCloud
{
	public static void main( final String[] args ) throws Exception
	{
		final String dirToUpload = args[ 0 ], bucket = args[ 1 ];

		// recursively list all files that need to be uploaded
		final Path dirPath = Paths.get( dirToUpload );
		final List< Path > pathsToUpload = new ArrayList<>();
		try ( final Stream< Path > fileStream = Files.walk( dirPath ) )
		{
			fileStream.filter( Files::isRegularFile ).forEach( pathsToUpload::add );
		}
		final String[] filesToUploadArr = new String[ pathsToUpload.size() ];
		for ( int i = 0; i < filesToUploadArr.length; ++i )
			filesToUploadArr[ i ] = dirPath.relativize( pathsToUpload.get( i ) ).toString();

		// load access tokens
		final GoogleCloudOAuth oauth = new GoogleCloudOAuth(
				Collections.singleton( GoogleCloudStorageClient.StorageScope.READ_WRITE ),
				"n5-google-cloud-oauth2",
				UploadFilesToGoogleCloud.class.getResourceAsStream( "/googlecloud_client_secrets.json" )
			);

		final AtomicInteger numUploaded = new AtomicInteger();

		try ( final MultithreadedExecutor threadPool = new MultithreadedExecutor() )
		{
			threadPool.run(
					i ->
					{
						final String fileToUpload = filesToUploadArr[ i ];
						final GoogleCloudStorageClient storageClient = new GoogleCloudStorageClient(
								oauth.getAccessToken(),
								oauth.getClientSecrets(),
								oauth.getRefreshToken()
							);
						final Storage storage = storageClient.create();
						final BlobId blobId = BlobId.of( bucket, fileToUpload );
						final BlobInfo blobInfo = BlobInfo.newBuilder( blobId ).build();
						final byte[] bytes;
						try
						{
							bytes = Files.readAllBytes( Paths.get( dirToUpload, fileToUpload ) );
						}
						catch ( final IOException e )
						{
							throw new RuntimeException( e );
						}
						storage.create( blobInfo, bytes );

						final int uploaded = numUploaded.incrementAndGet();
						if ( uploaded % 200 == 0 )
							System.out.println(  "uploaded " + uploaded + " out of " + filesToUploadArr.length );
					},
					filesToUploadArr.length
				);
		}

		System.out.println();
		System.out.println( "Done" );
	}
}
