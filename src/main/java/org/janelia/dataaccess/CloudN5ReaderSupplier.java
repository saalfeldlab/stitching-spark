package org.janelia.dataaccess;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudOAuth;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudOAuth.Scope;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudResourceManagerClient;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageClient;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.spark.N5ReaderSupplier;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.auth.oauth2.AccessToken;
import com.google.cloud.storage.Storage;

public class CloudN5ReaderSupplier implements N5ReaderSupplier
{
	private static final long serialVersionUID = -1199787780776971335L;

	protected final String n5Link;
	protected final DataProviderType type;

	protected final AccessToken accessToken;
	protected final String refreshToken;
	protected final String clientId;
	protected final String clientSecret;

	public CloudN5ReaderSupplier( final String n5Path ) throws IOException
	{
		this( n5Path, Arrays.asList(
				GoogleCloudResourceManagerClient.ProjectsScope.READ_ONLY,
				GoogleCloudStorageClient.StorageScope.READ_ONLY
			) );
	}

	protected CloudN5ReaderSupplier( final String n5Link, final Collection< ? extends Scope > googleCloudScopes ) throws IOException
	{
		this.n5Link = n5Link;
		type = DataProviderFactory.detectType( n5Link );

		if ( type == DataProviderType.GOOGLE_CLOUD )
		{
			final GoogleCloudOAuth oauth = new GoogleCloudOAuth(
					googleCloudScopes,
					"n5-viewer-google-cloud-oauth2",  // TODO: create separate application? currently using n5-viewer app id
					getClass().getResourceAsStream("/googlecloud_client_secrets.json")
				);

			accessToken = oauth.getAccessToken();
			refreshToken = oauth.getRefreshToken();
			final GoogleClientSecrets clientSecrets = oauth.getClientSecrets();
			clientId = clientSecrets.getDetails().getClientId();
			clientSecret = clientSecrets.getDetails().getClientSecret();
		}
		else
		{
			accessToken = null;
			refreshToken = null;
			clientId = null;
			clientSecret = null;
		}
	}

	public DataProvider getDataProvider()
	{
		if ( type == DataProviderType.GOOGLE_CLOUD )
		{
			final GoogleClientSecrets.Details clientSecretsDetails = new GoogleClientSecrets.Details();
			clientSecretsDetails.setClientId( clientId );
			clientSecretsDetails.setClientSecret( clientSecret );
			final GoogleClientSecrets clientSecrets = new GoogleClientSecrets();
			clientSecrets.setInstalled( clientSecretsDetails );

			final GoogleCloudStorageClient storageClient = new GoogleCloudStorageClient(
					accessToken,
					clientSecrets,
					refreshToken
				);

			final Storage storage = storageClient.create();
			return DataProviderFactory.createGoogleCloudDataProvider( storage );
		}
		else
		{
			return DataProviderFactory.create( type );
		}
	}

	@Override
	public N5Reader get() throws IOException
	{
		return getDataProvider().createN5Reader( n5Link );
	}
}
