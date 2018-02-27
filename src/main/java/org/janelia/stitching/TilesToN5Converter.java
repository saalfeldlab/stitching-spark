package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudOAuth;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudResourceManagerClient;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageClient;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.janelia.util.ImageImporter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.auth.oauth2.AccessToken;
import com.google.cloud.storage.Storage;

import ij.ImagePlus;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;

abstract class TilesToN5Converter
{
	static class TilesToN5Arguments implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-i", aliases = { "--inputConfigurationPath" }, required = true,
				usage = "Path to an input tile configuration file. Multiple configurations (channels) can be passed at once.")
		private List< String > inputChannelsPath;

		@Option(name = "-o", aliases = { "--n5OutputPath" }, required = true,
				usage = "Path to an N5 output container (can be a filesystem path, an Amazon S3 link, or a Google Cloud link).")
		private String n5OutputPath;

		@Option(name = "-b", aliases = { "--blockSize" }, required = false,
				usage = "Output block size.")
		private int blockSize = 128;

		private boolean parsedSuccessfully = false;

		public TilesToN5Arguments( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}
		}

		public boolean parsedSuccessfully() { return parsedSuccessfully; }

		public List< String > getInputChannelsPath() { return inputChannelsPath; }
		public String getN5OutputPath() { return n5OutputPath; }
		public int getBlockSize() { return blockSize; }
	}

	static class CloudN5WriterSupplier implements N5WriterSupplier
	{
		private static final long serialVersionUID = -1199787780776971335L;

		private final URI n5Uri;
		private final DataAccessType type;

		private final AccessToken accessToken;
		private final String refreshToken;
		private final String clientId;
		private final String clientSecret;

		public CloudN5WriterSupplier( final String n5Path ) throws IOException
		{
			n5Uri = URI.create( n5Path );
			type = DataProviderFactory.getTypeByURI( n5Uri );

			if ( type == DataAccessType.GOOGLE_CLOUD )
			{
				final GoogleCloudOAuth oauth = new GoogleCloudOAuth(
						Arrays.asList(
								GoogleCloudResourceManagerClient.ProjectsScope.READ_ONLY,
								GoogleCloudStorageClient.StorageScope.READ_WRITE
							),
						"n5-viewer-google-cloud-oauth2",  // TODO: create separate application? currently using n5-viewer app id
						TilesToN5Converter.class.getResourceAsStream("/googlecloud_client_secrets.json")
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
			if ( type == DataAccessType.GOOGLE_CLOUD )
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
				return DataProviderFactory.createByType( type );
			}
		}

		@Override
		public N5Writer get() throws IOException
		{
			if ( type == DataAccessType.GOOGLE_CLOUD )
			{
				final DataProvider googleCloudDataProvider = getDataProvider();
				try
				{
					return googleCloudDataProvider.createN5Writer( n5Uri );
				}
				catch ( final Exception e )
				{
					if ( e instanceof IOException )
						throw e;
					else
						throw new RuntimeException( "Please create the desired output Google Cloud bucket first." );
				}
			}
			else
			{
				return getDataProvider().createN5Writer( n5Uri );
			}
		}
	}

	/**
	 * Converts a stack of TIFF images to N5 breaking the images down into cells of given size.
	 *
	 * @param inputTile
	 * @param n5
	 * @param outputPath
	 * @param blockSize
	 * @param n5Compression
	 * @return output dataset path for converted tile
	 * @throws IOException
	 */
	public static < T extends NumericType< T > & NativeType< T > > String convertTileToN5(
			final TileInfo inputTile,
			final N5Writer n5,
			final String outputGroupPath,
			final int blockSize,
			final Compression n5Compression ) throws IOException
	{
		final int dimensionality = inputTile.numDimensions();

		final int[] blockSizeArr = new int[ dimensionality ];
		Arrays.fill( blockSizeArr, blockSize );

		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final String tileDatasetPath = PathResolver.get( outputGroupPath, PathResolver.getFileName( inputTile.getFilePath() ) );
		final ImagePlus imp = ImageImporter.openImage( inputTile.getFilePath() );
		final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );
		N5Utils.save( img, n5, tileDatasetPath, blockSizeArr, n5Compression );

		return tileDatasetPath;
	}

	static Map< String, TileInfo[] > getTilesChannels( final List< String > inputChannelsPath ) throws IOException
	{
		final Map< String, TileInfo[] > tilesChannels = new LinkedHashMap<>();
		for ( final String inputPath : inputChannelsPath )
		{
			final String channelName = getChannelName( inputPath );
			final DataProvider inputDataProvider = DataProviderFactory.createByURI( URI.create( inputPath ) );
			final TileInfo[] channelTiles = TileInfoJSONProvider.loadTilesConfiguration( inputDataProvider.getJsonReader( URI.create( inputPath ) ) );
			tilesChannels.put( channelName, channelTiles );
		}
		return tilesChannels;
	}

	static String getChannelName( final String tileConfigPath )
	{
		final String filename = PathResolver.getFileName( tileConfigPath );
		final int lastDotIndex = filename.lastIndexOf( '.' );
		final String filenameWithoutExtension = lastDotIndex != -1 ? filename.substring( 0, lastDotIndex ) : filename;
		return filenameWithoutExtension;
	}

	static void saveTilesChannels( final List< String > inputChannelsPath, final Map< String, TileInfo[] > newTiles, final String n5Path ) throws IOException
	{
		final DataProvider dataProvider = new CloudN5WriterSupplier( n5Path ).getDataProvider();
		for ( final String inputPath : inputChannelsPath )
		{
			final String channelName = getChannelName( inputPath );
			final TileInfo[] newChannelTiles = newTiles.get( channelName );
			final String newConfigPath = PathResolver.get( n5Path, Utils.addFilenameSuffix( PathResolver.getFileName( inputPath ), "-converted-n5" ) );
			TileInfoJSONProvider.saveTilesConfiguration( newChannelTiles, dataProvider.getJsonWriter( URI.create( newConfigPath ) ) );
		}
	}
}
