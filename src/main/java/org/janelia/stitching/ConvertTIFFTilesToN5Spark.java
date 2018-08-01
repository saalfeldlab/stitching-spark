package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.CloudURI;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudOAuth;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudResourceManagerClient;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageClient;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
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
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.util.Intervals;
import scala.Tuple3;

public class ConvertTIFFTilesToN5Spark
{
	private static final int MAX_PARTITIONS = 15000;

	private static class TilesToN5Arguments implements Serializable
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

			// make sure that inputTileConfigurations contains absolute file paths if running on a traditional filesystem
			for ( int i = 0; i < inputChannelsPath.size(); ++i )
			{
				if ( !CloudURI.isCloudURI( inputChannelsPath.get( i ) ) )
				{
					inputChannelsPath.set( i, Paths.get( inputChannelsPath.get( i ) ).toAbsolutePath().toString() );
				}
			}
			// make sure that n5OutputPath is absolute if running on a traditional filesystem
			if ( !CloudURI.isCloudURI( n5OutputPath ) )
				n5OutputPath = Paths.get( n5OutputPath ).toAbsolutePath().toString();
		}
	}

	private static class CloudN5WriterSupplier implements N5WriterSupplier
	{
		private static final long serialVersionUID = -1199787780776971335L;

		private final URI n5Uri;
		private final DataProviderType type;

		private final AccessToken accessToken;
		private final String refreshToken;
		private final String clientId;
		private final String clientSecret;

		public CloudN5WriterSupplier( final String n5Path ) throws IOException
		{
			n5Uri = URI.create( n5Path );
			type = DataProviderFactory.getTypeByURI( n5Uri );

			if ( type == DataProviderType.GOOGLE_CLOUD )
			{
				final GoogleCloudOAuth oauth = new GoogleCloudOAuth(
						Arrays.asList(
								GoogleCloudResourceManagerClient.ProjectsScope.READ_ONLY,
								GoogleCloudStorageClient.StorageScope.READ_WRITE
							),
						"n5-viewer-google-cloud-oauth2",  // TODO: create separate application? currently using n5-viewer app id
						ConvertTIFFTilesToN5Spark.class.getResourceAsStream("/googlecloud_client_secrets.json")
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
				return DataProviderFactory.createByType( type );
			}
		}

		@Override
		public N5Writer get() throws IOException
		{
			if ( type == DataProviderType.GOOGLE_CLOUD )
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
	 * Converts a collection of TIFF tile images to N5 breaking the images down into cells of given size using Spark.
	 *
	 * @param sparkContext
	 * 			Spark context
	 * @param outputPath
	 * 			Base N5 path for constructing new tile paths
	 * @param n5Supplier
	 * 			{@link N5Writer} supplier
	 * @param tilesChannels
	 * 			Input tile configurations for channels
	 * @param blockSize
	 * 			Output block size
	 * @param compression
	 * 			Output N5 compression
	 * @return
	 * 			New tile configurations with updated paths
	 * @throws IOException
	 */
	public static < T extends NumericType< T > & NativeType< T > > Map< String, TileInfo[] > convertTiffToN5(
			final JavaSparkContext sparkContext,
			final Map< String, TileInfo[] > inputTilesChannels,
			final String outputN5Path,
			final N5WriterSupplier n5Supplier,
			final int blockSize,
			final Compression n5Compression ) throws IOException
	{
		final int dimensionality = inputTilesChannels.values().iterator().next()[ 0 ].numDimensions();

		final int[] blockSizeArr = new int[ dimensionality ];
		Arrays.fill( blockSizeArr, blockSize );

		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final List< Tuple3< String, Integer, TileInfo > > inputChannelIndexTileTuples = new ArrayList<>();
		for ( final Entry< String, TileInfo[] > entry : inputTilesChannels.entrySet() )
		{
			final String channelName = entry.getKey();
			n5Supplier.get().createGroup( channelName );

			for ( int i = 0; i < entry.getValue().length; ++i )
				inputChannelIndexTileTuples.add( new Tuple3<>( entry.getKey(), i, entry.getValue()[ i ] )  );
		}

		final List< Tuple3< String, Integer, TileInfo > > outputChannelIndexTileTuples = sparkContext
				.parallelize( inputChannelIndexTileTuples, Math.min( inputChannelIndexTileTuples.size(), MAX_PARTITIONS ) )
				.map( inputChannelIndexTileTuple ->
					{
						final String channelName = inputChannelIndexTileTuple._1();
						final Integer index = inputChannelIndexTileTuple._2();
						final TileInfo inputTile = inputChannelIndexTileTuple._3();

						final String outputTileDatasetPath = convertTileToN5(
								inputTile,
								n5Supplier.get(),
								channelName,
								blockSize,
								n5Compression
							);

						final String outputTilePath = PathResolver.get( outputN5Path, outputTileDatasetPath );
						final TileInfo outputTile = inputTile.clone();
						outputTile.setFilePath( outputTilePath );
						return new Tuple3<>( channelName, index, outputTile );
					}
				).collect();

		final Map< String, TileInfo[] > outputTilesChannels = new LinkedHashMap<>();
		for ( final Entry< String, TileInfo[] > entry : inputTilesChannels.entrySet() )
			outputTilesChannels.put( entry.getKey(), new TileInfo[ entry.getValue().length ] );

		for ( final Tuple3< String, Integer, TileInfo > outputChannelIndexTileTuple : outputChannelIndexTileTuples )
		{
			final String channelName = outputChannelIndexTileTuple._1();
			final Integer index = outputChannelIndexTileTuple._2();
			final TileInfo outputTile = outputChannelIndexTileTuple._3();
			outputTilesChannels.get( channelName )[ index ] = outputTile;
		}

		return outputTilesChannels;
	}

	/**
	 * Converts a tile image to N5 breaking the images down into cells of given size.
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

		if ( !Intervals.equalDimensions( img, new FinalInterval( inputTile.getSize() ) ) )
		{
			throw new RuntimeException( String.format(
					"Image size %s does not match the value from metadata %s, filepath: %s",
					Arrays.toString( Intervals.dimensionsAsLongArray( img ) ),
					Arrays.toString( inputTile.getSize() ),
					inputTile.getFilePath()
				) );
		}

		N5Utils.save( img, n5, tileDatasetPath, blockSizeArr, n5Compression );
		return tileDatasetPath;
	}

//	private static boolean checkDimensionality( final Interval img, final TileInfo metadata )
//	{
//		if ( img.numDimensions() != metadata.numDimensions() )
//		{
//			if ( img.numDimensions() == 5 ) // must be x,y,c,z,t
//			{
//				if ( ( metadata.numDimensions() > 2 ? metadata.getSize( 2 ) : 1 ) != img.dimension( 3 ) ) // check slices
//					return false;
//
//				if ( img.dimension( 4 ) != 1 ) // check that there is a single timepoint (multiple timepoints are not supported)
//					return false;
//
//				return true;
//			}
//			else
//				return false;
//		}
//		else
//			return true;
//	}

//	private static < T extends NumericType< T > & NativeType< T > > List< RandomAccessibleInterval< T > > getChannelImgs( final RandomAccessibleInterval< T > img )
//	{
//		if ( img.numDimensions() == 5 ) // must be x,y,c,z,t
//		{
//			final List< RandomAccessibleInterval< T > > channelImgs = new ArrayList<>();
//			final int channelDim = 2;
//			for ( int ch = 0; ch < img.dimension( channelDim ); ++ch )
//				channelImgs.add( Views.dropSingletonDimensions( Views.hyperSlice( img, channelDim, ch ) ) );
//			return channelImgs;
//		}
//		else
//			return Collections.singletonList( img );
//	}

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

	public static void main( final String... args ) throws IOException
	{
		final TilesToN5Arguments parsedArgs = new TilesToN5Arguments( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		final Map< String, TileInfo[] > inputTilesChannels = getTilesChannels( parsedArgs.inputChannelsPath );
		final CloudN5WriterSupplier cloudN5WriterSupplier = new CloudN5WriterSupplier( parsedArgs.n5OutputPath );

		System.out.println( "Converting tiles to N5..." );

		final Map< String, TileInfo[] > outputTilesChannels;
		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "ConvertTIFFTilesToN5Spark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			outputTilesChannels = convertTiffToN5(
					sparkContext,
					inputTilesChannels,
					parsedArgs.n5OutputPath,
					cloudN5WriterSupplier,
					parsedArgs.blockSize,
					new GzipCompression()
				);
		}

		saveTilesChannels( parsedArgs.inputChannelsPath, outputTilesChannels, parsedArgs.n5OutputPath );
		System.out.println( "Done" );
	}
}
