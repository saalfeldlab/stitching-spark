package org.janelia.stitching;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.janelia.util.ImageImporter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.esotericsoftware.kryo.Kryo;

import ij.ImagePlus;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import scala.Tuple2;

public class TilesToN5Converter
{
	/**
	 * Converts a stack of TIFF images to N5 breaking the images down into cells
	 * with the block size in Z adjusted to the pixel resolution of the data.
	 *
	 * @param sparkContext
	 * 			Spark context instantiated with {@link Kryo} serializer
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
			final String outputPath,
			final N5WriterSupplier n5Supplier,
			final Map< String, TileInfo[] > tilesChannels,
			final int blockSize,
			final CompressionType n5Compression ) throws IOException
	{
		final int dimensionality = tilesChannels.values().iterator().next()[ 0 ].numDimensions();

		final int[] blockSizeArr = new int[ dimensionality ];
		Arrays.fill( blockSizeArr, blockSize );

		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final Map< String, TileInfo[] > ret = new LinkedHashMap<>();

		for ( final Entry< String, TileInfo[] > entry : tilesChannels.entrySet() )
		{
			final String channelName = entry.getKey();
			n5Supplier.get().createGroup( channelName );

			final Map< Integer, String > newTilePaths = sparkContext.parallelize( Arrays.asList( entry.getValue() ), entry.getValue().length ).mapToPair( tile ->
				{
					final N5Writer n5 = n5Supplier.get();
					final String tileDatasetPath = PathResolver.get( channelName, PathResolver.getFileName( tile.getFilePath() ) );
					final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
					final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );
					N5Utils.save( img, n5, tileDatasetPath, blockSizeArr, n5Compression );
					return new Tuple2<>( tile.getIndex(), PathResolver.get( outputPath, tileDatasetPath ) );
				}
			).collectAsMap();

			final TileInfo[] newTiles = new TileInfo[ entry.getValue().length ];
			for ( int i = 0; i < newTiles.length; ++i )
			{
				newTiles[ i ] = entry.getValue()[ i ].clone();
				newTiles[ i ].setFilePath( newTilePaths.get( newTiles[ i ].getIndex() ) );
			}

			ret.put( channelName, newTiles );
		}

		return ret;
	}

	public static void main( final String... args ) throws IOException
	{
		final Arguments parsedArgs = new Arguments( args );
		if ( !parsedArgs.parsedSuccessfully() )
			System.exit( 1 );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "TilesToN5Spark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final DataProvider tileConfigDataProvider = DataProviderFactory.createByURI( URI.create( parsedArgs.getInputChannelsPath().get( 0 ) ) );
			final Map< String, TileInfo[] > tilesChannels = new LinkedHashMap<>();
			for ( final String inputPath : parsedArgs.getInputChannelsPath() )
			{
				final String channelName = getChannelName( inputPath );
				final TileInfo[] channelTiles = TileInfoJSONProvider.loadTilesConfiguration( tileConfigDataProvider.getJsonReader( URI.create( inputPath ) ) );
				tilesChannels.put( channelName, channelTiles );
			}

			final URI n5Uri = URI.create( parsedArgs.getN5OutputPath() );
			final N5WriterSupplier n5Supplier = () -> DataProviderFactory.createByURI( n5Uri ).createN5Writer( n5Uri );

			final Map< String, TileInfo[] > newTiles = convertTiffToN5(
					sparkContext,
					parsedArgs.getN5OutputPath(),
					n5Supplier,
					tilesChannels,
					parsedArgs.getBlockSize(),
					parsedArgs.getN5Compression()
				);

			for ( final String inputPath : parsedArgs.getInputChannelsPath() )
			{
				final String channelName = getChannelName( inputPath );
				final TileInfo[] newChannelTiles = newTiles.get( channelName );
				final String newConfigPath = Utils.addFilenameSuffix( inputPath, "-converted-n5" );
				TileInfoJSONProvider.saveTilesConfiguration( newChannelTiles, tileConfigDataProvider.getJsonWriter( URI.create( newConfigPath ) ) );
			}
		}

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static String getChannelName( final String tileConfigPath )
	{
		final String filename = PathResolver.getFileName( tileConfigPath );
		final int lastDotIndex = filename.lastIndexOf( '.' );
		final String filenameWithoutExtension = lastDotIndex != -1 ? filename.substring( 0, lastDotIndex ) : filename;
		return filenameWithoutExtension;
	}

	private static class Arguments
	{
		@Option(name = "-i", aliases = { "--inputConfigurationPath" }, required = true,
				usage = "Path to an input tile configuration file. Multiple configurations (channels) can be passed at once.")
		private List< String > inputChannelsPath;

		@Option(name = "-o", aliases = { "--n5OutputPath" }, required = true,
				usage = "Path to an N5 output container (can be a filesystem path or an Amazon S3 link).")
		private String n5OutputPath;

		@Option(name = "-b", aliases = { "--blockSize" }, required = false,
				usage = "Output block size.")
		private int blockSize = 64;

		@Option(name = "-c", aliases = { "--n5Compression" }, required = false,
				usage = "N5 compression.")
		private CompressionType n5Compression = CompressionType.GZIP;

		private boolean parsedSuccessfully = false;

		public Arguments( final String... args ) throws IllegalArgumentException
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
		public CompressionType getN5Compression() { return n5Compression; }
	}
}
