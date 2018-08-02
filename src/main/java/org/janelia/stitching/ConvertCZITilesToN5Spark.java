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
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.CloudN5WriterSupplier;
import org.janelia.dataaccess.CloudURI;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.N5WriterSupplier;
import org.janelia.util.ImageImporter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImagePlus;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import scala.Tuple2;

public class ConvertCZITilesToN5Spark
{
	private static class ConvertCZITilesToN5CmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-i", aliases = { "--inputConfigurationPath" }, required = true,
				usage = "Path to an input tile configuration file.")
		private String inputPath;

		@Option(name = "-o", aliases = { "--n5OutputPath" }, required = true,
				usage = "Path to an N5 output container (can be a filesystem path, an Amazon S3 link, or a Google Cloud link).")
		private String n5OutputPath;

		@Option(name = "-b", aliases = { "--blockSize" }, required = false,
				usage = "Output block size.")
		private int blockSize = 128;

		private boolean parsedSuccessfully = false;

		public ConvertCZITilesToN5CmdArgs( final String... args ) throws IllegalArgumentException
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

			// make sure that inputTileConfiguration is an absolute file path if running on a traditional filesystem
			if ( !CloudURI.isCloudURI( inputPath ) )
				inputPath = Paths.get( inputPath ).toAbsolutePath().toString();

			// make sure that n5OutputPath is absolute if running on a traditional filesystem
			if ( !CloudURI.isCloudURI( n5OutputPath ) )
				n5OutputPath = Paths.get( n5OutputPath ).toAbsolutePath().toString();
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
	public static < T extends NumericType< T > & NativeType< T > > Map< String, TileInfo[] > convertCziToN5(
			final JavaSparkContext sparkContext,
			final TileInfo[] inputTiles,
			final String outputN5Path,
			final N5WriterSupplier n5Supplier,
			final int blockSize,
			final Compression n5Compression ) throws IOException
	{
		final int dimensionality = inputTiles[ 0 ].numDimensions();

		final int[] blockSizeArr = new int[ dimensionality ];
		Arrays.fill( blockSizeArr, blockSize );

		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final List< Tuple2< Integer, TileInfo > > outputChannelTileTuples = sparkContext
				.parallelize( Arrays.asList( inputTiles ) )
				.flatMap( inputTile ->
					{
						final List< String > outputTileChannelDatasetPaths = convertTileToN5(
								inputTile,
								n5Supplier.get(),
								blockSize,
								n5Compression
							);

						final List< Tuple2< Integer, TileInfo > > outputChannelTiles = new ArrayList<>();
						for ( int ch = 0; ch < outputTileChannelDatasetPaths.size(); ++ch )
						{
							final String outputChannelTilePath = PathResolver.get( outputN5Path, outputTileChannelDatasetPaths.get( ch ) );
							final TileInfo outputChannelTile = inputTile.clone();
							outputChannelTile.setFilePath( outputChannelTilePath );
							outputChannelTiles.add( new Tuple2<>( Integer.valueOf( ch ), outputChannelTile ) );
						}
						return outputChannelTiles.iterator();
					}
				).collect();

		// group tiles by channel index
		final Map< Integer, List< TileInfo > > tilesGroupedByChannels = new TreeMap<>();
		for ( final Tuple2< Integer, TileInfo > outputChannelTileTuple : outputChannelTileTuples )
		{
			final Integer channelIndex = outputChannelTileTuple._1();
			final TileInfo outputTile = outputChannelTileTuple._2();
			if ( !tilesGroupedByChannels.containsKey( channelIndex ) )
				tilesGroupedByChannels.put( channelIndex, new ArrayList<>() );
			tilesGroupedByChannels.get( channelIndex ).add( outputTile );
		}

		// convert to the expected format (mapping from channel name to an array of tiles)
		final Map< String, TileInfo[] > outputTilesChannels = new LinkedHashMap<>();
		for ( final Entry< Integer, List< TileInfo > > tilesChannelGroup : tilesGroupedByChannels.entrySet() )
			outputTilesChannels.put( getChannelName( tilesChannelGroup.getKey() ), tilesChannelGroup.getValue().toArray( new TileInfo[ 0 ] ) );

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
	public static < T extends NumericType< T > & NativeType< T > > List< String > convertTileToN5(
			final TileInfo inputTile,
			final N5Writer n5,
			final int blockSize,
			final Compression n5Compression ) throws IOException
	{
		final int dimensionality = inputTile.numDimensions();

		final int[] blockSizeArr = new int[ dimensionality ];
		Arrays.fill( blockSizeArr, blockSize );

		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		final ImagePlus imp = ImageImporter.openImage( inputTile.getFilePath() );
		final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );

		if ( img.numDimensions() >= 5 && img.dimension( 4 ) != 1 )
			throw new UnsupportedOperationException( "Multiple timepoints are not supported" );

		final List< String > channelTileDatasetPaths = new ArrayList<>();

		final boolean multichannel = img.numDimensions() > inputTile.numDimensions();
		final int channelDim = 2;

		for ( int ch = 0; ch < ( multichannel ? img.dimension( channelDim ) : 1 ); ++ch )
		{
			final RandomAccessibleInterval< T > channelImg = Views.dropSingletonDimensions( multichannel ? Views.hyperSlice( img, channelDim, ch ) : img );

			if ( !Intervals.equalDimensions( channelImg, new FinalInterval( inputTile.getSize() ) ) )
			{
				throw new RuntimeException( String.format(
						"Image size %s does not match the value from metadata %s, filepath: %s",
						Arrays.toString( Intervals.dimensionsAsLongArray( channelImg ) ),
						Arrays.toString( inputTile.getSize() ),
						inputTile.getFilePath()
					) );
			}

			final String channelTileDatasetPath = PathResolver.get( getChannelName( ch ), PathResolver.getFileName( inputTile.getFilePath() ) );
			N5Utils.save( channelImg, n5, channelTileDatasetPath, blockSizeArr, n5Compression );
			channelTileDatasetPaths.add( channelTileDatasetPath );
		}

		return channelTileDatasetPaths;
	}

	private static String getChannelName( final int channel )
	{
		return "c" + channel;
	}

	private static void saveTilesChannels( final String inputPath, final Map< String, TileInfo[] > newTiles, final String n5Path ) throws IOException
	{
		final DataProvider dataProvider = new CloudN5WriterSupplier( n5Path ).getDataProvider();
		for ( final Entry< String, TileInfo[] > tilesChannel : newTiles.entrySet() )
		{
			final String channelName = tilesChannel.getKey();
			final TileInfo[] newChannelTiles = tilesChannel.getValue();
			final String newConfigPath = PathResolver.get( n5Path, channelName + "_" + Utils.addFilenameSuffix( PathResolver.getFileName( inputPath ), "-converted-n5" ) );
			TileInfoJSONProvider.saveTilesConfiguration( newChannelTiles, dataProvider.getJsonWriter( URI.create( newConfigPath ) ) );
		}
	}

	public static void main( final String... args ) throws IOException
	{
		final ConvertCZITilesToN5CmdArgs parsedArgs = new ConvertCZITilesToN5CmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		final DataProvider inputDataProvider = DataProviderFactory.createByURI( URI.create( parsedArgs.inputPath ) );
		final TileInfo[] inputTiles = TileInfoJSONProvider.loadTilesConfiguration( inputDataProvider.getJsonReader( URI.create( parsedArgs.inputPath ) ) );

		final CloudN5WriterSupplier cloudN5WriterSupplier = new CloudN5WriterSupplier( parsedArgs.n5OutputPath );

		System.out.println( "Converting tiles to N5..." );

		final Map< String, TileInfo[] > outputTilesChannels;
		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "ConvertCZITilesToN5Spark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			outputTilesChannels = convertCziToN5(
					sparkContext,
					inputTiles,
					parsedArgs.n5OutputPath,
					cloudN5WriterSupplier,
					parsedArgs.blockSize,
					new GzipCompression()
				);
		}

		saveTilesChannels( parsedArgs.inputPath, outputTilesChannels, parsedArgs.n5OutputPath );
		System.out.println( "Done" );
	}
}
