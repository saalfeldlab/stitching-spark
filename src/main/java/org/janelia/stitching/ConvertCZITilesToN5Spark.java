package org.janelia.stitching;

import ij.ImagePlus;
import ij.process.ImageProcessor;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.*;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.util.ImageImporter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.Tuple3;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;

public class ConvertCZITilesToN5Spark
{
	public static final String tilesN5ContainerName = "tiles.n5";

	private static class ConvertCZITilesToN5CmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-i", aliases = { "--inputConfigurationPath" }, required = true,
				usage = "Path to an input tile configuration file. Multiple configurations (channels) can be passed at once.")
		private List< String > inputChannelsPaths;

		@Option(name = "-o", aliases = { "--n5OutputPath" }, required = false,
				usage = "Path to an N5 output container (can be a filesystem path, an Amazon S3 link, or a Google Cloud link).")
		private String n5OutputPath;

		@Option(name = "-b", aliases = { "--blockSize" }, required = false,
				usage = "Output block size as a comma-separated list.")
		private String blockSizeStr = "128,128,64";

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

			// make sure that inputTileConfigurations contains absolute file paths if running on a traditional filesystem
			for ( int i = 0; i < inputChannelsPaths.size(); ++i )
				if ( !CloudURI.isCloudURI( inputChannelsPaths.get( i ) ) )
					inputChannelsPaths.set( i, Paths.get( inputChannelsPaths.get( i ) ).toAbsolutePath().toString() );

			if ( n5OutputPath != null )
			{
				// make sure that n5OutputPath is absolute if running on a traditional filesystem
				if ( !CloudURI.isCloudURI( n5OutputPath ) )
					n5OutputPath = Paths.get( n5OutputPath ).toAbsolutePath().toString();
			}
			else
			{
				n5OutputPath = PathResolver.get( PathResolver.getParent( inputChannelsPaths.iterator().next() ), tilesN5ContainerName );
			}
		}
	}

	private static final int MAX_PARTITIONS = 15000;

	public static void main( final String... args ) throws IOException
	{
		final ConvertCZITilesToN5CmdArgs parsedArgs = new ConvertCZITilesToN5CmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			throw new IllegalArgumentException( "argument format mismatch" );

		System.out.println( "Converting tiles to N5..." );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "ConvertCZITilesToN5Spark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			run(
					sparkContext,
					parsedArgs.inputChannelsPaths.iterator().next(),
					parsedArgs.n5OutputPath,
					CmdUtils.parseIntArray( parsedArgs.blockSizeStr ),
					new GzipCompression()
			);
		}
		System.out.println( "Done" );
	}

	public static void run(
			final JavaSparkContext sparkContext,
			final String inputTilesPath,
			final String outputN5Path,
			final int[] blockSize,
			final Compression n5Compression ) throws IOException
	{
		final DataProvider inputDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputTilesPath ) );
		final TileInfo[] inputTiles = inputDataProvider.loadTiles( inputTilesPath );

		final CloudN5WriterSupplier cloudN5WriterSupplier = new CloudN5WriterSupplier( outputN5Path );

		final Map< String, TileInfo[] > outputTilesChannels = convertTilesToN5(
				sparkContext,
				inputTiles,
				outputN5Path,
				cloudN5WriterSupplier,
				blockSize,
				n5Compression
			);

		saveTilesChannels( inputTilesPath, outputTilesChannels );
	}

	// Used by the parser so this step doesn't need to open the CZI file again to find out the number of channels
	// before submitting data conversion tasks.
	public static void createTargetDirectories(
			final String outputN5Path,
			final int numChannels) throws IOException
	{
		final CloudN5WriterSupplier cloudN5WriterSupplier = new CloudN5WriterSupplier( outputN5Path );
		final N5Writer n5 = cloudN5WriterSupplier.get();
		for ( int ch = 0; ch < numChannels; ++ch )
		{
			final String channelName = getChannelName( ch );
			n5.createGroup( channelName );
		}
	}

	public static < T extends NumericType< T > & NativeType< T > > Map< String, TileInfo[] > convertTilesToN5(
			final JavaSparkContext sparkContext,
			final TileInfo[] inputTiles,
			final String outputN5Path,
			final N5WriterSupplier n5Supplier,
			final int[] blockSize,
			final Compression n5Compression ) throws IOException
	{
		// TODO: can consider pixel resolution to calculate isotropic block size in Z

		// find out whether tile images are stored in separate .czi files or in a single .czi container
		final boolean singleCziContainer = Arrays.asList( inputTiles ).stream().map( tileInfo -> tileInfo.getFilePath() ).distinct().count() == 1;

		// NOTE: the channel groups should already exist because they are created in the previous step: ParseCZITilesMetadata
		final N5Writer n5 = n5Supplier.get();
		final int numChannels = n5.list( "" ).length;
		for ( int ch = 0; ch < numChannels; ++ch )
		{
			if ( !n5.exists( getChannelName( ch ) ) )
				throw new RuntimeException( "Channel groups should already exist in N5 container because they are created in the previous step of the pipeline" );
		}

		// create output datasets for the tiles
		System.out.println( "Detected " + numChannels + " channels" );
		for ( int ch = 0; ch < numChannels; ++ch )
		{
			for ( final TileInfo tile : inputTiles )
			{
				n5.createDataset(
						getChannelTileDataset( ch, tile, singleCziContainer ),
						tile.getSize(),
						blockSize,
						N5Utils.dataType( ( T ) tile.getType().getType() ),
						n5Compression
				);
			}
		}

		// create tasks, parallelizing over channels, tiles, and intervals (which are grouped into Z chunks aligned with the block size and cover the entire XY plane of the tile)
		final List< Tuple3< Integer, TileInfo, Interval > > tasks = new ArrayList<>();
		for ( final TileInfo tile : inputTiles )
		{
			for ( long zPos = 0; zPos < tile.getSize( 2 ); zPos += blockSize[ 2 ] )
			{
				final Interval taskInterval = new FinalInterval(
						new long[] { 0, 0, zPos },
						new long[] { tile.getSize(0) - 1, tile.getSize(1) - 1, Math.min(zPos + blockSize[2] - 1, tile.getSize(2) - 1) }
				);
				for ( int ch = 0; ch < numChannels; ++ch )
					tasks.add( new Tuple3<>( ch, tile, taskInterval ) );
			}
		}

		sparkContext
				.parallelize( tasks, Math.min( tasks.size(), MAX_PARTITIONS ) )
				.foreach( channelAndInputTileAndInterval ->
						{
							final int channel = channelAndInputTileAndInterval._1();
							final TileInfo inputTile = channelAndInputTileAndInterval._2();
							final Interval interval = channelAndInputTileAndInterval._3();

							if ( inputTile.numDimensions() != blockSize.length )
								throw new RuntimeException( "dimensionality mismatch" );

							System.out.println("Opening image...");
							long elapsedMsec = System.currentTimeMillis();
							final ImagePlus[] imps = ImageImporter.openBioformatsImageSeries( inputTile.getFilePath() );
							elapsedMsec = System.currentTimeMillis() - elapsedMsec;
							System.out.println("Opened, took " + (elapsedMsec / 1000) + "s");

							final ImagePlus imp;
							if ( singleCziContainer )
							{
								if ( inputTile.getIndex() >= imps.length )
									throw new RuntimeException( "Identified that all tile images are stored in a single .czi container, but there are not enough images in the loaded image series (file=" + inputTile.getFilePath() + ", numImages=" + imps.length + ", tileIndex=" + inputTile.getIndex() );
								imp = imps[ inputTile.getIndex() ];
							}
							else
							{
								if ( imps.length != 1 )
									throw new RuntimeException( "Expected one tile image per .czi file, got " + imps.length + " images in file " + inputTile.getFilePath() );
								imp = imps[ 0 ];
							}

							if ( imp.getNChannels() != numChannels )
								throw new RuntimeException( "Number of channels in the metadata doesn't match the actual number of channels: " +
										"metadata=" + numChannels + ", actual=" + imp.getNChannels() );

							System.out.println( String.format(
									"Converting interval min=%s,max=%s of tile %d, channel %d into N5",
									Arrays.toString( Intervals.minAsLongArray( interval ) ),
									Arrays.toString( Intervals.maxAsLongArray( interval ) ),
									inputTile.getIndex(),
									channel
								) );

							final T type = ( T ) inputTile.getType().getType();
							final RandomAccessibleInterval< T > dstImg = Views.translate(
									new ArrayImgFactory<>( type ).create(
											imp.getWidth(),
											imp.getHeight(),
											interval.dimension( 2 )
										),
									Intervals.minAsLongArray( interval )
								);

							for ( int zPos = ( int ) interval.min( 2 ); zPos <= interval.max( 2 ); ++zPos )
							{
								imp.setPosition( channel + 1, zPos + 1, imp.getT() );
								final ImageProcessor processor = imp.getProcessor();
								final ImagePlus sliceImp = new ImagePlus( "tile" + inputTile.getIndex() + ",c" + channel + ",z" + zPos, processor );
								final RandomAccessibleInterval< T > srcSliceImg = ImagePlusImgs.from( sliceImp );
								final Cursor< T > srcCursor = Views.flatIterable( srcSliceImg ).cursor();
								final IntervalView< T > dstSliceImg = Views.hyperSlice( dstImg, 2, zPos );
								final Cursor<T> dstCursor = Views.flatIterable( dstSliceImg ).cursor();
								while ( srcCursor.hasNext() || dstCursor.hasNext() )
									dstCursor.next().set( srcCursor.next() );
							}

							System.out.println("Saving interval into N5...");
							final long[] gridOffset = new long[ blockSize.length ];
							Arrays.setAll( gridOffset, d -> dstImg.min( d ) / blockSize[ d ] );
							N5Utils.saveBlock(
									dstImg,
									n5Supplier.get(),
									getChannelTileDataset( channel, inputTile, singleCziContainer ),
									gridOffset
								);
							System.out.println("Saving interval into N5: done");
						}
				);

		// create output tiles metadata
		final Map< String, TileInfo[] > outputTilesChannels = new LinkedHashMap<>();
		for ( int ch = 0; ch < numChannels; ++ch )
		{
			final List< TileInfo > outputTiles = new ArrayList<>();
			for ( final TileInfo inputTile : inputTiles )
			{
				final TileInfo outputTile = inputTile.clone();
				final String outputTileDatasetPath = getChannelTileDataset( ch, inputTile, singleCziContainer );
				outputTile.setFilePath( PathResolver.get( outputN5Path, outputTileDatasetPath ) );
				outputTiles.add( outputTile );
			}
			outputTilesChannels.put( getChannelName( ch ), outputTiles.toArray( new TileInfo[ outputTiles.size() ] ) );
		}

		return outputTilesChannels;
	}

	private static String getChannelName( final int channel )
	{
		return "c" + channel;
	}

	private static String getChannelTileDataset( final int channel, final TileInfo tile, final boolean singleCziContainer )
	{
		final String channelName = getChannelName( channel );
		return PathResolver.get(
				channelName,
				channelName + "_" + PathResolver.getFileName( tile.getFilePath() + ( singleCziContainer ? "_tile" + tile.getIndex() : "" ) )
		);
	}

	private static void saveTilesChannels( final String inputPath, final Map< String, TileInfo[] > newTiles ) throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputPath ) );
		final String baseDir = PathResolver.getParent( inputPath );
		for ( final Entry< String, TileInfo[] > tilesChannel : newTiles.entrySet() )
		{
			final String channelName = tilesChannel.getKey();
			final TileInfo[] newChannelTiles = tilesChannel.getValue();
			final String newConfigPath = PathResolver.get( baseDir, channelName + "-n5.json" );
			dataProvider.saveTiles( newChannelTiles, newConfigPath );
		}
	}
}
