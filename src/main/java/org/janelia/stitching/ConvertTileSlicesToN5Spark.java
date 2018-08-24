package org.janelia.stitching;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.util.ImageImporter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

/**
 * Takes a tile configuration where filepaths are regular expressions to tile slices.
 * Creates a single 3D image for each tile from the slices and saves it as N5.
 * Writes out new tile configuration pointing to the converted N5 tiles.
 */
public class ConvertTileSlicesToN5Spark implements Serializable, AutoCloseable
{
	private static final long serialVersionUID = -8600903133731319708L;

	private static class ConvertTileSlicesToN5CmdArgs implements Serializable
	{
		private static final long serialVersionUID = -8996450783846140673L;

		@Option(name = "-i", aliases = { "--input" }, required = true,
				usage = "Path/link to a tile configuration JSON file. Multiple configurations can be passed at once meaning multiple channels.")
		public List< String > inputTileConfigurations;

		@Option(name = "-o", aliases = { "--output" }, required = true,
				usage = "Output location (filesystem directory or cloud bucket) to store the resulting tiles and configurations.")
		public String outputLocation;

		@Option(name = "-b", aliases = { "--blockSize" }, required = true,
				usage = "N5 block size.")
		public String blockSizeStrArr;

		public boolean parsedSuccessfully = false;

		public ConvertTileSlicesToN5CmdArgs( final String[] args ) throws IllegalArgumentException
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

		public int[] getBlockSize()
		{
			return CmdUtils.parseIntArray( blockSizeStrArr );
		}
	}

	public static void main( final String[] args ) throws Exception
	{
		final ConvertTileSlicesToN5CmdArgs cmdArgs = new ConvertTileSlicesToN5CmdArgs( args );
		if ( !cmdArgs.parsedSuccessfully )
			System.exit( 1 );

		try ( final ConvertTileSlicesToN5Spark driver = new ConvertTileSlicesToN5Spark( cmdArgs ) )
		{
			driver.run();
		}
	}

	private final ConvertTileSlicesToN5CmdArgs args;
	private final transient JavaSparkContext sparkContext;

	private ConvertTileSlicesToN5Spark( final ConvertTileSlicesToN5CmdArgs args ) throws IOException
	{
		this.args = args;

		// create N5 container to prevent race conditions
		final DataProvider targetDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( args.outputLocation ) );
		targetDataProvider.createN5Writer( args.outputLocation );

		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "ConvertTileSlicesToN5" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);
	}

	private void run() throws IOException
	{
		for ( final String inputTileConfiguration : args.inputTileConfigurations )
			processChannel( inputTileConfiguration );
	}

	private void processChannel( final String inputTileConfiguration ) throws IOException
	{
		final DataProvider sourceDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputTileConfiguration ) );
		final TileInfo[] patternTiles = sourceDataProvider.loadTiles( inputTileConfiguration );
		final List< TileInfo > convertedTiles = sparkContext.parallelize( Arrays.asList( patternTiles ), patternTiles.length ).map(
				patternTile -> convertTileToN5( patternTile )
			).collect();

		final DataProvider targetDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( args.outputLocation ) );
		final String convertedTilesConfigurationFilename = Utils.addFilenameSuffix( PathResolver.getFileName( inputTileConfiguration ), "-converted-n5" );
		final String convertedTilesConfigurationPath = PathResolver.get( args.outputLocation, convertedTilesConfigurationFilename );
		targetDataProvider.saveTiles( convertedTiles.toArray( new TileInfo[ 0 ] ), convertedTilesConfigurationPath );
	}

	@SuppressWarnings( "unchecked" )
	private < T extends NativeType< T > & RealType< T > > TileInfo convertTileToN5( final TileInfo patternTile ) throws IOException
	{
		final DataProvider targetDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( args.outputLocation ) );
		final N5Writer n5 = targetDataProvider.createN5Writer( args.outputLocation );

		final Path tileSlicesBaseDirPath = Paths.get( patternTile.getFilePath() ).getParent();
		final String tileSlicesFileNamePattern = PathResolver.getFileName( patternTile.getFilePath() );
		final List< String > tileSlicesImagePaths = getTileSlicesImagePaths( tileSlicesBaseDirPath, tileSlicesFileNamePattern );
		System.out.println( "Found " + tileSlicesImagePaths.size() + " slice images for tile " + patternTile.getIndex() );

		final Matcher matcher = Pattern.compile( tileSlicesFileNamePattern ).matcher( PathResolver.getFileName( tileSlicesImagePaths.iterator().next() ) );
		if (!matcher.find() || matcher.groupCount() != 1 )
			throw new RuntimeException( "filename pattern is incorrect" );
		final int matchStart = matcher.start( 1 ), matchEnd = matcher.end( 1 );
		final String convertedTileDatasetName = matcher.group().substring( 0, matchStart ) + matcher.group().substring( matchEnd );

		final int[] blockSize = args.getBlockSize();
		final T type = ( T ) patternTile.getType().getType();
		n5.createDataset( convertedTileDatasetName, patternTile.getSize(), blockSize, N5Utils.dataType( type ), new GzipCompression() );

		int partialTileImgStartingSlice = 0;
		RandomAccessibleInterval< T > partialTileImg = null;
		for ( int slice = 0; slice < tileSlicesImagePaths.size(); ++slice )
		{
			if ( partialTileImg == null )
			{
				final long[] partialTileImgSize = patternTile.getSize().clone();
				partialTileImgSize[ 2 ] = Math.min( blockSize[ 2 ], tileSlicesImagePaths.size() - slice );
				partialTileImg = new ArrayImgFactory< T >().create( partialTileImgSize, type );
				partialTileImgStartingSlice = slice;
			}

			final RandomAccessibleInterval< T > tileSliceImg = ImagePlusImgs.from( ImageImporter.openImage( tileSlicesImagePaths.get( slice ) ) );
			final Cursor< T > tileSliceImgCursor = Views.flatIterable( tileSliceImg ).cursor();
			final Cursor< T > fullTileSliceCursor = Views.flatIterable( Views.hyperSlice( partialTileImg, 2, slice - partialTileImgStartingSlice ) ).cursor();
			while ( fullTileSliceCursor.hasNext() || tileSliceImgCursor.hasNext() )
				fullTileSliceCursor.next().set( tileSliceImgCursor.next() );

			if ( slice - partialTileImgStartingSlice + 1 == partialTileImg.dimension( 2 ) )
			{
				N5Utils.saveBlock( partialTileImg, n5, convertedTileDatasetName, new long[] { 0, 0, partialTileImgStartingSlice / blockSize[ 2 ] } );
				partialTileImg = null;
			}
		}

		final TileInfo convertedTile = patternTile.clone();
		convertedTile.setFilePath( PathResolver.get( args.outputLocation, convertedTileDatasetName ) );
		return convertedTile;
	}

	private List< String > getTileSlicesImagePaths( final Path baseImagesDirPath, final String fileNamePattern )
	{
		final DataProvider sourceDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( baseImagesDirPath.toString() ) );
		if ( sourceDataProvider.getType() != DataProviderType.FILESYSTEM )
			throw new IllegalArgumentException( "Only filesystem storage type is supported for input data" );

		final FilenameFilter fileNameFilter = new FilenameFilter()
		{
			@Override
			public boolean accept( final File dir, final String name )
			{
				return name.matches( fileNamePattern );
			}
		};

		final TreeMap< Integer, String > sortedTileSlicesImagePaths = new TreeMap<>();
		final String[] fileList = baseImagesDirPath.toFile().list( fileNameFilter );

		final Pattern pattern = Pattern.compile( fileNamePattern );
		for ( final String fileName : fileList )
		{
			final Matcher matcher = pattern.matcher( fileName );
			if (!matcher.find() || matcher.groupCount() != 1 )
				throw new RuntimeException( "filename pattern is incorrect" );
			final int sliceIndex = Integer.parseInt( matcher.group( 1 ) );

			sortedTileSlicesImagePaths.put( sliceIndex, PathResolver.get( baseImagesDirPath.toString(), fileName ) );
		}

		if ( sortedTileSlicesImagePaths.lastKey().intValue() - sortedTileSlicesImagePaths.firstKey().intValue() + 1 != sortedTileSlicesImagePaths.size() )
			throw new RuntimeException( "some of the slices are missing" );

		return new ArrayList<>( sortedTileSlicesImagePaths.values() );
	}

	@Override
	public void close() throws Exception
	{
		sparkContext.close();
	}
}
