package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.CloudURI;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public abstract class ConvertTilesToN5Spark
{
	private static class ConvertTilesToN5CmdArgs implements Serializable
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

		public ConvertTilesToN5CmdArgs( final String... args ) throws IllegalArgumentException
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
				n5OutputPath = PathResolver.get( PathResolver.getParent( inputChannelsPaths.iterator().next() ), "tiles.n5" );
			}
		}
	}

	private static enum InputFileFormat
	{
		TIF,
		CZI
	}

	private static InputFileFormat detectInputFileFormat( final TileInfo tile )
	{
		if ( tile.getFilePath().toLowerCase().endsWith( ".czi" ) )
			return InputFileFormat.CZI;
		else if ( tile.getFilePath().toLowerCase().endsWith( ".tif" ) || tile.getFilePath().toLowerCase().endsWith( ".tiff" ) )
			return InputFileFormat.TIF;
		else
			throw new IllegalArgumentException( "Unknown file format: " + PathResolver.getFileName( tile.getFilePath() ) + ". Supported file formats are: czi, tif" );
	}

	private static InputFileFormat detectInputFileFormat( final List< String > inputChannelsPaths ) throws IOException
	{
		InputFileFormat inputFileFormat = null;
		for ( final String inputChannelPath : inputChannelsPaths )
		{
			final DataProvider inputDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputChannelPath ) );
			final TileInfo[] channelTiles = inputDataProvider.loadTiles( inputChannelPath );
			for ( final TileInfo tile : channelTiles )
			{
				if ( inputFileFormat == null )
					inputFileFormat = detectInputFileFormat( tile );
				else if ( inputFileFormat != detectInputFileFormat( tile ) )
					throw new IllegalArgumentException( "all tiles are expected to be in the same format (czi of tif)" );
			}
		}
		return inputFileFormat;
	}

	public static void main( final String... args ) throws IOException
	{
		final ConvertTilesToN5CmdArgs parsedArgs = new ConvertTilesToN5CmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			throw new IllegalArgumentException( "argument format mismatch" );

		final InputFileFormat inputFileFormat = detectInputFileFormat( parsedArgs.inputChannelsPaths );
		System.out.println( "Detected input file format: " + inputFileFormat );

		if ( inputFileFormat == InputFileFormat.CZI && parsedArgs.inputChannelsPaths.size() > 1 )
			throw new IllegalArgumentException( "expected only one tile configuration in czi format because each czi tile image contains all channels" );

		System.out.println( "Converting tiles to N5..." );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "ConvertTilesToN5Spark" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			switch ( inputFileFormat )
			{
			case TIF:
				ConvertTIFFTilesToN5Spark.convertTilesToN5(
						sparkContext,
						parsedArgs.inputChannelsPaths,
						parsedArgs.n5OutputPath,
						CmdUtils.parseIntArray( parsedArgs.blockSizeStr ),
						new GzipCompression()
					);
				break;
			case CZI:
				ConvertCZITilesToN5Spark.convertTilesToN5(
						sparkContext,
						parsedArgs.inputChannelsPaths.iterator().next(),
						parsedArgs.n5OutputPath,
						CmdUtils.parseIntArray( parsedArgs.blockSizeStr ),
						new GzipCompression()
					);
				break;
			default:
				throw new UnsupportedOperationException();
			}
		}
		System.out.println( "Done" );
	}
}
