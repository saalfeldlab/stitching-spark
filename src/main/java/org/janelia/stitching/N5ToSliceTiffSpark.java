package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.CloudURI;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadataReader;
import org.janelia.saalfeldlab.n5.spark.util.SliceDimension;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils.TiffCompression;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class N5ToSliceTiffSpark
{
	private static class N5ToSliceTiffCmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-i", aliases = { "--input" }, required = true,
				usage = "Path to N5 export")
		private String n5Path;

		@Option(name = "-o", aliases = { "--outputPath" }, required = false,
				usage = "Output path to store slice TIFFs.")
		private String outputPath;

		@Option(name = "-s", aliases = { "--scaleLevel" }, required = false,
				usage = "Scale level to use for conversion.")
		private int scaleLevel = 0;

		@Option(name = "-c", aliases = { "--compress" }, required = false,
				usage = "Compress generated TIFFs using LZW compression.")
		private boolean compressTiffs = false;

		@Option(name = "-d", aliases = { "--sameDir" }, required = false,
				usage = "Instead of storing each channel in a separate folder, store all TIFFs in the same folder and prepend the filenames with the channel index."
						+ " (for compatibility with Imaris)")
		private boolean storeInSameFolder = false;

		@Option(name = "-z", aliases = { "--leadingZeroes" }, required = false,
				usage = "Pad slice indices in the output filenames with leading zeroes.")
		private boolean useLeadingZeroes = false;

		private boolean parsedSuccessfully = false;

		public N5ToSliceTiffCmdArgs( final String... args ) throws IllegalArgumentException
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

			// make sure that input path is absolute if it's a filesystem path
			if ( !CloudURI.isCloudURI( n5Path ) )
				n5Path = Paths.get( n5Path ).toAbsolutePath().toString();

			if ( outputPath != null )
			{
				// make sure that output path is absolute if it's a filesystem path
				if ( !CloudURI.isCloudURI( outputPath ) )
					outputPath = Paths.get( outputPath ).toAbsolutePath().toString();
			}
			else
			{
				outputPath = PathResolver.get( PathResolver.getParent( n5Path ), "slice-tiff-s" + scaleLevel );
			}
		}
	}

	public static void main( final String[] args ) throws Exception
	{
		final N5ToSliceTiffCmdArgs parsedArgs = new N5ToSliceTiffCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			throw new IllegalArgumentException( "argument format mismatch" );

		final TiffCompression tiffCompression = parsedArgs.compressTiffs ? TiffCompression.LZW : TiffCompression.NONE;
		System.out.println( "Output path: " + parsedArgs.outputPath );
		System.out.println( "Tiff compression: " + tiffCompression );

		final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( parsedArgs.n5Path ) );
		final DataProviderType dataProviderType = dataProvider.getType();

		final N5Reader n5 = dataProvider.createN5Reader( parsedArgs.n5Path, N5ExportMetadata.getGsonBuilder() );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf().setAppName( "ConvertN5ToSliceTIFF" ) ) )
		{
			final N5ExportMetadataReader exportMetadata = N5ExportMetadata.openForReading( n5 );
			for ( int channel = 0; channel < exportMetadata.getNumChannels(); ++channel )
			{
				final String n5DatasetPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, parsedArgs.scaleLevel );
				final String outputChannelPath, filenamePrefix;
				if ( parsedArgs.storeInSameFolder )
				{
					outputChannelPath = parsedArgs.outputPath;
					filenamePrefix = "ch" + channel + "_";
				}
				else
				{
					outputChannelPath = PathResolver.get( parsedArgs.outputPath, "ch" + channel );
					filenamePrefix = "";
				}
				final String sliceIndexFormat = parsedArgs.useLeadingZeroes ? "%05d" : "%d";
				final String filenameFormat = filenamePrefix + sliceIndexFormat + ".tif";
				org.janelia.saalfeldlab.n5.spark.N5ToSliceTiffSpark.convert(
						sparkContext,
						() -> {
							try {
								return DataProviderFactory.create( dataProviderType ).createN5Reader( parsedArgs.n5Path, N5ExportMetadata.getGsonBuilder() );
							} catch ( final IOException e ) {
								throw new RuntimeException( e );
							}
						},
						n5DatasetPath,
						outputChannelPath,
						tiffCompression,
						SliceDimension.Z,
						filenameFormat
					);
			}
		}
	}
}
