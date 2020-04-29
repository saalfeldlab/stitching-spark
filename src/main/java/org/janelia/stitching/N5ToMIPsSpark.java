package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.*;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadataReader;
import org.janelia.saalfeldlab.n5.spark.N5MaxIntensityProjectionSpark;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils.TiffCompression;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class N5ToMIPsSpark
{
	private static class N5ToMIPsCmdArgs implements Serializable
	{
		private static final long serialVersionUID = 372184156792999688L;

		@Option(name = "-i", aliases = { "--input" }, required = true,
				usage = "Path to N5 export")
		private String n5Path;

		@Option(name = "-o", aliases = { "--outputPath" }, required = false,
				usage = "Output path to store MIPs.")
		private String outputPath;

		@Option(name = "-g", aliases = { "--grouping" }, required = false,
				usage = "Specifies how many slices in each dimension to include in a single MIP image (MIP step)."
						+ "By default, all slices are included in one group, so the result is simply a single MIP image in each dimension.")
		private String groupingStr = null;

		@Option(name = "-s", aliases = { "--scaleLevel" }, required = false,
				usage = "Scale level to use (0 corresponds to full resolution).")
		private int scaleLevel = 0;

		@Option(name = "-c", aliases = { "--compress" }, required = false,
				usage = "Compress generated TIFFs using LZW compression.")
		private boolean compressTiffs = false;

		private boolean parsedSuccessfully = false;

		public N5ToMIPsCmdArgs( final String... args ) throws IllegalArgumentException
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
				outputPath = PathResolver.get( PathResolver.getParent( n5Path ), "mip" + ( groupingStr != null ? "-" + groupingStr : "" ) + "-s" + scaleLevel );
			}
		}
	}

	public static void main( final String[] args ) throws Exception
	{
		final N5ToMIPsCmdArgs parsedArgs = new N5ToMIPsCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			throw new IllegalArgumentException( "argument format mismatch" );

		final TiffCompression tiffCompression = parsedArgs.compressTiffs ? TiffCompression.LZW : TiffCompression.NONE;
		System.out.println( "Output path: " + parsedArgs.outputPath );
		System.out.println( "Tiff compression: " + tiffCompression );

		final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( parsedArgs.n5Path ) );
		final DataProviderType dataProviderType = dataProvider.getType();

		final List< int[] > channelsCellDimensions = new ArrayList<>();
		final N5Reader n5 = dataProvider.createN5Reader( parsedArgs.n5Path, N5ExportMetadata.getGsonBuilder() );
		final N5ExportMetadataReader exportMetadata = N5ExportMetadata.openForReading( n5 );
		for ( int channel = 0; channel < exportMetadata.getNumChannels(); ++channel )
		{
			final String n5DatasetPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, parsedArgs.scaleLevel );
			final int[] cellDimensions = n5.getDatasetAttributes( n5DatasetPath ).getBlockSize();
			channelsCellDimensions.add( cellDimensions );
		}

		final long[] grouping = CmdUtils.parseLongArray( parsedArgs.groupingStr );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5ToMIPs" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.rdd.compress", "true" )
			) )
		{
			for ( int channel = 0; channel < channelsCellDimensions.size(); ++channel )
			{
				final String n5DatasetPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, parsedArgs.scaleLevel );
				final String outputChannelPath = PathResolver.get( parsedArgs.outputPath, "ch" + channel );
				final int[] cellDimensions = channelsCellDimensions.get( channel );
				final int[] mipStepsCells = new int[ cellDimensions.length ];
				for ( int d = 0; d < mipStepsCells.length; ++d )
				{
					if ( grouping == null || grouping[ d ] <= 0 )
						mipStepsCells[ d ] = Integer.MAX_VALUE;
					else
						mipStepsCells[ d ] = Math.max(
								( int ) Math.round(
										( ( double ) grouping[ d ] / cellDimensions[ d ] )
									),
								1
							);
				}

				N5MaxIntensityProjectionSpark.createMaxIntensityProjection(
						sparkContext,
						() -> {
							try {
								return DataProviderFactory.create( dataProviderType ).createN5Reader( parsedArgs.n5Path, N5ExportMetadata.getGsonBuilder() );
							} catch ( final IOException e ) {
								throw new RuntimeException( e );
							}
						},
						n5DatasetPath,
						mipStepsCells,
						outputChannelPath,
						new MultiBackendTiffWriter(),
						tiffCompression
					);
			}
		}
	}
}
