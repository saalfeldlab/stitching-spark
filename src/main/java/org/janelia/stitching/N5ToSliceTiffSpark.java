package org.janelia.stitching;

import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.spark.N5SliceTiffConverter;
import org.janelia.saalfeldlab.n5.spark.TiffUtils;

public class N5ToSliceTiffSpark
{
	private static final int SCALE_LEVEL = 0;
	private static final int SCALE_LEVEL_BINNED = 3;

	public static void main( final String[] args ) throws Exception
	{
		int lastArg = 0;
		final String n5Path = args[ lastArg++ ];

		final Integer requestedChannel;
		{
			if ( args.length > lastArg )
			{
				Integer requestedChannelParsed;
				try
				{
					requestedChannelParsed = Integer.parseInt( args[ lastArg ] );
					lastArg++;
				}
				catch ( final NumberFormatException e )
				{
					requestedChannelParsed = null;
				}
				requestedChannel = requestedChannelParsed;
			}
			else
			{
				requestedChannel = null;
			}
		}

		final boolean binned = args.length > lastArg && args[ lastArg++ ].equalsIgnoreCase( "--binned" );

		final int scaleLevel = binned ? SCALE_LEVEL_BINNED : SCALE_LEVEL;
		System.out.println( "Using scale level " + scaleLevel + " to generate slice TIFFs" );

		final String outBaseFolder = Paths.get( n5Path ).getParent().toString();
		final String outFolder = "slice-tiff" + ( binned ? "-binned" : "" );
		final String outputPath = Paths.get( outBaseFolder, outFolder ).toString();
		System.out.println( "Output path: " + outputPath );
		System.out.println( "Tiff compression: none" );

		System.out.println( requestedChannel != null ? "Processing channel " + requestedChannel : "Processing all channels" );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf().setAppName( "ConvertN5ToSliceTIFF" ) ) )
		{
			final N5ExportMetadata exportMetadata = new N5ExportMetadata( n5Path );
			for ( int channel = 0; channel < exportMetadata.getNumChannels(); ++channel )
			{
				if ( requestedChannel != null && channel != requestedChannel.intValue() )
					continue;

				final String n5DatasetPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, scaleLevel );
				final String outputChannelPath = Paths.get( outputPath, "ch" + channel ).toString();
				N5SliceTiffConverter.convertToSliceTiff( sparkContext, n5Path, n5DatasetPath, outputChannelPath, TiffUtils.TiffCompression.LZW );
			}
		}
	}
}
