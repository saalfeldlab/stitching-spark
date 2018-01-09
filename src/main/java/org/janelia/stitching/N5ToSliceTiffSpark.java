package org.janelia.stitching;

import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadataReader;
import org.janelia.saalfeldlab.n5.spark.N5SliceTiffConverter;
import org.janelia.saalfeldlab.n5.spark.TiffUtils;

public class N5ToSliceTiffSpark
{
	private static final int SCALE_LEVEL = 0;
	private static final int SCALE_LEVEL_BINNED = 3;

	public static void main( final String[] args ) throws Exception
	{
		final String n5Path = args[ 0 ];
		final boolean binned;
		if ( args.length > 1 )
		{
			if ( args[ 1 ].equalsIgnoreCase( "--binned" ) )
				binned = true;
			else
				throw new Exception( "Unexpected argument. Possible values are: --binned" );
		}
		else
			binned = false;

		final int scaleLevel = binned ? SCALE_LEVEL_BINNED : SCALE_LEVEL;
		System.out.println( "Using scale level " + scaleLevel + " to generate slice TIFFs" );

		final String outBaseFolder = Paths.get( n5Path ).getParent().toString();
		final String outFolder = "slice-tiff" + ( binned ? "-binned" : "" );
		final String outputPath = Paths.get( outBaseFolder, outFolder ).toString();
		System.out.println( "Output path: " + outputPath );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf().setAppName( "ConvertN5ToSliceTIFF" ) ) )
		{
			final N5ExportMetadataReader exportMetadata = N5ExportMetadata.openForReading( N5.openFSReader( n5Path ) );
			for ( int channel = 0; channel < exportMetadata.getNumChannels(); ++channel )
			{
				final String n5DatasetPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, scaleLevel );
				final String outputChannelPath = Paths.get( outputPath, "ch" + channel ).toString();
				N5SliceTiffConverter.convertToSliceTiff(
						sparkContext,
						() -> N5.openFSReader( n5Path ),
						n5DatasetPath,
						outputChannelPath,
						TiffUtils.TiffCompression.LZW
					);
			}
		}
	}
}
