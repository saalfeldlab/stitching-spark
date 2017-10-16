package org.janelia.stitching;

import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.spark.N5MaxIntensityProjection;
import org.janelia.saalfeldlab.n5.spark.TiffUtils;

public class N5ToMIPsSpark
{
	private static final int SCALE_LEVEL = 0;
	private static final int SCALE_LEVEL_BINNED = 3;

	public static void main( final String[] args ) throws Exception
	{
		int argCounter = 0;
		final String n5Path = args[ argCounter++ ];

		final boolean binned;
		final long[] slicing;

		if ( args.length > argCounter )
		{
			if ( args[ argCounter ].equalsIgnoreCase( "--binned" ) )
			{
				binned = true;
				if ( args.length > ++argCounter )
					throw new Exception( "Unexpected argument. Usage: ./mip-n5.sh <nodes> <n5 export path> [<slicing>] [--binned]" );
				slicing = null;
			}
			else
			{
				slicing = parseLongArray( args[ argCounter++ ] );
				if ( args.length > argCounter )
				{
					if ( args[ argCounter++ ].equalsIgnoreCase( "--binned" ) )
						binned = true;
					else
						throw new Exception( "Unexpected argument. Usage: ./mip-n5.sh <nodes> <n5 export path> [<slicing>] [--binned]" );
				}
				else
				{
					binned = false;
				}
			}
		}
		else
		{
			binned = false;
			slicing = null;
		}

		final int scaleLevel = binned ? SCALE_LEVEL_BINNED : SCALE_LEVEL;
		System.out.println( "Using scale level " + scaleLevel + " to generate MIPs" );

		final String outBaseFolder = Paths.get( n5Path ).getParent().toString();
		final String slicingSuffix = ( slicing != null ? "-" + arrayToString( slicing ) : "" );
		final String binnedSuffix = ( binned ? "-binned" : "" );
		final String outFolder = "MIP" + slicingSuffix + binnedSuffix;
		final String outputPath = Paths.get( outBaseFolder, outFolder ).toString();
		System.out.println( "Output path: " + outputPath );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5ToMIPs" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.rdd.compress", "true" )
			) )
		{
			final N5ExportMetadata exportMetadata = new N5ExportMetadata( n5Path );
			for ( int channel = 0; channel < exportMetadata.getNumChannels(); ++channel )
			{
				final String n5DatasetPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, scaleLevel );
				final String outputChannelPath = Paths.get( outputPath, "ch" + channel ).toString();

				final N5Writer n5 = N5.openFSWriter( n5Path );
				final DatasetAttributes attributes = n5.getDatasetAttributes( n5DatasetPath );
				final int[] cellDimensions = attributes.getBlockSize();

				final int[] mipStepsCells = new int[ cellDimensions.length ];
				for ( int d = 0; d < mipStepsCells.length; ++d )
				{
					if ( slicing == null || slicing[ d ] <= 0 )
						mipStepsCells[ d ] = Integer.MAX_VALUE;
					else
						mipStepsCells[ d ] = Math.max(
								( int ) Math.round(
										( ( double ) slicing[ d ] / cellDimensions[ d ] )
									),
								1
							);
				}

				N5MaxIntensityProjection.createMaxIntensityProjection(
						sparkContext,
						n5,
						n5DatasetPath,
						mipStepsCells,
						outputChannelPath,
						TiffUtils.TiffCompression.NONE
					);
			}
		}
	}

	private static long[] parseLongArray( final String str )
	{
		final String[] tokens = str.trim().split( "," );
		final long[] ret = new long[ tokens.length ];
		for ( int i = 0; i < ret.length; i++ )
			ret[ i ] = Long.parseLong( tokens[ i ] );
		return ret;
	}

	private static String arrayToString( final long[] arr )
	{
		final StringBuilder sb = new StringBuilder();
		for ( final long val : arr )
			sb.append( sb.length() > 0 ? "," : "" ).append( val );
		return sb.toString();
	}
}
