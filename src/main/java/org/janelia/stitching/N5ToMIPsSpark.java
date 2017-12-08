package org.janelia.stitching;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadataReader;
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

		final String outBaseFolder = PathResolver.getParent( n5Path );
		final String slicingSuffix = ( slicing != null ? "-" + arrayToString( slicing ) : "" );
		final String binnedSuffix = ( binned ? "-binned" : "" );
		final String outFolder = "MIP" + slicingSuffix + binnedSuffix;
		final String outputPath = PathResolver.get( outBaseFolder, outFolder );
		System.out.println( "Output path: " + outputPath );

		final DataProvider dataProvider = DataProviderFactory.createByURI( URI.create( n5Path ) );
		final DataProviderType dataProviderType = dataProvider.getType();

		final List< int[] > channelsCellDimensions = new ArrayList<>();
		final N5Reader n5 = dataProvider.createN5Reader( URI.create( n5Path ), N5ExportMetadata.getGsonBuilder() );
		final N5ExportMetadataReader exportMetadata = N5ExportMetadata.openForReading( n5 );
		for ( int channel = 0; channel < exportMetadata.getNumChannels(); ++channel )
		{
			final String n5DatasetPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, scaleLevel );
			final int[] cellDimensions = n5.getDatasetAttributes( n5DatasetPath ).getBlockSize();
			channelsCellDimensions.add( cellDimensions );
		}

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5ToMIPs" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.rdd.compress", "true" )
			) )
		{
			for ( int channel = 0; channel < channelsCellDimensions.size(); ++channel )
			{
				final String n5DatasetPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, scaleLevel );
				final String outputChannelPath = PathResolver.get( outputPath, "ch" + channel );
				final int[] cellDimensions = channelsCellDimensions.get( channel );
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
						() -> {
							try {
								return DataProviderFactory.createByType( dataProviderType ).createN5Reader( URI.create( n5Path ), N5ExportMetadata.getGsonBuilder() );
							} catch ( final IOException e ) {
								throw new RuntimeException( e );
							}
						},
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
