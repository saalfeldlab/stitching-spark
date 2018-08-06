package org.janelia.stitching;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadataReader;
import org.janelia.saalfeldlab.n5.spark.N5SliceTiffConverter;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils;

public class N5ToSliceTiffSpark
{
	private static final int SCALE_LEVEL = 0;
	private static final int SCALE_LEVEL_BINNED = 3;

	public static void main( final String[] args ) throws Exception
	{
		int lastArg = 0;
		final String n5Path = Paths.get( args[ lastArg++ ] ).toAbsolutePath().toString();

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

		// FIXME: does not really work with AWS/GoogleCloud because its parent will be null.
		final String outBaseFolder = PathResolver.getParent( n5Path );
		final String outFolder = "slice-tiff" + ( binned ? "-binned" : "" );
		final String outputPath = PathResolver.get( outBaseFolder, outFolder );
		System.out.println( "Output path: " + outputPath );
		System.out.println( "Tiff compression: none" );

		System.out.println( requestedChannel != null ? "Processing channel " + requestedChannel : "Processing all channels" );

		final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( n5Path ) );
		final DataProviderType dataProviderType = dataProvider.getType();

		final N5Reader n5 = dataProvider.createN5Reader( n5Path, N5ExportMetadata.getGsonBuilder() );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf().setAppName( "ConvertN5ToSliceTIFF" ) ) )
		{
			final N5ExportMetadataReader exportMetadata = N5ExportMetadata.openForReading( n5 );
			for ( int channel = 0; channel < exportMetadata.getNumChannels(); ++channel )
			{
				if ( requestedChannel != null && channel != requestedChannel.intValue() )
					continue;

				final String n5DatasetPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, scaleLevel );
				final String outputChannelPath = PathResolver.get( outputPath, "ch" + channel );
				N5SliceTiffConverter.convertToSliceTiff(
						sparkContext,
						() -> {
							try {
								return DataProviderFactory.create( dataProviderType ).createN5Reader( n5Path, N5ExportMetadata.getGsonBuilder() );
							} catch ( final IOException e ) {
								throw new RuntimeException( e );
							}
						},
						n5DatasetPath,
						outputChannelPath,
						TiffUtils.TiffCompression.NONE,
						2 // xy slices
					);
			}
		}
	}
}
