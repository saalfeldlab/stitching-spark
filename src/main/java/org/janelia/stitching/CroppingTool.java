package org.janelia.stitching;

import java.io.Serializable;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class CroppingTool
{
	private static class CroppingToolCmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-i", aliases = { "--input" }, required = true,
				usage = "Path to the n5 export")
		private String inputN5Path;

		@Option(name = "-o", aliases = { "--output" }, required = true,
				usage = "Path to the output folder")
		private String outputPath;

		@Option(name = "-c", aliases = { "--channel" }, required = true,
				usage = "Channel")
		private int channel;

		@Option(name = "--min", required = true,
				usage = "Min (in pixels)")
		private String minStr;

		@Option(name = "--max", required = true,
				usage = "Max (in pixels)")
		private String maxStr;

		private boolean parsedSuccessfully = false;

		public CroppingToolCmdArgs( final String... args ) throws IllegalArgumentException
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
	}

	public static < T extends NativeType< T > & RealType< T > > void main( final String[] args ) throws Exception
	{
		final CroppingToolCmdArgs parsedArgs = new CroppingToolCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( parsedArgs.inputN5Path ) );
		final N5Reader n5 = dataProvider.createN5Reader( parsedArgs.inputN5Path );
		final RandomAccessibleInterval< T > img = N5Utils.open( n5, N5ExportMetadata.getScaleLevelDatasetPath( parsedArgs.channel, 0 ) );
		final Interval cropInterval = new FinalInterval( CmdUtils.parseLongArray( parsedArgs.minStr ), CmdUtils.parseLongArray( parsedArgs.maxStr ) );
		final ImagePlusImg< T, ? > imgCrop = new ImagePlusImgFactory< T >().create( cropInterval, Util.getTypeFromInterval( img ) );
		final Cursor< T > imgCursor = Views.flatIterable( Views.offsetInterval( Views.extendZero( img ), cropInterval ) ).cursor();
		final Cursor< T > imgCropCursor = Views.flatIterable( imgCrop ).cursor();
		while ( imgCropCursor.hasNext() || imgCursor.hasNext() )
			imgCropCursor.next().set( imgCursor.next() );
		final ImagePlus imp = imgCrop.getImagePlus();
		Utils.workaroundImagePlusNSlices( imp );
		dataProvider.saveImage( imp, PathResolver.get( parsedArgs.outputPath, parsedArgs.minStr + "_" + parsedArgs.maxStr + ".tif" ) );
	}
}
