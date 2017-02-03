package org.janelia.stitching;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Interval;
import net.imglib2.SerializableFinalInterval;

/**
 * Command line arguments parser for illumination correction.
 *
 * @author Igor Pisarev
 */

public class IlluminationCorrectionArguments
{
	@Option(name = "-i", aliases = { "--input" }, required = true,
			usage = "Path to a tile configuration JSON file")
	private String inputFilePath;

	@Option(name = "--crop", required = false,
			usage = "Crop interval in a form of xMin,yMin,zMin,xMax,yMax,zMax")
	private String cropMinMaxInterval = null;

	@Option(name = "-b", aliases = { "--bins" }, required = false,
			usage = "Number of bins to use")
	private int bins = 256;

	@Option(name = "--min", required = false,
			usage = "Min value of a histogram")
	private Integer histMinValue = null;

	@Option(name = "--max", required = false,
			usage = "Max value of a histogram")
	private Integer histMaxValue = null;


	private boolean parsedSuccessfully = false;

	public IlluminationCorrectionArguments( final String[] args ) throws CmdLineException
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

	public boolean parsedSuccessfully() { return parsedSuccessfully; }

	public String inputFilePath() { return inputFilePath; }
	public int bins() { return bins; }
	public Integer histMinValue() { return histMinValue; }
	public Integer histMaxValue() { return histMaxValue; }
	public String cropMinMaxIntervalStr() { return cropMinMaxInterval; };

	public Interval cropMinMaxInterval( final long[] fullTileSize )
	{
		if ( cropMinMaxInterval == null )
			return SerializableFinalInterval.createMinSize( 0,0,0, fullTileSize[0],fullTileSize[1],fullTileSize[2] );

		final String[] intervalStrSplit = cropMinMaxInterval.trim().split(",");
		final long[] intervalMinMax = new long[ intervalStrSplit.length ];
		for ( int i = 0; i < intervalMinMax.length; i++ )
			intervalMinMax[ i ] = Long.parseLong( intervalStrSplit[ i ] );
		return SerializableFinalInterval.createMinMax( intervalMinMax );
	}
}
