package org.janelia.stitching;

import java.io.Serializable;
import java.util.List;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Command line arguments parser for a stitching job.
 *
 * @author Igor Pisarev
 */

public class StitchingArguments implements Serializable {

	public static enum RestitchingMode implements Serializable
	{
		FULL,
		INCREMENTAL
	}

	private static final long serialVersionUID = -8996450783846140673L;

	@Option(name = "-i", aliases = { "--input" }, required = true,
			usage = "Path to a tile configuration JSON file. Multiple configurations can be passed at once.")
	private List< String > inputTileConfigurations;

	@Option(name = "-n", aliases = { "--neighbors" }, required = false,
			usage = "Min neighborhood for estimating confidence intervals using offset statistics")
	private int minStatsNeighborhood = 5;

	@Option(name = "-r", aliases = { "--radius" }, required = false,
			usage = "Search radius multiplier (error ellipse size)")
	private double searchRadiusMultiplier = 3;

	@Option(name = "-w", aliases = { "--searchwindow" }, required = false,
			usage = "Search window size for local offset statistics in terms of number of tiles")
	private String statsWindowSizeTiles = "3,3,3";

	@Option(name = "-f", aliases = { "--fusioncell" }, required = false,
			usage = "Size of an individual tile when fusing")
	private int fusionCellSize = 128;

	@Option(name = "-b", aliases = { "--blursigma" }, required = false,
			usage = "Sigma value of the gaussian blur preapplied to the images before stitching")
	private double blurSigma = 2.0;

	@Option(name = "-p", aliases = { "--padding" }, required = false,
			usage = "Padding for the overlap regions")
	private String padding = "0,0,0";

	@Option(name = "--min", required = false,
			usage = "Min coordinate for exporting")
	private String minCoord = null;

	@Option(name = "--max", required = false,
			usage = "Max coordinate for exporting")
	private String maxCoord = null;

	@Option(name = "--allpairs", required = false,
			usage = "Compute pairwise shifts between all pairs (by default only adjacent pairs are used)")
	private boolean allPairs = false;

	@Option(name = "-m", aliases = { "--mode" }, required = false,
			usage = "Mode for restitching ('restitching-full' or 'restitching-incremental')")
	private String restitchingModeStr = "";

	private RestitchingMode restitchingMode = null;

	@Option(name = "--overlaps", required = false,
			usage = "Export overlaps channel based on which connections between tiles have been used for final stitching")
	private boolean exportOverlaps = false;

	@Option(name = "--blending", required = false,
			usage = "Export the dataset using blending strategy instead of hardcut (max.min.distance)")
	private boolean blending = false;

	@Option(name = "--maxintensity", required = false,
			usage = "Export the dataset using max intensity strategy instead of hardcut (max.min.distance)")
	private boolean maxIntensity = false;

	@Option(name = "-s", aliases = { "--split" }, required = false,
			usage = "Number of parts the overlap is split in along every 'long' edge")
	private int splitOverlapParts = 2;

	/**
	 * Toggle pipeline stages. By default all stages are executed.
	 */
	@Option(name = "--stitch", required = false, usage = "Only stitch the tiles, i.e. find optimal positions")
	private boolean stitchOnly = false;

	@Option(name = "--fuse", required = false, usage = "Only fuse the tiles, i.e. export the image in the CellFileViewer format")
	private boolean fuseOnly = false;


	private boolean parsedSuccessfully = false;

	public StitchingArguments( final String[] args ) throws IllegalArgumentException
	{
		final CmdLineParser parser = new CmdLineParser( this );
		try {
			parser.parseArgument( args );
			parsedSuccessfully = true;
		} catch ( final CmdLineException e ) {
			System.err.println( e.getMessage() );
			parser.printUsage( System.err );
		}

		if ( !stitchOnly && !fuseOnly )
			throw new IllegalArgumentException( "Please specify mode: --stitch / --fuse" );

		if ( stitchOnly && fuseOnly )
			throw new IllegalArgumentException( "Please specify one mode at a time: --stitch / --fuse" );

		if ( !fuseOnly )
		{
			if ( restitchingModeStr.equalsIgnoreCase( "restitching-full" ) )
				restitchingMode = RestitchingMode.FULL;
			else if ( restitchingModeStr.equalsIgnoreCase( "restitching-incremental" ) )
				restitchingMode = RestitchingMode.INCREMENTAL;
			else
				throw new IllegalArgumentException( "Invalid restitching mode. Possible values are: 'restitching-full' or 'restitching-incremental'" );
		}

		if ( blending && maxIntensity )
			throw new IllegalArgumentException( "fusion strategy specified incorrectly" );
	}

	protected StitchingArguments() { }

	public static StitchingArguments defaultFusionArgs()
	{
		final StitchingArguments args = new StitchingArguments();
		args.fuseOnly = true;
		return args;
	}

	public boolean parsedSuccessfully() { return parsedSuccessfully; }

	public long[] padding()
	{
		return parseArray( padding );
	}

	public long[] minCoord()
	{
		return parseArray( minCoord );
	}
	public long[] maxCoord()
	{
		return parseArray( maxCoord );
	}

	public List< String > inputTileConfigurations() { return inputTileConfigurations; }
	public int minStatsNeighborhood() { return minStatsNeighborhood; }
	public double searchRadiusMultiplier() { return searchRadiusMultiplier; }
	public int fusionCellSize() { return fusionCellSize; }
	public double blurSigma() { return blurSigma; }
	public boolean useAllPairs() { return allPairs; }
	public boolean exportOverlaps() { return exportOverlaps; }
	public boolean blending() { return blending; }
	public boolean maxIntensity() { return maxIntensity; }
	public int splitOverlapParts() { return splitOverlapParts; }

	public boolean stitchOnly() { return stitchOnly; }
	public boolean fuseOnly() { return fuseOnly; }

	public RestitchingMode restitchingMode() { return restitchingMode; }

	private long[] parseArray( final String str )
	{
		if ( str == null )
			return null;

		final String[] tokens = str.split( "," );
		final long[] values = new long[ tokens.length ];
		for ( int i = 0; i < values.length; i++ )
			values[ i ] = Long.parseLong( tokens[ i ] );
		return values;
	}
}
