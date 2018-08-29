package org.janelia.stitching;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;

import org.janelia.dataaccess.CloudURI;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Command line arguments parser for a stitching job.
 *
 * @author Igor Pisarev
 */

public class StitchingArguments implements Serializable {

	public static enum RematchingMode implements Serializable
	{
		FULL,
		INCREMENTAL
	}

	private static final long serialVersionUID = -8996450783846140673L;

	@Option(name = "-i", aliases = { "--input" }, required = true,
			usage = "Path/link to a tile configuration JSON file. Multiple configurations can be passed at once.")
	private List< String > inputTileConfigurations;

	@Option(name = "-n", aliases = { "--neighbors" }, required = false,
			usage = "Min neighborhood for estimating confidence intervals using offset statistics")
	private int minStatsNeighborhood = 5;

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
			usage = "Rematching mode ('full' or 'incremental')")
	private String rematchingModeStr = "incremental";

	private RematchingMode rematchingMode = null;

	@Option(name = "--noleaves", required = false,
			usage = "Optimize tile configurations that don't contain any leaves (thus all edges are properly constrained)")
	private boolean noLeaves = false;

	@Option(name = "--overlaps", required = false,
			usage = "Export overlaps channel based on which connections between tiles have been used for final stitching")
	private boolean exportOverlaps = false;

	@Option(name = "--blending", required = false,
			usage = "Export the dataset using blending strategy instead of hardcut (max.min.distance)")
	private boolean blending = false;

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
			if ( rematchingModeStr.equalsIgnoreCase( "full" ) )
				rematchingMode = RematchingMode.FULL;
			else if ( rematchingModeStr.equalsIgnoreCase( "incremental" ) )
				rematchingMode = RematchingMode.INCREMENTAL;
			else
				throw new IllegalArgumentException( "Invalid rematching mode. Possible values are: 'full' or 'incremental'" );
		}

		// make sure that inputTileConfigurations contains absolute file paths if running on a traditional filesystem
		for ( int i = 0; i < inputTileConfigurations.size(); ++i )
			if ( !CloudURI.isCloudURI( inputTileConfigurations.get( i ) ) )
				inputTileConfigurations.set( i, Paths.get( inputTileConfigurations.get( i ) ).toAbsolutePath().toString() );
	}

	protected StitchingArguments() { }

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
	public int fusionCellSize() { return fusionCellSize; }
	public double blurSigma() { return blurSigma; }
	public boolean useAllPairs() { return allPairs; }
	public boolean noLeaves() { return noLeaves; }
	public boolean exportOverlaps() { return exportOverlaps; }
	public boolean blending() { return blending; }

	public boolean stitchOnly() { return stitchOnly; }
	public boolean fuseOnly() { return fuseOnly; }

	public RematchingMode rematchingMode() { return rematchingMode; }

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
