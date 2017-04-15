package org.janelia.stitching;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.VoxelDimensions;

/**
 * Command line arguments parser for a stitching job.
 *
 * @author Igor Pisarev
 */

public class StitchingArguments implements Serializable {

	private static final long serialVersionUID = -8996450783846140673L;

	@Option(name = "-i", aliases = { "--input" }, required = true,
			usage = "Path to a tile configuration JSON file. Multiple configurations can be passed at once.")
	private List< String > inputTileConfigurations;

	@Option(name = "-t", aliases = { "--threshold" }, required = false,
			usage = "A threshold value of cross correlation for accepting a shift")
	private double crossCorrelationThreshold = -1.0;

	@Option(name = "-f", aliases = { "--fusioncell" }, required = false,
			usage = "Size of an individual tile when fusing")
	private int fusionCellSize = 64;

	@Option(name = "-b", aliases = { "--blursigma" }, required = false,
			usage = "Sigma value of the gaussian blur preapplied to the images before stitching")
	private double blurSigma = 1.0;

	@Option(name = "-r", aliases = { "--voxelsize" }, required = false,
			usage = "Voxel type and dimensions")
	private String voxelDimensions = "um=1.0,1.0,1.0";

	@Option(name = "-p", aliases = { "--padding" }, required = false,
			usage = "Padding for the overlap regions")
	private String padding = "0,0,0";

	@Option(name = "--adjacent", required = false,
			usage = "Compute pairwise shifts only between adjacent pairs of tiles")
	private boolean adjacent = true;

	@Option(name = "--overlaps", required = false,
			usage = "Export overlaps channel based on which connections between tiles have been used for final stitching")
	private boolean exportOverlaps = false;

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

		if ( stitchOnly && fuseOnly )
			throw new IllegalArgumentException( "Contradicting arguments: stitchOnly && fuseOnly" );
	}

	protected StitchingArguments() { }

	public boolean parsedSuccessfully() { return parsedSuccessfully; }

	public VoxelDimensions voxelDimensions()
	{
		final int delim = voxelDimensions.indexOf( "=" );
		final String unit = voxelDimensions.substring( 0, delim );
		final String[] tokens = voxelDimensions.substring( delim + 1 ).trim().split( "," );
		final double[] values = new double[ tokens.length ];
		for ( int i = 0; i < values.length; i++ )
			values[ i ] = Double.parseDouble( tokens[ i ] );
		System.out.println( String.format( "Voxel dimensions = %s (%s)", Arrays.toString( values ), unit ) );
		return new FinalVoxelDimensions( unit, values );
	}

	public long[] padding()
	{
		final String[] tokens = padding.split( "," );
		final long[] values = new long[ tokens.length ];
		for ( int i = 0; i < values.length; i++ )
			values[ i ] = Long.parseLong( tokens[ i ] );
		return values;
	}

	public List< String > inputTileConfigurations() { return inputTileConfigurations; }
	public double crossCorrelationThreshold() { return crossCorrelationThreshold; }
	public int fusionCellSize() { return fusionCellSize; }
	public double blurSigma() { return blurSigma; }
	public boolean adjacent() { return adjacent; }
	public boolean exportOverlaps() { return exportOverlaps; }

	public boolean stitchOnly() { return stitchOnly; }
	public boolean fuseOnly() { return fuseOnly; }
}
