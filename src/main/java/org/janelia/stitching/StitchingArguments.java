package org.janelia.stitching;

import java.io.Serializable;
import java.util.Arrays;

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
			usage = "Path to a tile registration JSON file.")
	private String inputFilePath;

	@Option(name = "-t", aliases = { "--threshold" }, required = false,
			usage = "A threshold value of cross correlation for accepting a shift")
	private double crossCorrelationThreshold = -1.0;

	@Option(name = "-f", aliases = { "--fusioncell" }, required = false,
			usage = "Size of an individual tile when fusing")
	private int fusionCellSize = 64;

	@Option(name = "-b", aliases = { "--blurstrength" }, required = false,
			usage = "Blur strength")
	private double blurStrength = 3.0;
	
	@Option(name = "-v", aliases = { "--voxelsize" }, required = false,
			usage = "Voxel dimensions")
	private String voxelDimensions = "nm=1.0,1.0,1.0";

	@Option(name = "--noroi", required = false,
			usage = "Compute phase correlation between full tile images instead of their ROIs")
	private boolean noRoi;
	
	@Option(name = "--adjacent", required = false,
			usage = "Compute phase correlation only between adjacent tiles (with large intersection plane)")
	private boolean adjacent;

	/**
	 * Toggle pipeline stages. By default all stages are executed.
	 */
	@Option(name = "--onlymeta", required = false, usage = "Query metadata for input tile images. Always included by default.")
	private boolean onlyMeta;

	@Option(name = "--onlyblur", required = false, usage = "Preapply Gaussian blur to all tiles and store the result in separate files")
	private boolean onlyBlur;
	@Option(name = "--noblur", required = false, usage = "Don't preapply Gaussian blur to all tiles and store the result in separate files")
	private boolean noBlur;

	@Option(name = "--onlyshift", required = false, usage = "Compute shifted tile positions")
	private boolean onlyShift;
	@Option(name = "--noshift", required = false, usage = "Don't compute shifted tile positions")
	private boolean noShift;

	@Option(name = "--onlyic", required = false, usage = "Perform intensity correction")
	private boolean onlyIntensityCorrection;
	@Option(name = "--noic", required = false, usage = "Don't perform intensity correction")
	private boolean noIntensityCorrection;

	@Option(name = "--onlyfuse", required = false, usage = "Fuse tiles")
	private boolean onlyFuse;
	@Option(name = "--nofuse", required = false, usage = "Don't fuse tiles")
	private boolean noFuse;

	@Option(name = "--onlyexport", required = false, usage = "Combine tiles into a single HDF5 file")
	private boolean onlyExport;
	@Option(name = "--noexport", required = false, usage = "Don't combine tiles into a single HDF5 file")
	private boolean noExport;


	private boolean parsedSuccessfully = false;

	public StitchingArguments( final String[] args ) {
		final CmdLineParser parser = new CmdLineParser( this );
		try {
			parser.parseArgument( args );
			parsedSuccessfully = true;
		} catch ( final CmdLineException e ) {
			System.err.println( e.getMessage() );
			parser.printUsage( System.err );
		}
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

	public String inputFilePath() { return inputFilePath; }
	public double crossCorrelationThreshold() { return crossCorrelationThreshold; }
	public int fusionCellSize() { return fusionCellSize; }
	public double blurStrength() { return blurStrength; }
	public boolean noRoi() { return noRoi; }
	public boolean adjacent() { return adjacent; }

	public boolean onlyMeta() { return onlyMeta; }

	public boolean onlyBlur() { return onlyBlur; }
	public boolean noBlur() { return noBlur; }

	public boolean onlyShift() { return onlyShift; }
	public boolean noShift() { return noShift; }

	public boolean onlyIntensityCorrection() { return onlyIntensityCorrection; }
	public boolean noIntensityCorrection() { return noIntensityCorrection; }

	public boolean onlyFuse() { return onlyFuse; }
	public boolean noFuse() { return noFuse; }

	public boolean onlyExport() { return onlyExport; }
	public boolean noExport() { return noExport; }
}
