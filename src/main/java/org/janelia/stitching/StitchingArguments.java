package org.janelia.stitching;

import java.io.Serializable;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * @author pisarevi
 *
 */

public class StitchingArguments implements Serializable {

	private static final long serialVersionUID = -8996450783846140673L;

	@Option(name = "-i", aliases = { "--input" }, required = true,
			usage = "Path to a tile registration JSON file.")
	private String input;

	@Option(name = "-t", aliases = { "--threshold" }, required = false,
			usage = "A threshold value of cross correlation for accepting a shift")
	private double crossCorrelationThreshold = -1.0;

	@Option(name = "-s", aliases = { "--subsize" }, required = false,
			usage = "Size of an individual tile when fusing")
	private int subregionSize = 64;

	@Option(name = "--meta", required = false,
			usage = "Only query the metadata of the tile images and save it to separate file")
	private boolean meta;

	@Option(name = "--nofuse", required = false,
			usage = "Don't fuse images and only output the resulting tile configuration")
	private boolean noFuse;

	@Option(name = "--fuseonly", required = false,
			usage = "Only fuse images assuming that input tile configuration is already correct")
	private boolean fuseOnly;

	@Option(name = "--hdf5", required = false,
			usage = "Combine a set of tiles into a single HDF5 file")
	private boolean hdf5;

	@Option(name = "--blur", required = false,
			usage = "Apply Gaussian blur to all tiles and store the result in separate files")
	private boolean blur;

	@Option(name = "--noroi", required = false,
			usage = "Compute phase correlation between full tile images instead of their ROIs")
	private boolean noRoi;

	@Option(name = "--noic", required = false,
			usage = "No intensity correction")
	private boolean noIntensityCorrection;

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

	public boolean parsedSuccessfully() {
		return parsedSuccessfully;
	}

	public String getInput() {
		return input;
	}

	public double getCrossCorrelationThreshold() {
		return crossCorrelationThreshold;
	}

	public int getSubregionSize() {
		return subregionSize;
	}

	public boolean getMeta() {
		return meta;
	}

	public boolean getNoFuse() {
		return noFuse;
	}

	public boolean getFuseOnly() {
		return fuseOnly;
	}

	public boolean getHdf5() {
		return hdf5;
	}

	public boolean getBlur() {
		return blur;
	}

	public boolean getNoRoi() {
		return noRoi;
	}

	public boolean getNoIntensityCorrection() {
		return noIntensityCorrection;
	}
}
