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
	
	@Option(name = "-s", aliases = { "--subsize" }, required = false,
			usage = "Size of an individual tile when fusing")
	private int subregionSize;
	
	@Option(name = "--meta", required = false,
			usage = "Only query the metadata of the tile images and save it to separate file")
	private boolean meta;
	
	@Option(name = "--nofuse", required = false,
			usage = "Don't fuse images and only output the resulting tile configuration")
	private boolean noFuse;
	
	@Option(name = "--fuseonly", required = false,
			usage = "Only fuse images assuming that input tile configuration is already correct")
	private boolean fuseOnly;
	
	private boolean parsedSuccessfully = false;
	
	public StitchingArguments( String[] args ) {
		CmdLineParser parser = new CmdLineParser( this );
		try {
			parser.parseArgument( args );
			parsedSuccessfully = true;
		} catch ( CmdLineException e ) {
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
}
