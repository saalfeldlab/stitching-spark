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
	
	protected StitchingArguments() {
	
	}
	
	public boolean parsedSuccessfully() {
		return parsedSuccessfully;
	}

	public String getInput() {
		return input;
	}
}
