package org.janelia.stitching;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.janelia.dataaccess.CloudURI;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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

	@Option(name = "--correction-images-paths", required = false,
			usage = "Path/link to correction images")
	private List< String > correctionImagesPaths;

	@Option(name = "-r", aliases = { "--registrationChannelIndex" }, required = false,
			usage = "Index of the input channel to be used for registration (indexing starts from 0). If omitted or equal to -1, all input channels will be used for registration by averaging the tile images.")
	private Integer registrationChannelIndex = null;

	@Option(name = "-n", aliases = { "--minNeighbors" }, required = false,
			usage = "Min neighborhood for estimating confidence intervals using offset statistics")
	private int minStatsNeighborhood = 5;

	@Option(name = "-w", aliases = { "--searchWindow" }, required = false,
			usage = "Search window size for local offset statistics in terms of number of tiles")
	private String statsWindowSizeTiles = "3,3,3";

	@Option(name = "-f", aliases = { "--n5BlockSize" }, required = false,
			usage = "X/Y N5 block size in the exported volume")
	private int fusionCellSize = 128;

	@Option(name = "-b", aliases = { "--blurSigma" }, required = false,
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

	@Option(name = "--allPairs", required = false,
			usage = "Compute pairwise shifts between all pairs (by default only adjacent pairs are used)")
	private boolean allPairs = false;

	@Option(name = "-m", aliases = { "--mode" }, required = false,
			usage = "Rematching mode ('full' or 'incremental')")
	private String rematchingModeStr = "incremental";

	private RematchingMode rematchingMode = null;

	@Option(name = "--noLeaves", required = false,
			usage = "Optimize tile configurations that don't contain any leaves (thus all edges are properly constrained)")
	private boolean noLeaves = false;

	@Option(name = "--overlaps", required = false,
			usage = "Export overlaps channel based on which connections between tiles have been used for final stitching")
	private boolean exportOverlaps = false;

	@Option(name = "--blending", required = false,
			usage = "Export the dataset using blending strategy instead of hardcut (max.min.distance)")
	private boolean blending = false;

	@Option(name = "--fillBackground", aliases = { "--fill" }, required = false,
			usage = "Fill the outer space in N5 export with the background value of the data instead of zero")
	private boolean fillBackground = false;

	/**
	 * Toggle pipeline stages. By default all stages are executed.
	 */
	@Option(name = "--stitch", required = false, usage = "Only stitch the tiles, i.e. find optimal positions")
	private boolean stitchOnly = false;

	@Option(name = "--fuse", required = false, usage = "Only fuse the tiles, i.e. export the image in the CellFileViewer format")
	private boolean fuseOnly = false;

	@Option(name = "--fusestage", required = false,
			usage = "Allow fusing tiles using their stage coordinates (when there is no '-final' suffix in the tile configuration filename), i.e. when exporting initial tile configuration.")
	private boolean allowFusingStage = false;
	
	@Option(name = "--applyRawStitchingToDecon", required = false,
		usage = "Apply stitching parameters obtained from raw tiles to deconvolved tiles.")
	private boolean applyRawStitchingToDecon = false;


	private boolean parsedSuccessfully = false;

	public StitchingArguments( final String[] args ) throws IllegalArgumentException, IOException
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
		
		if ( applyRawStitchingToDecon )
		{
		    	for ( int channel = 0; channel < inputTileConfigurations.size(); channel++ )
		    	{
		    		String inputTileConfiguration = inputTileConfigurations.get( channel );
			    	JsonArray jsonArray = (JsonArray) new JsonParser().parse( new FileReader( inputTileConfiguration ) );
			    	JsonObject firstTile = (JsonObject) jsonArray.get( 0 );
			    	String parentDirectory = new File( firstTile.get( "file" ).toString() ).getParent();
			    	
		    		String content = new String( Files.readAllBytes( Paths.get( inputTileConfiguration ) ), StandardCharsets.UTF_8);
		    		content = content.replaceAll( parentDirectory, parentDirectory + "/matlab_decon" );
		    		content = content.replaceAll( ".tif", "_decon.tif" );
		    		String outputTileConfiguration;
		    		if ( inputTileConfiguration.contains( "-final.json" ) )
		    			outputTileConfiguration = inputTileConfiguration.replace( "-final.json", "-rawToDecon-final.json" );
		    		else 
		    			outputTileConfiguration = inputTileConfiguration.replace( ".json", "-rawToDecon.json" );
		    		
		    		Files.write( Paths.get( outputTileConfiguration ), content.getBytes( StandardCharsets.UTF_8 ) );
		    		inputTileConfigurations.set(channel, outputTileConfiguration);
		    	}
		}
		
		if (correctionImagesPaths != null && correctionImagesPaths.size() > 0)
		{
			if (correctionImagesPaths.size() != inputTileConfigurations.size())
				throw new IllegalArgumentException("Correction images must match input tile configurations");
		} else
			correctionImagesPaths = new ArrayList<>(inputTileConfigurations);

		if ( !fuseOnly )
		{
			if ( registrationChannelIndex != null && registrationChannelIndex == -1 )
				registrationChannelIndex = null;
			else if ( registrationChannelIndex != null && ( registrationChannelIndex < 0 || registrationChannelIndex >= inputTileConfigurations.size() ) )
				throw new IllegalArgumentException( "Registration channel index " + registrationChannelIndex + " is out of bounds [0-" + ( inputTileConfigurations.size() - 1 ) + "]" );
		}
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
	public List< String > correctionImagesPaths() { return correctionImagesPaths; }
	public Integer registrationChannelIndex() { return registrationChannelIndex; }
	public int minStatsNeighborhood() { return minStatsNeighborhood; }
	public int fusionCellSize() { return fusionCellSize; }
	public double blurSigma() { return blurSigma; }
	public boolean useAllPairs() { return allPairs; }
	public boolean noLeaves() { return noLeaves; }
	public boolean exportOverlaps() { return exportOverlaps; }
	public boolean blending() { return blending; }
	public boolean fillBackground() { return fillBackground; }
	public boolean allowFusingStage() { return allowFusingStage; }

	public boolean stitchOnly() { return stitchOnly; }
	public boolean fuseOnly() { return fuseOnly; }
	
	public boolean applyRawStitchingToDecon() { return applyRawStitchingToDecon; }

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
