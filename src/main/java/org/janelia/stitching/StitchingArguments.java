package org.janelia.stitching;

import java.io.Serializable;
import java.util.List;

import org.janelia.fusion.FusionMode;
import org.janelia.stitching.StitchingOptimizer.OptimizerMode;
import org.janelia.stitching.StitchingOptimizer.RegularizerType;
import org.janelia.util.Conversions;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Command line arguments parser for a stitching job.
 *
 * @author Igor Pisarev
 */

public class StitchingArguments implements Serializable {

	private static final long serialVersionUID = -8996450783846140673L;

	@Option(name = "-i", aliases = { "--input" }, required = true,
			usage = "Path/link to a tile configuration JSON file. Multiple configurations can be passed at once.")
	private List< String > inputTileConfigurations;

	@Option(name = "-n", aliases = { "--neighbors" }, required = false,
			usage = "Min neighborhood for estimating confidence intervals using offset statistics")
	private int minNumNeighboringTiles = 5;

	@Option(name = "-r", aliases = { "--radius" }, required = false,
			usage = "Search radius multiplier (error ellipse size)")
	private double searchRadiusMultiplier = 3;

	@Option(name = "--constrain", required = false,
			usage = "Constrain pairwise matching on the first iteration")
	private boolean constrainMatchingOnFirstIteration = false;

	@Option(name = "-w", aliases = { "--weighted" }, required = false,
			usage = "Weighted predictions for tile positions in the rematching phase")
	private boolean weightedPredictions = false;

	@Option(name = "-sr", aliases = { "--sphereradius" }, required = false,
			usage = "Radius of search sphere as a percent of tile size (when prediction model is not available, usually on the first stitching iteration)")
	private double errorEllipseRadiusAsTileSizeRatio = 0.1;

	@Option(name = "-sw", aliases = { "--searchwindow" }, required = false,
			usage = "Search window size for local offset statistics in terms of number of tiles")
	private String statsWindowSizeTiles = "3,3,3";

	@Option(name = "-c", aliases = { "--fusioncell" }, required = false,
			usage = "Size of an individual tile when fusing")
	private int fusionCellSize = 128;

	@Option(name = "-b", aliases = { "--blursigma" }, required = false,
			usage = "Sigma value of the gaussian blur preapplied to the images before stitching")
	private double blurSigma = 2.0;

	@Option(name = "-p", aliases = { "--padding" }, required = false,
			usage = "Padding for the overlap regions")
	private String padding = "0,0,0";

	@Option(name = "--peaks", required = false,
			usage = "Number of phase correlation peaks to inspect")
	private int numCheckPeaks = 50;

	@Option(name = "-s", aliases = { "--subdivision" }, required = false,
			usage = "Size of the subtile grid in single dimension")
	private int subdivisionGridSize = 2;

	@Option(name = "--min", required = false,
			usage = "Min coordinate for exporting")
	private String minCoord = null;

	@Option(name = "--max", required = false,
			usage = "Max coordinate for exporting")
	private String maxCoord = null;

	@Option(name = "--allpairs", required = false,
			usage = "Compute pairwise shifts between all pairs (by default only adjacent pairs are used)")
	private boolean allPairs = false;

	@Option(name = "--onlyexcluded", required = false,
			usage = "If set along with the incremental rematching mode, only those overlapping pairs will be rematched where at least one of the tiles was excluded in the previous stitched configuration.")
	private boolean rematchOnlyExcludedTiles = false;

	@Option(name = "--noleaves", required = false,
			usage = "Optimize tile configurations that don't contain any leaves (thus all edges are properly constrained)")
	private boolean noLeaves = false;

	@Option(name = "-m", aliases = { "--stitching-mode" }, required = false,
			usage = "Stitching mode for rematching tile pairs (can be 'incremental' or 'full')")
	private String stitchingModeStr = "";
	private StitchingMode stitchingMode = null;

	@Option(name = "-f", aliases = { "--fusion-mode" }, required = false,
			usage = "Fusion mode (can be 'max-min-distance', 'max-intensity', 'blending', or 'debug-overlaps')")
	private String fusionModeStr = "max-min-distance";
	private FusionMode fusionMode = null;

	@Option(name = "--fusestage", required = false,
			usage = "Allow fusing tiles using their stage coordinates if the world transform is missing, i.e. when exporting initial tile configuration.")
	private boolean allowFusingStage = false;

	@Option(name = "-opt", aliases = { "--optimizer" }, required = false,
			usage = "Optimizer model")
	private String optimizerModeStr = "affine";
	private OptimizerMode optimizerMode = null;

	@Option(name = "-reg", aliases = { "--regularizer" }, required = false,
			usage = "Regularizer model")
	private String regularizerTypeStr = "rigid";
	private RegularizerType regularizerType = null;

	@Option(name = "-l", aliases = { "--lambda" }, required = false,
			usage = "Regularizer lambda")
	private double regularizerLambda = 0.1;

	@Option(name = "--overlaps", required = false,
			usage = "Export overlaps channel based on which connections between tiles have been used for final stitching")
	private boolean exportOverlaps = false;

	@Option(name = "--maxErrorLimit", required = false,
			usage = "Max allowed error limit for global optimization")
	private double maxAllowedErrorLimit = 30;

	@Option(name = "--maxErrorStart", required = false,
			usage = "Initial max allowed error for global optimization")
	private double initialMaxAllowedError = 5;

	@Option(name = "--maxErrorStep", required = false,
			usage = "Max allowed error step for global optimization (the max allowed error will be increased by the given step on every rematching iteration until it reaches the given limit)")
	private double maxAllowedErrorStep = 5;

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
			stitchingMode = StitchingMode.valueOf( stitchingModeStr.replace( '-', '_' ).toUpperCase() );
			optimizerMode = OptimizerMode.valueOf( optimizerModeStr.replace( '-', '_' ).toUpperCase() );
			regularizerType = RegularizerType.valueOf( regularizerTypeStr.replace( '-', '_' ).toUpperCase() );

			if ( optimizerMode == OptimizerMode.TRANSLATION && regularizerType != RegularizerType.NONE )
				throw new IllegalArgumentException( "optimizer set to TRANSLATION, expected regularizer to be NONE, got " + regularizerType );
		}

		if ( !stitchOnly )
			fusionMode = FusionMode.valueOf( fusionModeStr.replace( '-', '_' ).toUpperCase() );

		if ( stitchingMode != StitchingMode.INCREMENTAL && rematchOnlyExcludedTiles )
			throw new IllegalArgumentException( "--onlyexcluded is allowed only with the incremental rematching mode" );
	}

	protected StitchingArguments() { }

	public static StitchingArguments defaultFusionArgs()
	{
		final StitchingArguments args = new StitchingArguments();
		args.fuseOnly = true;
		return args;
	}

	public boolean parsedSuccessfully() { return parsedSuccessfully; }

	public List< String > inputTileConfigurations() { return inputTileConfigurations; }

	public boolean stitchOnly() { return stitchOnly; }
	public boolean fuseOnly() { return fuseOnly; }

	public StitchingMode stitchingMode() { return stitchingMode; }
	public FusionMode fusionMode() { return fusionMode; }

	// Stitching options
	public int minNumNeighboringTiles() { return minNumNeighboringTiles; }
	public double searchRadiusMultiplier() { return searchRadiusMultiplier; }
	public boolean constrainMatchingOnFirstIteration() { return constrainMatchingOnFirstIteration; }
	public double errorEllipseRadiusAsTileSizeRatio() { return errorEllipseRadiusAsTileSizeRatio; }
	public int[] searchWindowSizeTiles() { return Conversions.toIntArray( parseArray( statsWindowSizeTiles ) ); }
	public boolean weightedPredictions() { return weightedPredictions; }
	public long[] padding() { return parseArray( padding ); }
	public int numCheckPeaks() { return numCheckPeaks; }
	public int subdivisionGridSize() { return subdivisionGridSize; }
	public double blurSigma() { return blurSigma; }
	public boolean useAllPairs() { return allPairs; }
	public boolean rematchOnlyExcludedTiles() { return rematchOnlyExcludedTiles; }
	public boolean noLeaves() { return noLeaves; }

	public OptimizerMode optimizerMode() { return optimizerMode; }
	public RegularizerType regularizerType() { return regularizerType; }
	public double regularizerLambda() { return regularizerLambda; }

	public double maxAllowedErrorLimit() { return maxAllowedErrorLimit; }
	public double initialMaxAllowedError() { return initialMaxAllowedError; }
	public double maxAllowedErrorStep() { return maxAllowedErrorStep; }

	// Fusion options
	public int fusionCellSize() { return fusionCellSize; }
	public boolean allowFusingStage() { return allowFusingStage; }
	public boolean exportOverlaps() { return exportOverlaps; }
	public long[] minCoord() { return parseArray( minCoord ); }
	public long[] maxCoord() { return parseArray( maxCoord ); }

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
