package org.janelia.stitching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;
import org.janelia.flatfield.FlatfieldCorrection;
import org.janelia.stitching.StitchSubdividedTileBoxPair.StitchingResult;

import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.RandomAccessiblePairNullable;

/**
 * Computes updated tile positions using phase correlation for pairwise matches and then global optimization for fitting all of them together.
 * Saves updated tile configuration on the disk.
 *
 * @author Igor Pisarev
 */

public class PipelineStitchingStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -7152174064553332061L;

	private final SerializableStitchingParameters stitchingParameters;

	public PipelineStitchingStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
		stitchingParameters = job.getParams();
	}

	@Override
	public void run() throws PipelineExecutionException
	{
		try
		{
			final DataProvider dataProvider = job.getDataProvider();
			for ( int iteration = 0; ; ++iteration )
			{
				final int[] tileBoxesGridSize = new int[ job.getDimensionality() ];
				Arrays.fill( tileBoxesGridSize, 2 );
				final List< SubdividedTileBox > tileBoxes = SplitTileOperations.splitTilesIntoBoxes( job.getTiles( 0 ), tileBoxesGridSize );
				final List< SubdividedTileBoxPair > overlappingBoxes = SplitTileOperations.findOverlappingTileBoxes( tileBoxes, !job.getArgs().useAllPairs() );

				final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
				final String filename = PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( 0 ) );
				final String iterationDirname = "iter" + iteration;
				final String stitchedTilesFilepath = PathResolver.get( basePath, iterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) );

				if ( !dataProvider.fileExists( URI.create( stitchedTilesFilepath ) ) )
				{
					System.out.println( "************** Iteration " + iteration + " **************" );
					preparePairwiseShifts( overlappingBoxes, iteration );

					final StitchingOptimizer optimizer = new StitchingOptimizer( job, sparkContext );
					optimizer.optimizeAffine( iteration );
				}
				else
				{
					System.out.println( "Stitched tiles file already exists for iteration " + iteration + ", continue..." );
				}

				// check if number of stitched tiles has increased compared to the previous iteration
				final TileInfo[] stageTiles = job.getTiles( 0 );
				final TileInfo[] stitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( stitchedTilesFilepath ) ) );

				// TODO: test and keep if works or remove (currently generates worse solutions)
				// find new pairs using new solution for predicting positions of the excluded (missing) tiles
//				final List< TilePair > newPairs = FindPairwiseChanges.getPairsWithPrediction( stageTiles, stitchedTiles, job.getArgs().minStatsNeighborhood(), !job.getArgs().useAllPairs() );
//				overlappingTiles.clear();
//				overlappingTiles.addAll( newPairs );

				// stop if all input tiles are included in the stitched set
				if ( stageTiles.length == stitchedTiles.length )
				{
					System.out.println( "Stopping on iteration " + iteration + ": all input tiles (n=" + stageTiles.length + ") are included in the stitched set" );
					copyFinalSolution( iteration );
					break;
				}
				else if ( iteration > 0 )
				{
					final String previousIterationDirname = "iter" + ( iteration - 1 );
					final String previousStitchedTilesFilepath = PathResolver.get( basePath, previousIterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) );
					final TileInfo[] previousStitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( previousStitchedTilesFilepath ) ) );

					//final String usedPairsFilepath = PathResolver.get( basePath, iterationDirname, "pairwise-used.json" );
					//final String previousUsedPairsFilepath = PathResolver.get( basePath, previousIterationDirname, "pairwise-used.json" );
					//final List< SerializablePairWiseStitchingResult > usedPairs = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( usedPairsFilepath ) ) );
					//final List< SerializablePairWiseStitchingResult > previousUsedPairs = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( previousUsedPairsFilepath ) ) );

					if ( stitchedTiles.length <= previousStitchedTiles.length )
					{
//					if ( stitchedTiles.length < previousStitchedTiles.length || ( stitchedTiles.length == previousStitchedTiles.length && usedPairs.size() <= previousUsedPairs.size() ) )
//					{
						/*if ( !higherOrderStitching )
						{
							higherOrderStitching = true;
						}
						else*/
						{
							// mark the last solution as not used because it is worse than from the previous iteration
							dataProvider.moveFolder(
									URI.create( PathResolver.get( basePath, iterationDirname ) ),
									URI.create( PathResolver.get( basePath, Utils.addFilenameSuffix( iterationDirname, "-notused" ) ) )
								);
							copyFinalSolution( iteration - 1 );
							System.out.println( "Stopping on iteration " + iteration + ": the new solution (n=" + stitchedTiles.length + ") is not greater than the previous solution (n=" + previousStitchedTiles.length + "). Input tiles n=" + stageTiles.length );
							break;
						}
					}
				}
			}
		}
		catch ( final IOException e )
		{
			System.out.println( "Something went wrong during stitching:" );
			e.printStackTrace();
			throw new PipelineExecutionException( e );
		}
	}

	/**
	 * Stores the final solution in the main folder when further iterations don't yield better results
	 *
	 * @param fromIteration
	 * @throws IOException
	 */
	private void copyFinalSolution( final int fromIteration ) throws IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( channel ) );
			final String filename = PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( channel ) );
			final String iterationDirname = "iter" + fromIteration;
			final String stitchedTilesFilepath = PathResolver.get( basePath, iterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) );
			final String finalTilesFilepath = PathResolver.get( basePath, Utils.addFilenameSuffix( filename, "-final" ) );
			dataProvider.copyFile( URI.create( stitchedTilesFilepath ), URI.create( finalTilesFilepath ) );

			if ( channel == 0 )
				dataProvider.copyFile(
						URI.create( PathResolver.get( basePath, iterationDirname, "optimizer.txt" ) ),
						URI.create( PathResolver.get( basePath, "optimizer-final.txt" ) )
					);
		}
	}

	/**
	 * Initiates the computation for pairs that have not been precomputed. Stores the pairwise file for this iteration of stitching.
	 *
	 * @param overlappingBoxes
	 * @param iteration
	 * @throws PipelineExecutionException
	 * @throws IOException
	 */
	private void preparePairwiseShifts( final List< SubdividedTileBoxPair > overlappingBoxes, final int iteration ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String iterationDirname = "iter" + iteration;
		final String previousIterationDirname = iteration == 0 ? null : "iter" + ( iteration - 1 );
		final String pairwiseFilename = "pairwise.json";
		dataProvider.createFolder( URI.create( PathResolver.get( basePath, iterationDirname ) ) );
		final String pairwisePath = PathResolver.get( basePath, iterationDirname, pairwiseFilename );

		// FIXME: replaces checking contents of the pairwise file by simply checking its existence
		if ( dataProvider.fileExists( URI.create( pairwisePath ) ) )
		{
			System.out.println( "pairwise.json file exists, don't recompute shifts" );
			return;
		}

		final List< SerializablePairWiseStitchingResult > pairwiseShifts = tryLoadPrecomputedShifts( basePath, iteration );
		final List< SubdividedTileBoxPair > pendingOverlappingBoxes = removePrecomputedPendingPairs( pairwisePath, overlappingBoxes, pairwiseShifts );

		if ( pendingOverlappingBoxes.isEmpty() && !pairwiseShifts.isEmpty() )
		{
			// If we're able to load precalculated pairwise results, save some time skipping this step and jump to the global optimization
			System.out.println( "Successfully loaded all pairwise results from disk!" );
		}
		else
		{
			final String statsTileConfigurationPath = iteration == 0 ? null : PathResolver.get(
					basePath,
					previousIterationDirname,
					Utils.addFilenameSuffix(
							PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( 0 ) ),
							"-stitched"
						)
				);

			// Initiate the computation
			final List< StitchingResult > stitchingResults = computePairwiseShifts( pendingOverlappingBoxes, statsTileConfigurationPath );

			// merge results with preloaded pairwise shifts
			for ( final StitchingResult result : stitchingResults )
				pairwiseShifts.add( result.shift );

//			saveSearchRadiusStats( stitchingResults, PathResolver.get( basePath, iterationDirname, "searchRadiusStats.txt" ) );

			try
			{
				System.out.println( "Stitched all tiles pairwise, store this information on disk.." );
				TileInfoJSONProvider.savePairwiseShifts( pairwiseShifts, dataProvider.getJsonWriter( URI.create( pairwisePath ) ) );
			}
			catch ( final IOException e )
			{
				e.printStackTrace();
			}
		}
	}

	/**
	 * Returns precomputed pairwise shifts if the corresponding file exists.
	 *
	 * @param basePath
	 * @param iteration
	 * @return
	 * @throws PipelineExecutionException
	 * @throws IOException
	 */
	private List< SerializablePairWiseStitchingResult > tryLoadPrecomputedShifts(
			final String basePath,
			final int iteration ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final String iterationDirname = "iter" + iteration;
		final String previousIterationDirname = iteration == 0 ? null : "iter" + ( iteration - 1 );
		final String pairwiseFilename = "pairwise.json";
		final String pairwisePath = PathResolver.get( basePath, iterationDirname, pairwiseFilename );

		if ( iteration == 0 )
		{
			// use the pairwise file from the previous run in the old mode if exists
			final String oldPairwisePath = PathResolver.get( basePath, pairwiseFilename );
			if ( dataProvider.fileExists( URI.create( oldPairwisePath ) ) )
				dataProvider.moveFile( URI.create( oldPairwisePath ), URI.create( pairwisePath ) );
		}
		else
		{
			if ( job.getArgs().stitchingMode() == StitchingMode.INCREMENTAL )
			{
				throw new UnsupportedOperationException( "TODO: handle pairwise-used.json correctly in StitchingOptimizer. Incremental stitching is disabled for now." );

				/*System.out.println( "Restitching only excluded pairs" );
				// use pairwise-used from the previous iteration, so they will not be restitched
				if ( !dataProvider.fileExists( URI.create( pairwisePath ) ) )
					dataProvider.copyFile(
							URI.create( PathResolver.get( basePath, previousIterationDirname, Utils.addFilenameSuffix( pairwiseFilename, "-used" ) ) ),
							URI.create( pairwisePath )
						);*/
			}
			else
			{
				System.out.println( "Full restitching" );
			}
		}

		// Try to load precalculated shifts for some pairs of tiles
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = new ArrayList<>();
		if ( dataProvider.fileExists( URI.create( pairwisePath ) ) )
		{
			try
			{
				System.out.println( "try to load pairwise results from disk" );
				pairwiseShifts.addAll( TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwisePath ) ) ) );
			}
			catch ( final FileNotFoundException e )
			{
				System.out.println( "Pairwise results file not found" );
			}
			catch ( final NullPointerException e )
			{
				System.out.println( "Pairwise results file is malformed" );
				e.printStackTrace();
				throw e;
			}
			catch ( final IOException e )
			{
				e.printStackTrace();
			}
		}
		return pairwiseShifts;
	}

	/**
	 * Removes pending pairs where their shift vectors have already been precomputed.
	 *
	 * @param pairwisePath
	 * @param overlappingTiles
	 * @param pairwiseShifts
	 * @return
	 * @throws PipelineExecutionException
	 * @throws IOException
	 */
	private List< SubdividedTileBoxPair > removePrecomputedPendingPairs(
			final String pairwisePath,
			final List< SubdividedTileBoxPair > overlappingBoxes,
			final List< SerializablePairWiseStitchingResult > pairwiseShifts ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		// remove redundant pairs (that are not contained in the given overlappingTiles list)
		final Map< Integer, Set< Integer > > overlappingPairsCache = new TreeMap<>();
		for ( final SubdividedTileBoxPair boxPair : overlappingBoxes )
		{
			final int ind1 = Math.min( boxPair.getA().getIndex(), boxPair.getB().getIndex() );
			final int ind2 = Math.max( boxPair.getA().getIndex(), boxPair.getB().getIndex() );
			if ( !overlappingPairsCache.containsKey( ind1 ) )
				overlappingPairsCache.put( ind1, new TreeSet<>() );
			overlappingPairsCache.get( ind1 ).add( ind2 );
		}
		int pairsRemoved = 0;
		for ( final Iterator< SerializablePairWiseStitchingResult > it = pairwiseShifts.iterator(); it.hasNext(); )
		{
			final SerializablePairWiseStitchingResult result = it.next();
			final Integer[] indexes = new Integer[ 2 ];
			for ( int i = 0; i < 2; ++i )
				if ( indexes[ i ] == null )
					indexes[ i ] = result.getTileBoxPair().toArray()[ i ].getIndex();
				else if ( !indexes[ i ].equals( result.getTileBoxPair().toArray()[ i ].getIndex() ) )
					throw new PipelineExecutionException( "Tile indexes do not match" );

			final int ind1 = Math.min( indexes[ 0 ], indexes[ 1 ] );
			final int ind2 = Math.max( indexes[ 0 ], indexes[ 1 ] );
			if ( !overlappingPairsCache.containsKey( ind1 ) || !overlappingPairsCache.get( ind1 ).contains( ind2 ) )
			{
				it.remove();
				++pairsRemoved;
			}
		}
		System.out.println( "Removed " + pairsRemoved + " redundant pairs from the cached pairwise file" );

		// resave the new file if something has changed
		if ( pairsRemoved != 0 )
			TileInfoJSONProvider.savePairwiseShifts( pairwiseShifts, dataProvider.getJsonWriter( URI.create( pairwisePath ) ) );

		// find only pairs that need to be computed
		final List< SubdividedTileBoxPair > pendingOverlappingBoxes = new ArrayList<>();

		// Create a cache to efficiently lookup the existing pairs of tiles loaded from disk
		final Map< Integer, Set< Integer > > cache = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult result : pairwiseShifts )
		{
			final Integer[] indexes = new Integer[ 2 ];
			for ( int i = 0; i < 2; ++i )
				if ( indexes[ i ] == null )
					indexes[ i ] = result.getTileBoxPair().toArray()[ i ].getIndex();
				else if ( !indexes[ i ].equals( result.getTileBoxPair().toArray()[ i ].getIndex() ) )
					throw new PipelineExecutionException( "Tile indexes do not match" );

			final int firstIndex =  Math.min( indexes[ 0 ], indexes[ 1 ] ), secondIndex  =  Math.max( indexes[ 0 ], indexes[ 1 ] );
			if ( !cache.containsKey( firstIndex ) )
				cache.put( firstIndex, new TreeSet<>() );
			cache.get( firstIndex ).add( secondIndex );
		}

		// Populate a new list of pending tile pairs (add only those pairs that are not contained in the cache)
		for ( final SubdividedTileBoxPair boxPair : overlappingBoxes )
		{
			final int firstIndex =  Math.min( boxPair.getA().getIndex(), boxPair.getB().getIndex() ),
					secondIndex =  Math.max( boxPair.getA().getIndex(), boxPair.getB().getIndex() );
			if ( !cache.containsKey( firstIndex ) || !cache.get( firstIndex ).contains( secondIndex ) )
				pendingOverlappingBoxes.add( boxPair );
		}

		return pendingOverlappingBoxes;
	}

	/**
	 * Saves statistics on search radius for each tile into a txt file.
	 *
	 * @param stitchingResults
	 * @param searchRadiusStatsPath
	 * @throws PipelineExecutionException
	 */
	private void saveSearchRadiusStats( final List< StitchingResult > stitchingResults, final String searchRadiusStatsPath ) throws PipelineExecutionException
	{
		final DataProvider dataProvider = job.getDataProvider();
		try ( final OutputStream out = dataProvider.getOutputStream( URI.create( searchRadiusStatsPath ) ) )
		{
			try ( final PrintWriter writer = new PrintWriter( out ) )
			{
				writer.println( "Tile1_index Tile1_grid_X Tile1_grid_Y Tile1_grid_Z Tile1_timestamp Tile2_index Tile2_grid_X Tile2_grid_Y Tile2_grid_Z Tile2_timestamp Radius_1st Radius_2nd Radius_3rd" );
				for ( final StitchingResult result : stitchingResults )
				{
					final TilePair originalTilePair = result.shift.getTileBoxPair().getOriginalTilePair();
					writer.println(
							String.format(
									""
									+ "%d %d %d %d %d"
									+ " "
									+ "%d %d %d %d %d"
									+ " "
									+ "%.2f %.2f %.2f",

									result.shift.getTileBoxPair().getA().getIndex(),
									Utils.getTileCoordinates( originalTilePair.getA() )[ 0 ],
									Utils.getTileCoordinates( originalTilePair.getA() )[ 1 ],
									Utils.getTileCoordinates( originalTilePair.getA() )[ 2 ],
									Utils.getTileTimestamp( originalTilePair.getA() ),

									result.shift.getTileBoxPair().getB().getIndex(),
									Utils.getTileCoordinates( originalTilePair.getB() )[ 0 ],
									Utils.getTileCoordinates( originalTilePair.getB() )[ 1 ],
									Utils.getTileCoordinates( originalTilePair.getB() )[ 2 ],
									Utils.getTileTimestamp( originalTilePair.getB() ),

									result.searchRadiusLength != null ? result.searchRadiusLength[ 0 ] : -1,
									result.searchRadiusLength != null ? result.searchRadiusLength[ 1 ] : -1,
									result.searchRadiusLength != null ? result.searchRadiusLength[ 2 ] : -1
								)
						);
				}
			}
		}
		catch ( final Exception e )
		{
			throw new PipelineExecutionException( "Can't write search radius stats: " + e.getMessage(), e );
		}
	}

	/**
	 * Computes the best possible pairwise shifts between every pair of tiles on a Spark cluster.
	 * It uses phase correlation for measuring similarity between two images.
	 */
	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > List< StitchingResult > computePairwiseShifts(
			final List< SubdividedTileBoxPair > overlappingBoxes,
			final String statsTileConfigurationPath ) throws PipelineExecutionException
	{
		final Broadcast< TileSearchRadiusEstimator > broadcastedSearchRadiusEstimator = sparkContext.broadcast( loadSearchRadiusEstimator( statsTileConfigurationPath ) );
		final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedFlatfieldCorrectionForChannels = sparkContext.broadcast( loadFlatfieldChannels() );
		final Broadcast< List< Map< String, TileInfo > > > broadcastedCoordsToTilesChannels = sparkContext.broadcast( getCoordsToTilesChannels() );

		System.out.println( "Processing " + overlappingBoxes.size() + " pairs..." );

		final LongAccumulator notEnoughNeighborsWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noOverlapWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noPeaksWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();

		final JavaRDD< StitchingResult > pairwiseStitching = sparkContext.parallelize( overlappingBoxes, overlappingBoxes.size() ).map( tileBoxPair ->
				new StitchSubdividedTileBoxPair<>(
						job,
						broadcastedSearchRadiusEstimator.value(),
						broadcastedCoordsToTilesChannels.value(),
						broadcastedFlatfieldCorrectionForChannels.value()
					)
				.stitchTileBoxPair( tileBoxPair )
			);

		final List< StitchingResult > stitchingResults = pairwiseStitching.collect();

		broadcastedFlatfieldCorrectionForChannels.destroy();
		broadcastedSearchRadiusEstimator.destroy();
		broadcastedCoordsToTilesChannels.destroy();

		int validPairs = 0;
		for ( final StitchingResult result : stitchingResults )
		{
			final SerializablePairWiseStitchingResult shift = result.shift;
			if ( shift != null && shift.getIsValidOverlap() )
				++validPairs;
		}
		System.out.println();
		System.out.println( "======== Pairwise stitching completed ========" );
		System.out.println( "Total pairs: " + stitchingResults.size() );
		System.out.println( "Valid pairs: " + validPairs );
		System.out.println( "Invalid pairs:" );
		System.out.println( "    not enough neighbors within estimation window: " + notEnoughNeighborsWithinConfidenceIntervalPairsCount.value() );
		System.out.println( "    <= 1px overlap within search radius: " + noOverlapWithinConfidenceIntervalPairsCount.value() );
		System.out.println( "    no peaks found within search radius: " + noPeaksWithinConfidenceIntervalPairsCount.value() );
		System.out.println();

		return stitchingResults;
	}

	/**
	 * Tries to load flatfield components for all channels.
	 *
	 * @return
	 * @throws IOException
	 */
	private < U extends NativeType< U > & RealType< U > > List< RandomAccessiblePairNullable< U, U > > loadFlatfieldChannels() throws PipelineExecutionException
	{
		final DataProvider dataProvider = job.getDataProvider();
		System.out.println( "Broadcasting flatfield correction images" );
		final List< RandomAccessiblePairNullable< U, U > > flatfieldCorrectionForChannels = new ArrayList<>();
		for ( final String channelPath : job.getArgs().inputTileConfigurations() )
		{
			final String channelPathNoExt = channelPath.lastIndexOf( '.' ) != -1 ? channelPath.substring( 0, channelPath.lastIndexOf( '.' ) ) : channelPath;
			// use it as a folder with the input file's name
			try
			{
				flatfieldCorrectionForChannels.add( FlatfieldCorrection.loadCorrectionImages( dataProvider, channelPathNoExt,  job.getDimensionality() ) );
			}
			catch ( final IOException e)
			{
				e.printStackTrace();
				throw new PipelineExecutionException( "Cannot load flatfields", e );
			}
		}
		return flatfieldCorrectionForChannels;
	}

	/**
	 * Creates a mapping from stage grid coordinates to tiles for each channel (to be able to refer to the same tile for channel averaging).
	 *
	 * @return
	 * @throws PipelineExecutionException
	 */
	private List< Map< String, TileInfo > > getCoordsToTilesChannels() throws PipelineExecutionException
	{
		final List< Map< String, TileInfo > > coordsToTilesChannels = new ArrayList<>();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final Map< String, TileInfo > coordsToTiles = new HashMap<>();
			for ( final TileInfo tile : job.getTiles( channel ) )
			{
				final String coords;
				try
				{
					coords = Utils.getTileCoordinatesString( tile );
				}
				catch ( final Exception e )
				{
					System.out.println( "Cannot get tile coordinates string: " + tile.getFilePath() );
					e.printStackTrace();
					throw new PipelineExecutionException( e );
				}

				if ( coordsToTiles.containsKey( coords ) )
					throw new PipelineExecutionException( "Tile with coords " + coords + " is not unique" );

				coordsToTiles.put( coords, tile );
			}
			coordsToTilesChannels.add( coordsToTiles );
		}
		return coordsToTilesChannels;
	}

	/**
	 * Tries to create a predictive model based on the previous stitching solution if exists.
	 *
	 * @param statsTileConfigurationPath
	 * @return
	 * @throws PipelineExecutionException
	 */
	private TileSearchRadiusEstimator loadSearchRadiusEstimator( final String statsTileConfigurationPath ) throws PipelineExecutionException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final TileSearchRadiusEstimator searchRadiusEstimator;
		if ( statsTileConfigurationPath != null )
		{
			System.out.println( "=== Building prediction model based on previous stitching solution ===" );
			try
			{
				final TileInfo[] statsTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( statsTileConfigurationPath ) ) );
				System.out.println( "-- Creating search radius estimator using " + job.getTiles( 0 ).length + " stage tiles and " + statsTiles.length + " stitched tiles --" );
				searchRadiusEstimator = new TileSearchRadiusEstimator(
						statsTiles,
						job.getArgs().searchRadiusMultiplier(),
						job.getArgs().searchWindowSizeTiles()
					);
				System.out.println( "-- Created search radius estimator. Estimation window size (neighborhood): " + Arrays.toString( searchRadiusEstimator.estimationWindowSize ) + " --" );
			}
			catch ( final IOException e )
			{
				e.printStackTrace();
				throw new PipelineExecutionException( "Cannot load previous solution for stats:" + statsTileConfigurationPath, e );
			}
		}
		else
		{
			searchRadiusEstimator = null;
		}
		return searchRadiusEstimator;
	}
}
