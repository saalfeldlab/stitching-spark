package org.janelia.stitching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;
import org.janelia.stitching.TileSearchRadiusEstimator.NotEnoughNeighboringTilesException;

import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.RandomAccessiblePairNullable;

/**
 * Computes updated tile positions using phase correlation for pairwise matches and then global optimization for fitting all of them together.
 * Saves updated tile configuration on the disk.
 *
 * @author Igor Pisarev
 */

public class StitchingIterationPerformer< U extends NativeType< U > & RealType< U > > implements Serializable
{
	private static final long serialVersionUID = -7152174064553332061L;

	private final StitchingJob job;
	private final transient JavaSparkContext sparkContext;
	private final int iteration;
	private final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedFlatfieldsForChannels;
	private final Broadcast< List< Map< Integer, TileInfo > > > broadcastedTileMapsForChannels;

	private Broadcast< TileSearchRadiusEstimator > broadcastedSearchRadiusEstimator;

	public StitchingIterationPerformer(
			final StitchingJob job,
			final JavaSparkContext sparkContext,
			final int iteration,
			final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedFlatfieldsForChannels,
			final Broadcast< List< Map< Integer, TileInfo > > > broadcastedTileMapsForChannels )
	{
		this.job = job;
		this.sparkContext = sparkContext;
		this.iteration = iteration;
		this.broadcastedFlatfieldsForChannels = broadcastedFlatfieldsForChannels;
		this.broadcastedTileMapsForChannels = broadcastedTileMapsForChannels;
	}

	public void run() throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String iterationDirname = "iter" + iteration;

		try ( final OutputStream logOut = dataProvider.getOutputStream( URI.create( PathResolver.get( basePath, iterationDirname, "pairwise-matcher.txt" ) ) ) )
		{
			try ( final PrintWriter logWriter = new PrintWriter( logOut ) )
			{
				printStats( logWriter );

				broadcastedSearchRadiusEstimator = sparkContext.broadcast( createSearchRadiusEstimator( logWriter ) );
				final TileInfo[] tilesWithEstimatedTransforms = getTilesWithEstimatedTransformation( logWriter );

				final int[] subTilesGridSize = new int[ job.getDimensionality() ];
				Arrays.fill( subTilesGridSize, job.getArgs().subdivisionGridSize() );
				final List< SubTile > subTiles = SubTileOperations.subdivideTiles( tilesWithEstimatedTransforms, subTilesGridSize );
				final List< SubTilePair > overlappingSubTiles = SubTileOperations.findOverlappingSubTiles(
						subTiles,
						!job.getArgs().useAllPairs(), // adjacent vs. all overlapping pairs
						iteration > 0 // use stage coords on the first run, and only world transform (last stitched / estimated) on subsequent iterations
					);
				preparePairwiseShifts( overlappingSubTiles, iteration, logWriter );
			}
		}

		try ( final OutputStream logOut = dataProvider.getOutputStream( URI.create( PathResolver.get( basePath, iterationDirname, "optimizer.txt" ) ) ) )
		{
			try ( final PrintWriter logWriter = new PrintWriter( logOut ) )
			{
				final StitchingOptimizer optimizer = new StitchingOptimizer( job, sparkContext );
				optimizer.optimize( iteration, logWriter );
			}
		}
	}

	private void printStats( final PrintWriter logWriter )
	{
		if ( iteration == 0 )
			logWriter.println( job.getArgs().constrainMatchingOnFirstIteration() ? "Constrained pairwise matching" : "Unconstrained pairwise matching" );

		final int[] subdividedTileGridSize = new int[ job.getDimensionality() ];
		Arrays.fill( subdividedTileGridSize, job.getArgs().subdivisionGridSize() );
		logWriter.println( "Subdivided tile grid size: " + Arrays.toString( subdividedTileGridSize ) );

		logWriter.println( "Number of phase correlation peaks to inspect: " + job.getArgs().numCheckPeaks() );
	}

	private TileInfo[] getStitchedTilesFromPreviousIteration() throws IOException
	{
		if ( iteration == 0 )
			return null;

		final DataProvider dataProvider = job.getDataProvider();
		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String filename = PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( 0 ) );

		final String previousIterationDirname = "iter" + ( iteration - 1 );
		final String previousStitchedTilesFilepath = PathResolver.get( basePath, previousIterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) );

		final TileInfo[] previousStitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( previousStitchedTilesFilepath ) ) );
		for ( final TileInfo previousStitchedTile : previousStitchedTiles )
			if ( previousStitchedTile.getTransform() == null )
				throw new RuntimeException( "all stitched tiles must have a stage->world transform" );
		return previousStitchedTiles;
	}

	/**
	 * Tries to create a predictive model based on the previous stitching solution if exists.
	 *
	 * @param iteration
	 * @return
	 * @throws IOException
	 */
	private TileSearchRadiusEstimator createSearchRadiusEstimator( final PrintWriter logWriter ) throws IOException
	{
		if ( iteration == 0 )
			return null;

		final TileInfo[] previousStitchedTiles = getStitchedTilesFromPreviousIteration();
		final double[] estimationWindow = TileSearchRadiusEstimator.getEstimationWindowSize( previousStitchedTiles[ 0 ].getSize(), job.getArgs().searchWindowSizeTiles() );

		if ( logWriter != null )
		{
			logWriter.println( "Creating search radius estimator with the following parameters:" );
			logWriter.println( "  estimation window: " + Arrays.toString( estimationWindow ) );
			logWriter.println( "  error ellipse radius multiplier: " + job.getArgs().searchRadiusMultiplier() + " (sigma)" );
			logWriter.println( "  min number of neighboring tiles: " + job.getArgs().minNumNeighboringTiles() );
			logWriter.println( "  using weighted predictions: " + job.getArgs().weightedPredictions() );
		}

		return new TileSearchRadiusEstimator(
				previousStitchedTiles,
				estimationWindow,
				job.getArgs().searchRadiusMultiplier(),
				job.getArgs().minNumNeighboringTiles(),
				job.getArgs().weightedPredictions()
			);
	}

	private TileInfo[] getTilesWithEstimatedTransformation( final PrintWriter logWriter ) throws PipelineExecutionException, IOException
	{
		final TileInfo[] tiles = new TileInfo[ job.getTiles( 0 ).length ];
		for ( int i = 0; i < tiles.length; ++i )
			tiles[ i ] = job.getTiles( 0 )[ i ].clone();

		if ( iteration == 0 )
			return tiles;

		final Map< Integer, TileInfo > previousStitchedTilesMap = Utils.createTilesMap( getStitchedTilesFromPreviousIteration() );
		int numTilesWithInsufficientNeighborhood = 0, numTilesWithoutTransformations = 0;
		for ( final TileInfo tile : tiles )
		{
			final TileInfo previousStitchedTile = previousStitchedTilesMap.get( tile.getIndex() );
			if ( job.getArgs().stitchingMode() == StitchingMode.FULL || previousStitchedTile == null )
			{
				// estimate tile transformation based on its neighboring tiles
				++numTilesWithoutTransformations;
				try
				{
					tile.setTransform(
							TransformedTileOperations.estimateAffineTransformation( tile, broadcastedSearchRadiusEstimator.value() )
						);
				}
				catch ( final NotEnoughNeighboringTilesException e )
				{
					++numTilesWithInsufficientNeighborhood;
					tile.setTransform( null );
				}
			}
			else
			{
				tile.setTransform( previousStitchedTile.clone().getTransform() );
			}
		}

		if ( logWriter != null )
		{
			logWriter.println( numTilesWithoutTransformations + " out of " + tiles.length + " tiles required estimating their transformations" );
			logWriter.println( "Estimated affine transformations for " + ( numTilesWithoutTransformations - numTilesWithInsufficientNeighborhood ) + " out of " + numTilesWithoutTransformations + " tiles " + " (others had insufficient neighborhood)" );
		}

		return tiles;
	}

	/**
	 * Initiates the computation for pairs that have not been precomputed. Stores the pairwise file for this iteration of stitching.
	 *
	 * @param overlappingBoxes
	 * @param iteration
	 * @throws PipelineExecutionException
	 * @throws IOException
	 */
	private void preparePairwiseShifts(
			final List< SubTilePair > overlappingBoxes,
			final int iteration,
			final PrintWriter logWriter ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String iterationDirname = "iter" + iteration;
		final String pairwiseFilename = "pairwise.json";
		dataProvider.createFolder( URI.create( PathResolver.get( basePath, iterationDirname ) ) );
		final String pairwisePath = PathResolver.get( basePath, iterationDirname, pairwiseFilename );

		final List< SerializablePairWiseStitchingResult > pairwiseShifts = tryLoadPrecomputedShifts( basePath );
		final List< SubTilePair > pendingOverlappingBoxes = removePrecomputedPendingPairs( pairwisePath, overlappingBoxes, pairwiseShifts );

		if ( pendingOverlappingBoxes.isEmpty() && !pairwiseShifts.isEmpty() )
		{
			// If we're able to load precalculated pairwise results, save some time skipping this step and jump to the global optimization
			System.out.println( "Successfully loaded all pairwise results from disk!" );
		}
		else
		{
			// Initiate the computation
			final List< SerializablePairWiseStitchingResult > pendingPairwiseStitchingResults = computePairwiseShifts( pendingOverlappingBoxes, logWriter );

			// merge results with preloaded pairwise shifts
			pairwiseShifts.addAll( pendingPairwiseStitchingResults );

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
	private List< SerializablePairWiseStitchingResult > tryLoadPrecomputedShifts( final String basePath ) throws PipelineExecutionException, IOException
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
				System.out.println( "Restitching only excluded pairs" );
				// use pairwise-used from the previous iteration without restitching
				if ( !dataProvider.fileExists( URI.create( pairwisePath ) ) )
				{
					dataProvider.copyFile(
							URI.create( PathResolver.get( basePath, previousIterationDirname, Utils.addFilenameSuffix( pairwiseFilename, "-used" ) ) ),
							URI.create( pairwisePath )
						);
				}
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
	private List< SubTilePair > removePrecomputedPendingPairs(
			final String pairwisePath,
			final List< SubTilePair > overlappingBoxes,
			final List< SerializablePairWiseStitchingResult > pairwiseShifts ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		// remove redundant pairs (that are not contained in the given overlappingTiles list)
		final Map< Integer, Set< Integer > > overlappingPairsCache = new TreeMap<>();
		for ( final SubTilePair boxPair : overlappingBoxes )
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
				indexes[ i ] = result.getSubTilePair().toArray()[ i ].getIndex();

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

		// Create a cache to efficiently lookup the existing pairs of tiles loaded from disk
		final Map< Integer, Set< Integer > > cache = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult result : pairwiseShifts )
		{
			final Integer[] indexes = new Integer[ 2 ];
			for ( int i = 0; i < 2; ++i )
				if ( indexes[ i ] == null )
					indexes[ i ] = result.getSubTilePair().toArray()[ i ].getIndex();
				else if ( !indexes[ i ].equals( result.getSubTilePair().toArray()[ i ].getIndex() ) )
					throw new PipelineExecutionException( "Tile indexes do not match" );

			final int firstIndex =  Math.min( indexes[ 0 ], indexes[ 1 ] ), secondIndex  =  Math.max( indexes[ 0 ], indexes[ 1 ] );
			if ( !cache.containsKey( firstIndex ) )
				cache.put( firstIndex, new TreeSet<>() );
			cache.get( firstIndex ).add( secondIndex );
		}

		// Populate a new list of pending tile pairs (add only those pairs that are not contained in the cache)
		final List< SubTilePair > pendingOverlappingBoxes = new ArrayList<>();
		for ( final SubTilePair boxPair : overlappingBoxes )
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
	/*private void saveSearchRadiusStats( final List< StitchingResult > stitchingResults, final String searchRadiusStatsPath ) throws PipelineExecutionException
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
	}*/

	/**
	 * Computes the best possible pairwise shifts between every pair of tiles on a Spark cluster.
	 * It uses phase correlation for measuring similarity between two images.
	 * @param overlappingBoxes
	 * @param iteration
	 * @return
	 * @throws PipelineExecutionException
	 * @throws IOException
	 */
	private < T extends NativeType< T > & RealType< T > > List< SerializablePairWiseStitchingResult > computePairwiseShifts(
			final List< SubTilePair > overlappingBoxes,
			final PrintWriter logWriter ) throws PipelineExecutionException, IOException
	{
		System.out.println( "Processing " + overlappingBoxes.size() + " pairs..." );

		final LongAccumulator notEnoughNeighborsWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noOverlapWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noPeaksWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();

		final List< SerializablePairWiseStitchingResult > pairwiseStitchingResults = sparkContext.parallelize( overlappingBoxes, overlappingBoxes.size() ).map(
				subTilePair ->
				{
					try
					{
						return new StitchSubTilePair<>(
								job,
								broadcastedSearchRadiusEstimator.value(),
								broadcastedFlatfieldsForChannels.value(),
								broadcastedTileMapsForChannels.value()
							)
						.stitchSubTilePair( subTilePair );
					}
					catch ( final Exception e )
					{
						throw new RuntimeException( "Exception when stitching " + subTilePair + ": " + e.getMessage(), e );
					}
				}
			).collect();

		broadcastedSearchRadiusEstimator.destroy();

		int validPairs = 0;
		for ( final SerializablePairWiseStitchingResult result : pairwiseStitchingResults )
			if ( result.getIsValidOverlap() )
				++validPairs;

		if ( logWriter != null )
		{
			logWriter.println();
			logWriter.println( "======== Pairwise stitching completed ========" );
			logWriter.println( "Total pairs: " + pairwiseStitchingResults.size() );
			logWriter.println( "Valid pairs: " + validPairs + ", invalid pairs: " + ( pairwiseStitchingResults.size() - validPairs ) );
			logWriter.println( "    not enough neighbors within estimation window: " + notEnoughNeighborsWithinConfidenceIntervalPairsCount.value() );
			logWriter.println( "    <= 1px overlap within search radius: " + noOverlapWithinConfidenceIntervalPairsCount.value() );
			logWriter.println( "    no peaks found within search radius: " + noPeaksWithinConfidenceIntervalPairsCount.value() );
			logWriter.println();
		}

		return pairwiseStitchingResults;
	}
}
