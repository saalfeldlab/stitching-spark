package org.janelia.stitching;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;
import org.janelia.flatfield.FlatfieldCorrection;
import org.janelia.stitching.StitchingArguments.RestitchingMode;
import org.janelia.stitching.analysis.FilterAdjacentShifts;
import org.janelia.util.Conversions;
import org.janelia.util.concurrent.SameThreadExecutorService;

import ij.ImagePlus;
import mpicbg.imglib.custom.OffsetConverter;
import net.imglib2.Cursor;
import net.imglib2.FinalDimensions;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

/**
 * Computes updated tile positions using phase correlation for pairwise matches and then global optimization for fitting all of them together.
 * Saves updated tile configuration on the disk.
 *
 * @author Igor Pisarev
 */

public class PipelineStitchingStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -7152174064553332061L;

	public PipelineStitchingStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run() throws PipelineExecutionException
	{
		final DataProvider dataProvider = job.getDataProvider();

		final List< TilePair > overlappingTiles = TileOperations.findOverlappingTiles( job.getTiles( 0 ) );
		System.out.println( "Overlapping pairs count = " + overlappingTiles.size() );

		// Remove pairs with small overlap area if only adjacent pairs are requested
		if ( !job.getArgs().useAllPairs() )
		{
			final List< TilePair > adjacentOverlappingTiles = FilterAdjacentShifts.filterAdjacentPairs( overlappingTiles );
			overlappingTiles.clear();
			overlappingTiles.addAll( adjacentOverlappingTiles );
			System.out.println( "Retaining only " + overlappingTiles.size() + " adjacent pairs of them" );
		}

		//final boolean pairsJustUpdated = false;

		final StitchingOptimizer optimizer = new StitchingOptimizer( job, sparkContext );
		try
		{
			for ( int iteration = 0; ; ++iteration )
			{
				// check if number of stitched tiles has increased compared to the previous iteration
				final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
				final String filename = PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( 0 ) );
				final String iterationDirname = "iter" + iteration;
				final String stitchedTilesFilepath = PathResolver.get( basePath, iterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) );

				if ( !dataProvider.fileExists( URI.create( stitchedTilesFilepath ) ) )
				{
					System.out.println( "************** Iteration " + iteration + " **************" );
					preparePairwiseShiftsMulti( overlappingTiles, iteration );
					optimizer.optimize( iteration );
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

				if ( iteration == 0 )
				{
					// stop if all input tiles are included in the stitched set
					if ( stageTiles.length == stitchedTiles.length )
					{
						System.out.println( "Stopping on iteration " + iteration + ": all input tiles (n=" + stageTiles.length + ") are included in the stitched set" );
						copyFinalSolution( iteration );
						break;
					}
				}
				else
				{
					final String previousIterationDirname = iteration == 0 ? null : "iter" + ( iteration - 1 );

					final String previousStitchedTilesFilepath = PathResolver.get( basePath, previousIterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) );
					final TileInfo[] previousStitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( previousStitchedTilesFilepath ) ) );

					final String usedPairsFilepath = PathResolver.get( basePath, iterationDirname, "pairwise-used.json" );
					final String previousUsedPairsFilepath = PathResolver.get( basePath, previousIterationDirname, "pairwise-used.json" );
					final List< SerializablePairWiseStitchingResult[] > usedPairs = TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( URI.create( usedPairsFilepath ) ) );
					final List< SerializablePairWiseStitchingResult[] > previousUsedPairs = TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( URI.create( previousUsedPairsFilepath ) ) );

					if ( stitchedTiles.length < previousStitchedTiles.length || ( stitchedTiles.length == previousStitchedTiles.length && usedPairs.size() <= previousUsedPairs.size() ) )
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
		catch ( final IOException e )
		{
			System.out.println( "Something went wrong during stitching:" );
			e.printStackTrace();
			throw new PipelineExecutionException( e );
		}
	}

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

	private void preparePairwiseShiftsMulti( final List< TilePair > overlappingTiles, final int iteration ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();

		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String iterationDirname = "iter" + iteration;
		final String previousIterationDirname = iteration == 0 ? null : "iter" + ( iteration - 1 );
		final String pairwiseFilename = "pairwise.json";
		dataProvider.createFolder( URI.create( PathResolver.get( basePath, iterationDirname ) ) );
		final String pairwisePath = PathResolver.get( basePath, iterationDirname, pairwiseFilename );

		if ( iteration == 0 )
		{
			// use the pairwise file from the previous run in the old mode if exists
			final String oldPairwiseFile = PathResolver.get( basePath, pairwiseFilename );
			if ( dataProvider.fileExists( URI.create( oldPairwiseFile ) ) )
				dataProvider.moveFile( URI.create( oldPairwiseFile ), URI.create( pairwisePath ) );
		}
		else
		{
			if ( job.getArgs().restitchingMode() == RestitchingMode.INCREMENTAL )
			{
				System.out.println( "Restitching only excluded pairs" );
				// use pairwise-used from the previous iteration, so they will not be restitched
				if ( !dataProvider.fileExists( URI.create( pairwisePath ) ) )
					dataProvider.copyFile(
							URI.create( PathResolver.get( basePath, previousIterationDirname, Utils.addFilenameSuffix( pairwiseFilename, "-used" ) ) ),
							URI.create( pairwisePath )
						);
			}
			else
			{
				System.out.println( "Full restitching" );
			}
		}

		// Try to load precalculated shifts for some pairs of tiles
		final List< SerializablePairWiseStitchingResult[] > pairwiseShiftsMulti = new ArrayList<>();

		if ( dataProvider.fileExists( URI.create( pairwisePath ) ) )
		{
			try
			{
				System.out.println( "try to load pairwise results from disk" );
				pairwiseShiftsMulti.addAll( TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( URI.create( pairwisePath ) ) ) );
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

		// remove redundant pairs (that are not contained in the given overlappingTiles list)
		{
			final Map< Integer, Set< Integer > > overlappingPairsCache = new TreeMap<>();
			for ( final TilePair pair : overlappingTiles )
			{
				final int ind1 = Math.min( pair.getA().getIndex(), pair.getB().getIndex() );
				final int ind2 = Math.max( pair.getA().getIndex(), pair.getB().getIndex() );
				if ( !overlappingPairsCache.containsKey( ind1 ) )
					overlappingPairsCache.put( ind1, new TreeSet<>() );
				overlappingPairsCache.get( ind1 ).add( ind2 );
			}
			int pairsRemoved = 0;
			for ( final Iterator< SerializablePairWiseStitchingResult[] > it = pairwiseShiftsMulti.iterator(); it.hasNext(); )
			{
				final SerializablePairWiseStitchingResult[] resultMulti = it.next();
				final Integer[] indexes = new Integer[ 2 ];
				for ( final SerializablePairWiseStitchingResult result : resultMulti )
					for ( int i = 0; i < 2; ++i )
						if ( indexes[ i ] == null )
							indexes[ i ] = result.getTilePair().toArray()[ i ].getIndex();
						else if ( !indexes[ i ].equals( result.getTilePair().toArray()[ i ].getIndex() ) )
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
				TileInfoJSONProvider.savePairwiseShiftsMulti( pairwiseShiftsMulti, dataProvider.getJsonWriter( URI.create( pairwisePath ) ) );
		}

		// find only pairs that need to be computed
		final List< TilePair > pendingOverlappingTiles = new ArrayList<>();
		{
			// Create a cache to efficiently lookup the existing pairs of tiles loaded from disk
			final Map< Integer, Set< Integer > > cache = new TreeMap<>();
			for ( final SerializablePairWiseStitchingResult[] resultMulti : pairwiseShiftsMulti )
			{
				final Integer[] indexes = new Integer[ 2 ];
				for ( final SerializablePairWiseStitchingResult result : resultMulti )
					for ( int i = 0; i < 2; ++i )
						if ( indexes[ i ] == null )
							indexes[ i ] = result.getTilePair().toArray()[ i ].getIndex();
						else if ( !indexes[ i ].equals( result.getTilePair().toArray()[ i ].getIndex() ) )
							throw new PipelineExecutionException( "Tile indexes do not match" );

				final int firstIndex =  Math.min( indexes[ 0 ], indexes[ 1 ] ), secondIndex  =  Math.max( indexes[ 0 ], indexes[ 1 ] );
				if ( !cache.containsKey( firstIndex ) )
					cache.put( firstIndex, new TreeSet<>() );
				cache.get( firstIndex ).add( secondIndex );
			}

			// Populate a new list of pending tile pairs (add only those pairs that are not contained in the cache)
			for ( final TilePair pair : overlappingTiles )
			{
				final int firstIndex =  Math.min( pair.getA().getIndex(), pair.getB().getIndex() ),
						secondIndex =  Math.max( pair.getA().getIndex(), pair.getB().getIndex() );
				if ( !cache.containsKey( firstIndex ) || !cache.get( firstIndex ).contains( secondIndex ) )
					pendingOverlappingTiles.add( pair );
			}
		}

		if ( pendingOverlappingTiles.isEmpty() && !pairwiseShiftsMulti.isEmpty() )
		{
			// If we're able to load precalculated pairwise results, save some time skipping this step and jump to the global optimization
			System.out.println( "Successfully loaded all pairwise results from disk!" );
		}
		else
		{
			final String statsTileConfigurationPath = iteration == 0 ? null : PathResolver.get(
					basePath,
					previousIterationDirname,
					Utils.addFilenameSuffix( PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( 0 ) ), "-stitched" )
				);

			// Initiate the computation
			final List< SerializablePairWiseStitchingResult[] > adjacentShiftsMulti = computePairwiseShifts( pendingOverlappingTiles, statsTileConfigurationPath );
			pairwiseShiftsMulti.addAll( adjacentShiftsMulti );

			try {
				System.out.println( "Stitched all tiles pairwise, store this information on disk.." );
				TileInfoJSONProvider.savePairwiseShiftsMulti( pairwiseShiftsMulti, dataProvider.getJsonWriter( URI.create( pairwisePath ) ) );
			} catch ( final IOException e ) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Computes the best possible pairwise shifts between every pair of tiles on a Spark cluster.
	 * It uses phase correlation for measuring similarity between two images.
	 * @throws IOException
	 */
	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > List< SerializablePairWiseStitchingResult[] > computePairwiseShifts( final List< TilePair > overlappingTiles, final String statsTileConfigurationPath ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();

		// try to load the stitched tile configuration from the previous iteration
		final TileSearchRadiusEstimator searchRadiusEstimator;
		if ( statsTileConfigurationPath != null )
		{
			System.out.println( "=== Building prediction model based on previous stitching solution ===" );
			try
			{
				final TileInfo[] statsTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( statsTileConfigurationPath ) ) );
				System.out.println( "-- Creating search radius estimator using " + job.getTiles( 0 ).length + " stage tiles and " + statsTiles.length + " stitched tiles --" );
				searchRadiusEstimator = new TileSearchRadiusEstimator( job.getTiles( 0 ), statsTiles );
				System.out.println( "-- Created search radius estimator. Estimation window size (neighborhood): " + Arrays.toString( Intervals.dimensionsAsIntArray( searchRadiusEstimator.getEstimationWindowSize() ) ) + " --" );
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
		final Broadcast< TileSearchRadiusEstimator > broadcastedSearchRadiusEstimator = sparkContext.broadcast( searchRadiusEstimator );

		// for dividing the overlap area into 1x1 or 2x2, etc. which leads to 1 or 4 matches per pair of tiles
		final int dividedParts = 1;

		System.out.println( "Broadcasting flatfield correction images" );
		final List< RandomAccessiblePairNullable< U, U > > flatfieldCorrectionForChannels = new ArrayList<>();
		for ( final String channelPath : job.getArgs().inputTileConfigurations() )
		{
			final String channelPathNoExt = channelPath.lastIndexOf( '.' ) != -1 ? channelPath.substring( 0, channelPath.lastIndexOf( '.' ) ) : channelPath;
			flatfieldCorrectionForChannels.add( FlatfieldCorrection.loadCorrectionImages( dataProvider, channelPathNoExt, job.getDimensionality() ) );
		}
		final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedFlatfieldCorrectionForChannels = sparkContext.broadcast( flatfieldCorrectionForChannels );

		final List< Map< Integer, TileInfo > > tileChannelMappingByIndex = new ArrayList<>();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
			tileChannelMappingByIndex.add( Utils.createTilesMap( job.getTiles( channel ) ) );
		final Broadcast< List< Map< Integer, TileInfo > > > broadcastedTileChannelMappingByIndex = sparkContext.broadcast( tileChannelMappingByIndex );

		System.out.println( "Processing " + overlappingTiles.size() + " pairs..." );

		final LongAccumulator notEnoughNeighborsWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noOverlapWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noPeaksWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();

		final JavaRDD< TilePair > rdd = sparkContext.parallelize( overlappingTiles, overlappingTiles.size() );
		final JavaRDD< SerializablePairWiseStitchingResult[] > pairwiseStitching = rdd.map( pairOfTiles ->
			{
				final DataProvider dataProviderLocal = job.getDataProvider();

				// stats
				final TileSearchRadiusEstimator localSearchRadiusEstimator = broadcastedSearchRadiusEstimator.value();
				final SearchRadius searchRadius;

				final TileInfo[] pair = pairOfTiles.toArray();
				final Interval[] overlaps = new Boundaries[ pair.length ];
				final ImagePlus[] imps = new ImagePlus[ pair.length ];

				final TileInfo fixedTile = pair[ 0 ], movingTile = pair[ 1 ];

				System.out.println( "Processing tile pair " + pairOfTiles );

				final double[] voxelDimensions = fixedTile.getPixelResolution();
				final double[] normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( voxelDimensions );
				System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );
				final double blurSigma = job.getArgs().blurSigma();
				final double[] blurSigmas = new  double[ normalizedVoxelDimensions.length ];
				for ( int d = 0; d < blurSigmas.length; d++ )
					blurSigmas[ d ] = blurSigma / normalizedVoxelDimensions[ d ];

				if ( localSearchRadiusEstimator != null )
				{
					final int minNumNearestNeighbors = job.getArgs().minStatsNeighborhood();
					final SearchRadius[] tilesSearchRadius = new SearchRadius[ pair.length ];
					for ( int j = 0; j < pair.length; j++ )
					{
						tilesSearchRadius[ j ] = localSearchRadiusEstimator.getSearchRadiusTreeWithinEstimationWindow( pair[ j ] );
						if ( tilesSearchRadius[ j ].getUsedPointsIndexes().size() < minNumNearestNeighbors )
						{
							notEnoughNeighborsWithinConfidenceIntervalPairsCount.add( 1 );

//								System.out.println( "Found " + searchRadiusEstimationWindow.getUsedPointsIndexes().size() + " neighbors within the search window but we require " + numNearestNeighbors + " nearest neighbors, perform a K-nearest neighbor search instead..." );
							System.out.println();
							System.out.println( pairOfTiles + ": found " + tilesSearchRadius[ j ].getUsedPointsIndexes().size() + " neighbors within the search window of the " + ( j == 0 ? "fixed" : "moving" ) + " tile but we require at least " + minNumNearestNeighbors + " nearest neighbors, so ignore this tile pair for now" );
							System.out.println();

							final SerializablePairWiseStitchingResult[] invalidResult = new SerializablePairWiseStitchingResult[ dividedParts ];
							for ( int i = 0; i < invalidResult.length; ++i )
							{
								invalidResult[ i ] = new SerializablePairWiseStitchingResult( pairOfTiles, null, 0 );
								invalidResult[ i ].setIsValidOverlap( false );
							}
							return invalidResult;

//								final SearchRadius searchRadiusNearestNeighbors = localSearchRadiusEstimator.getSearchRadiusTreeUsingKNearestNeighbors( movingTile, numNearestNeighbors );
//								if ( searchRadiusNearestNeighbors.getUsedPointsIndexes().size() != numNearestNeighbors )
//								{
//									if ( localSearchRadiusEstimator.getNumPoints() >= numNearestNeighbors )
//										throw new PipelineExecutionException( "Required " + numNearestNeighbors + " nearest neighbors, found only " + searchRadiusNearestNeighbors.getUsedPointsIndexes().size() );
//									else if ( searchRadiusNearestNeighbors.getUsedPointsIndexes().size() != localSearchRadiusEstimator.getNumPoints() )
//										throw new PipelineExecutionException( "Number of tiles in the stitched solution = " + localSearchRadiusEstimator.getNumPoints() + ", found " + searchRadiusNearestNeighbors.getUsedPointsIndexes().size() + " neighbors" );
//									else
//										System.out.println( "Got only " + localSearchRadiusEstimator.getNumPoints() + " neighbors as it is the size of the stitched solution" );
//								}
//								searchRadius = searchRadiusNearestNeighbors;
						}
						else
						{
							System.out.println( pairOfTiles + ": found " + tilesSearchRadius[ j ].getUsedPointsIndexes().size() + " neighbors within the search window for the " + ( j == 0 ? "fixed" : "moving" ) + " tile, estimate search radius based on that" );
						}
					}

					System.out.println();
					System.out.println( pairOfTiles + ": found search radiuses for both tiles in the pair, get a combined search radius for the moving tile" );
					System.out.println();

					searchRadius = localSearchRadiusEstimator.getCombinedCovariancesSearchRadius( tilesSearchRadius[ 0 ], tilesSearchRadius[ 1 ] );

					final Interval boundingBox = Intervals.smallestContainingInterval( searchRadius.getBoundingBox() );
					System.out.println( String.format( pairOfTiles + ": estimated combined search radius for the moving tile. Bounding box: min=%s, max=%s, size=%s",
							Arrays.toString( Intervals.minAsIntArray( boundingBox ) ),
							Arrays.toString( Intervals.maxAsIntArray( boundingBox ) ),
							Arrays.toString( Intervals.dimensionsAsIntArray( boundingBox ) ) ) );
				}
				else
				{
					searchRadius = null;
				}

				// detect dimension with short edge
				int shortEdgeDimension = -1;
				{
					final Boundaries testOverlap = TileOperations.getOverlappingRegion( pair[ 0 ], pair[ 1 ] );
					if ( testOverlap != null )
					{
						if ( shortEdgeDimension == -1 )
							for ( int d = 0; d < testOverlap.numDimensions(); ++d )
								if ( shortEdgeDimension == -1 || testOverlap.dimension( d ) < testOverlap.dimension( shortEdgeDimension ) )
									shortEdgeDimension = d;
					}
				}

				// find overlapping regions
				if ( searchRadius == null )
				{
					for ( int j = 0; j < pair.length; j++ )
					{
						System.out.println( "Prepairing #" + (j+1) + " of a pair,  padding ROI by " + Arrays.toString( job.getArgs().padding() ) + "(padding arg)" );
						final Boundaries overlap = TileOperations.getOverlappingRegion( pair[ j ], pair[ ( j + 1 ) % pair.length ] );
						overlaps[ j ] = TileOperations.padInterval(
								overlap,
								new FinalDimensions( pair[ j ].getSize() ),
								job.getArgs().padding()
							);
					}
				}
				else
				{
					final Pair< Interval, Interval > overlapsAdjustedToSearchRadius = adjustOverlappingRegion( pairOfTiles, searchRadius );
					if ( overlapsAdjustedToSearchRadius == null )
					{
						noOverlapWithinConfidenceIntervalPairsCount.add( 1 );
						System.out.println( pairOfTiles + ": cannot find a non-empty overlap that covers the confidence range (The confidence range says there is no overlap?)" );

						final SerializablePairWiseStitchingResult[] invalidResult = new SerializablePairWiseStitchingResult[ dividedParts ];
						for ( int i = 0; i < invalidResult.length; ++i )
						{
							invalidResult[ i ] = new SerializablePairWiseStitchingResult( pairOfTiles, null, 0 );
							invalidResult[ i ].setIsValidOverlap( false );
						}
						return invalidResult;
					}

					overlaps[ 0 ] = overlapsAdjustedToSearchRadius.getA();
					overlaps[ 1 ] = overlapsAdjustedToSearchRadius.getB();

					// find the shortest edge if the initial overlap was null (shortEdgeDimension is still -1)
					if ( shortEdgeDimension == -1 )
						for ( int d = 0; d < overlaps[ 0 ].numDimensions(); ++d )
							if ( shortEdgeDimension == -1 || overlaps[ 0 ].dimension( d ) < overlaps[ 0 ].dimension( shortEdgeDimension ) )
								shortEdgeDimension = d;
				}

				// when the overlap is 1px thick in Z, it breaks the code below because corresponding cropped images are 2D in this case, so just ignore this pair
				// gaussian blur and phase correlation also require at least 2px in every dimension
				if ( Arrays.stream( Intervals.dimensionsAsIntArray( overlaps[ 0 ] ) ).min().getAsInt() <= 1 )
				{
					noOverlapWithinConfidenceIntervalPairsCount.add( 1 );
					System.out.println( pairOfTiles + ": overlap is <= 1px" );

					final SerializablePairWiseStitchingResult[] invalidResult = new SerializablePairWiseStitchingResult[ dividedParts ];
					for ( int i = 0; i < invalidResult.length; ++i )
					{
						invalidResult[ i ] = new SerializablePairWiseStitchingResult( pairOfTiles, null, 0 );
						invalidResult[ i ].setIsValidOverlap( false );
					}
					return invalidResult;
				}

				// prepare images
				for ( int j = 0; j < pair.length; j++ )
				{
					System.out.println( "Averaging corresponding tile images for " + job.getChannels() + " channels" );
//					final ComparableTuple< Integer > coordinates = new ComparableTuple<>( Conversions.toBoxedArray( Utils.getTileCoordinates( pair[ j ] ) ) );
					final Integer tileIndex = pair[ j ].getIndex();
					int channelsUsed = 0;
					final ImagePlusImg< FloatType, ? > dst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( overlaps[ j ] ) );
					for ( int channel = 0; channel < job.getChannels(); channel++ )
					{
						final TileInfo tileInfo = broadcastedTileChannelMappingByIndex.value().get( channel ).get( tileIndex );
//						for ( final TileInfo tile : job.getTiles( channel ) )
//						{
//							if ( coordinates.compareTo( new ComparableTuple<>( Conversions.toBoxedArray( Utils.getTileCoordinates( tile ) ) ) ) == 0 )
//							{
//								tileInfo = tile;
//								break;
//							}
//						}

						// skip if no tile exists for this channel at this particular stage position
						if ( tileInfo == null )
							throw new PipelineExecutionException( pairOfTiles + ": cannot find corresponding tile for this channel" );

						// FIXME: throw exception in case some image files are missing (or, check for missing files beforehand)
						final RandomAccessibleInterval< T > img = TileLoader.loadTile( tileInfo, dataProviderLocal );
						if ( img == null )
							throw new PipelineExecutionException( "Cannot load tile image: " + tileInfo.getFilePath() );

						final T type = Util.getTypeFromInterval( img );
//						if ( imp != null )
						{
							// warn if image type and/or size do not match metadata
							if ( !type.getClass().equals( tileInfo.getType().getType().getClass() ) )
								throw new PipelineExecutionException( String.format( "Image type %s does not match the value from metadata %s", type.getClass().getName(), tileInfo.getType() ) );
							if ( !Arrays.equals( Intervals.dimensionsAsLongArray( img ), tileInfo.getSize() ) )
								throw new PipelineExecutionException( String.format( "Image size %s does not match the value from metadata %s", Arrays.toString( Intervals.dimensionsAsLongArray( img ) ), Arrays.toString( tileInfo.getSize() ) ) );

							final RandomAccessibleInterval< T > imgCrop = Views.interval( img, overlaps[ j ] );

							final RandomAccessibleInterval< FloatType > sourceInterval;
							final RandomAccessiblePairNullable< U, U > flatfield = broadcastedFlatfieldCorrectionForChannels.value().get( channel );
							if ( flatfield != null )
							{
								System.out.println( "Flat-fielding image.." );
								final FlatfieldCorrectedRandomAccessible< T, U > flatfieldCorrected = new FlatfieldCorrectedRandomAccessible<>( imgCrop, flatfield.toRandomAccessiblePair() );
								final RandomAccessibleInterval< U > correctedImg = Views.interval( flatfieldCorrected, imgCrop );
								sourceInterval = Converters.convert( correctedImg, new RealFloatConverter<>(), new FloatType() );
							}
							else
							{
								sourceInterval = Converters.convert( imgCrop, new RealFloatConverter<>(), new FloatType() );
							}

							final Cursor< FloatType > srcCursor = Views.flatIterable( sourceInterval ).cursor();
							final Cursor< FloatType > dstCursor = Views.flatIterable( dst ).cursor();
							while ( dstCursor.hasNext() || srcCursor.hasNext() )
								dstCursor.next().add( srcCursor.next() );

							++channelsUsed;
						}
					}

					if ( channelsUsed == 0 )
						throw new PipelineExecutionException( pairOfTiles + ": images are missing in all channels" );

					final FloatType denom = new FloatType( channelsUsed );
					final Cursor< FloatType > dstCursor = Views.iterable( dst ).cursor();
					while ( dstCursor.hasNext() )
						dstCursor.next().div( denom );

					if ( blurSigma > 0 )
					{
						System.out.println( String.format( "Blurring the overlap area of size %s with sigmas=%s (s=%f)", Arrays.toString( Intervals.dimensionsAsLongArray( dst ) ), Arrays.toString( blurSigmas ), blurSigma ) );
						blur( dst, blurSigmas );
					}

					imps[ j ] = dst.getImagePlus();
					Utils.workaroundImagePlusNSlices( imps[ j ] );
				}

				// divide hyperplane with long edges into subintervals
				final Boundaries fullRoi = new Boundaries( Conversions.toLongArray( Utils.getImagePlusDimensions( imps[ 0 ] ) ) );
				final int[] roiPartsCount = new int[ fullRoi.numDimensions() ];
				Arrays.fill( roiPartsCount, dividedParts );
				roiPartsCount[ shortEdgeDimension ] = 1;
				final List< TileInfo > roiParts = TileOperations.divideSpaceByCount( fullRoi, roiPartsCount );

				System.out.println( String.format( "Stitching (%d subintervals with grid of %s)..", roiParts.size(), Arrays.toString( roiPartsCount ) ) );

				final SerializablePairWiseStitchingResult[] roiPartsResults = new SerializablePairWiseStitchingResult[ roiParts.size() ];
				for ( int roiPartIndex = 0; roiPartIndex < roiParts.size(); ++roiPartIndex )
				{
					final Boundaries roiPartInterval = roiParts.get( roiPartIndex ).getBoundaries();
					final ImagePlus[] roiPartImps = new ImagePlus[ 2 ];
					for ( int i = 0; i < 2; ++i )
					{
						if ( roiParts.size() > 1 )
						{
							final RandomAccessibleInterval< FloatType > roiImg = ImagePlusImgs.from( imps[ i ] );
							final RandomAccessibleInterval< FloatType > roiPartImg = Views.offsetInterval( roiImg, roiPartInterval );
							final ImagePlusImg< FloatType, ? > roiPartDst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( roiPartInterval ) );
							final Cursor< FloatType > srcCursor = Views.flatIterable( roiPartImg ).cursor();
							final Cursor< FloatType > dstCursor = Views.flatIterable( roiPartDst ).cursor();
							while ( dstCursor.hasNext() || srcCursor.hasNext() )
								dstCursor.next().set( srcCursor.next() );
							roiPartImps[ i ] = roiPartDst.getImagePlus();
							Utils.workaroundImagePlusNSlices( roiPartImps[ i ] );
						}
						else
						{
							roiPartImps[ i ] = imps[ i ];
						}
					}

					// compute variance within this ROI for both images
					double pixelSum = 0, pixelSumSquares = 0;
					long pixelCount = 0;
					for ( int i = 0; i < 2; ++i )
					{
						final ImagePlusImg< FloatType, ? > roiImg = ImagePlusImgs.from( roiPartImps[ i ] );
						final Cursor< FloatType > roiImgCursor = Views.iterable( roiImg ).cursor();
						while ( roiImgCursor.hasNext() )
						{
							final double val = roiImgCursor.next().get();
							pixelSum += val;
							pixelSumSquares += Math.pow( val, 2 );
						}
						pixelCount += roiImg.size();
					}
					final double variance = pixelSumSquares / pixelCount - Math.pow( pixelSum / pixelCount, 2 );

					final int timepoint = 1;
					PairwiseStitchingPerformer.setThreads( 1 ); // TODO: determine automatically based on parallelism / smth else. Or, use different values for local/cluster configurations

					// for transforming 'overlap offset' to 'global offset'
					final long[][] roiToTileOffset = new long[ 2 ][];
					for ( int i = 0; i < 2; ++i )
					{
						roiToTileOffset[ i ] = new long[ roiPartInterval.numDimensions() ];
						for ( int d = 0; d < roiToTileOffset[ i ].length; ++d )
							roiToTileOffset[ i ][ d ] = overlaps[ i ].min( d ) + roiPartInterval.min( d );
					}

					// 'global offset' is the position of the fixed tile so the relative shift can be transformed to the global coordinate space
					final double[] globalOffset = new double[ fixedTile.numDimensions() ];
					for ( int d = 0; d < globalOffset.length; ++d )
						globalOffset[ d ] = fixedTile.getPosition( d );

					final OffsetConverter offsetConverter = new FinalOffsetConverter( roiToTileOffset, globalOffset );

					final SerializablePairWiseStitchingResult[] results = PairwiseStitchingPerformer.stitchPairwise(
							roiPartImps[0], roiPartImps[1], null, null, null, null, timepoint, timepoint, job.getParams(), 1,
							searchRadius, offsetConverter
						);

					final SerializablePairWiseStitchingResult result = results[ 0 ];

					if ( result == null )
					{
						noPeaksWithinConfidenceIntervalPairsCount.add( 1 );
						System.out.println( pairOfTiles + ": no peaks found within the confidence interval" );

						final SerializablePairWiseStitchingResult invalidResult = new SerializablePairWiseStitchingResult( pairOfTiles, null, 0 );
						invalidResult.setIsValidOverlap( false );
						roiPartsResults[ roiPartIndex ] = invalidResult;
					}
					else
					{
						result.setTilePair( pairOfTiles );
						result.setVariance( variance );

						// compute new offset between original tiles
						final double[] originalTileOffset = offsetConverter.roiOffsetToTileOffset( Conversions.toDoubleArray( result.getOffset() ) );
						for ( int d = 0; d < originalTileOffset.length; ++d )
							result.getOffset()[ d ] = ( float ) originalTileOffset[ d ];

						roiPartsResults[ roiPartIndex ] = result;
					}

					for ( int i = 0; i < 2; i++ )
						roiPartImps[ i ].close();
				}

				for ( int i = 0; i < 2; i++ )
					imps[ i ].close();

				System.out.println( "Stitched tile pair " + pairOfTiles + ", got " + roiPartsResults.length + " matches" );
				return roiPartsResults;
			} );

		final List< SerializablePairWiseStitchingResult[] > stitchingResults = pairwiseStitching.collect();

		broadcastedFlatfieldCorrectionForChannels.destroy();
		broadcastedSearchRadiusEstimator.destroy();
		broadcastedTileChannelMappingByIndex.destroy();

		int validPairs = 0;
		for ( final SerializablePairWiseStitchingResult[] shiftMulti : stitchingResults )
		{
			final SerializablePairWiseStitchingResult shift = shiftMulti[ 0 ];
			if ( shift.getIsValidOverlap() )
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

	private static < T extends NumericType< T > > void blur(
			final RandomAccessibleInterval< T > image,
			final double[] sigmas ) throws IncompatibleTypeException
	{
		final RandomAccessible< T > extendedImage = Views.extendMirrorSingle( image );
		Gauss3.gauss( sigmas, extendedImage, image, new SameThreadExecutorService() );
	}

	public static Pair< Interval, Interval > adjustOverlappingRegion( final TilePair tilePair, final SearchRadius combinedSearchRadius )
	{
		// adjust the ROI to capture the search radius entirely
		// try all corners of the bounding box of the search radius and use the largest overlaps
		final Interval searchRadiusBoundingBox = Intervals.smallestContainingInterval( combinedSearchRadius.getBoundingBox() );

		final TileInfo fixedTile = tilePair.getA();
		final TileInfo movingTile = tilePair.getB();

		final TileInfo[] pair = tilePair.toArray();
		final Interval[] overlaps = new Interval[ 2 ];
		for ( int j = 0; j < 2; ++j )
		{
			final int[] cornersPos = new int[ searchRadiusBoundingBox.numDimensions() ];
			final int[] cornersDimensions = new int[ searchRadiusBoundingBox.numDimensions() ];
			Arrays.fill( cornersDimensions, 2 );
			final IntervalIterator cornerIntervalIterator = new IntervalIterator( cornersDimensions );

			final long[] overlappingRegionMin = new long[ searchRadiusBoundingBox.numDimensions() ], overlappingRegionMax = new long[ searchRadiusBoundingBox.numDimensions() ];
			Arrays.fill( overlappingRegionMin, Long.MAX_VALUE);
			Arrays.fill( overlappingRegionMax, Long.MIN_VALUE);

			while ( cornerIntervalIterator.hasNext() )
			{
				cornerIntervalIterator.fwd();
				cornerIntervalIterator.localize( cornersPos );

				final double[] testMovingTilePosition = new double[ searchRadiusBoundingBox.numDimensions() ];
				for ( int d = 0; d < cornersPos.length; ++d )
					testMovingTilePosition[ d ] = ( cornersPos[ d ] == 0 ? searchRadiusBoundingBox.min( d ) : searchRadiusBoundingBox.max( d ) );

				// create fake tile to be able to find an overlapping region with the fixed tile
				// TODO: use Intervals for this purpose
				final TileInfo movingTilePossibility = movingTile.clone();
				movingTilePossibility.setPosition( testMovingTilePosition );

				final Map< Integer, TileInfo > tilesToCompare = new HashMap<>();
				tilesToCompare.put( fixedTile.getIndex(), fixedTile );
				tilesToCompare.put( movingTilePossibility.getIndex(), movingTilePossibility );

				final Boundaries overlapPossibility = TileOperations.getOverlappingRegion(
						tilesToCompare.get( pair[ j ].getIndex() ),
						tilesToCompare.get( pair[ ( j + 1 ) % pair.length ].getIndex() ) );

				if ( overlapPossibility != null )
				{
					for ( int d = 0; d < searchRadiusBoundingBox.numDimensions(); ++d )
					{
						overlappingRegionMin[ d ] = Math.min( overlapPossibility.min( d ), overlappingRegionMin[ d ] );
						overlappingRegionMax[ d ] = Math.max( overlapPossibility.max( d ), overlappingRegionMax[ d ] );
					}
				}
			}

			for ( int d = 0; d < searchRadiusBoundingBox.numDimensions(); ++d )
			{
				if ( overlappingRegionMin[ d ] == Long.MAX_VALUE || overlappingRegionMax[ d ] == Long.MIN_VALUE )
				{
					System.out.println();
					System.out.println( tilePair + ": cannot find a non-empty overlap that covers the confidence range (The confidence range says there is no overlap?)" );
					System.out.println();
					return null;
				}
			}

			overlaps[ j ] = new Boundaries( overlappingRegionMin, overlappingRegionMax );

			for ( int d = 0; d < overlaps[ j ].numDimensions(); ++d )
			{
				if ( overlaps[ j ].dimension( d ) <= 1 )
				{
					System.out.println();
					System.out.println( tilePair + ": overlap is 1px in dimension " + d + ", skip this pair" );
					System.out.println();
					return null;
				}
			}
		}

		return new ValuePair<>( overlaps[ 0 ], overlaps[ 1 ] );
	}
}
