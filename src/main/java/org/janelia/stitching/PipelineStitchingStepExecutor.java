package org.janelia.stitching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;
import org.janelia.flatfield.FlatfieldCorrection;
import org.janelia.stitching.StitchingArguments.RestitchingMode;
import org.janelia.stitching.analysis.FilterAdjacentShifts;
import org.janelia.util.Conversions;
import org.janelia.util.ImageImporter;
import org.janelia.util.concurrent.SameThreadExecutorService;

import ij.ImagePlus;
import mpicbg.models.Point;
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
		final int[] imagesMissing = new int[ job.getChannels() ];
		for ( int channel = 0; channel < job.getChannels(); ++channel )
			for ( final TileInfo tile : job.getTiles( channel ) )
				if ( !Files.exists( Paths.get( tile.getFilePath() ) ) )
					++imagesMissing[ channel ];
		for ( int channel = 0; channel < job.getChannels(); ++channel )
			System.out.println( imagesMissing[ channel ] + " images out of " + job.getTiles( channel ).length + " are missing in channel " + channel );

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
				final String basePath = Paths.get( job.getArgs().inputTileConfigurations().get( 0 ) ).getParent().toString();
				final String filename = Paths.get( job.getArgs().inputTileConfigurations().get( 0 ) ).getFileName().toString();
				final String iterationDirname = "iter" + iteration;
				final String stitchedTilesFilepath = Paths.get( basePath, iterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) ).toString();

				if ( !Files.exists( Paths.get( stitchedTilesFilepath ) ) )
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
				final TileInfo[] stitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( stitchedTilesFilepath );

				// TODO: test and keep if works or remove (currently generates worse solutions)
				// find new pairs using new solution for predicting positions of the excluded (missing) tiles
//				final List< TilePair > newPairs = FindPairwiseChanges.getPairsWithPrediction( stageTiles, stitchedTiles, job.getArgs().minStatsNeighborhood(), !job.getArgs().useAllPairs() );
//				overlappingTiles.clear();
//				overlappingTiles.addAll( newPairs );

				// FIXME: don't stop after the first iteration for now in order to restitch the dataset initializing it with translation-based solution
				if ( iteration == 0 )
				{
					// stop if all input tiles are included in the stitched set
					/*if ( stageTiles.length == stitchedTiles.length )
					{
						System.out.println( "Stopping on iteration " + iteration + ": all input tiles (n=" + stageTiles.length + ") are included in the stitched set" );
						copyFinalSolution( iteration );
						break;
					}*/
				}
				else
				{
					final String previousIterationDirname = iteration == 0 ? null : "iter" + ( iteration - 1 );

					final String previousStitchedTilesFilepath = Paths.get( basePath, previousIterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) ).toString();
					final TileInfo[] previousStitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( previousStitchedTilesFilepath );

					final String usedPairsFilepath = Paths.get( basePath, iterationDirname, "pairwise-used.json" ).toString();
					final String previousUsedPairsFilepath = Paths.get( basePath, previousIterationDirname, "pairwise-used.json" ).toString();
					final List< SerializablePairWiseStitchingResult[] > usedPairs = TileInfoJSONProvider.loadPairwiseShiftsMulti( usedPairsFilepath );
					final List< SerializablePairWiseStitchingResult[] > previousUsedPairs = TileInfoJSONProvider.loadPairwiseShiftsMulti( previousUsedPairsFilepath );

					if ( stitchedTiles.length < previousStitchedTiles.length || ( stitchedTiles.length == previousStitchedTiles.length && usedPairs.size() <= previousUsedPairs.size() ) )
					{
						// mark the last solution as not used because it is worse than from the previous iteration
						Files.move( Paths.get( basePath, iterationDirname ), Paths.get( basePath, Utils.addFilenameSuffix( iterationDirname, "-notused" ) ) );
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
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final String basePath = Paths.get( job.getArgs().inputTileConfigurations().get( channel ) ).getParent().toString();
			final String filename = Paths.get( job.getArgs().inputTileConfigurations().get( channel ) ).getFileName().toString();
			final String iterationDirname = "iter" + fromIteration;
			final String stitchedTilesFilepath = Paths.get( basePath, iterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) ).toString();
			final String finalTilesFilepath = Paths.get( basePath, Utils.addFilenameSuffix( filename, "-final" ) ).toString();
			Files.copy( Paths.get( stitchedTilesFilepath ), Paths.get( finalTilesFilepath ) );

			if ( channel == 0 )
				Files.copy( Paths.get( basePath, iterationDirname, "optimizer.txt" ), Paths.get( basePath, "optimizer-final.txt" ) );
		}
	}

	private void preparePairwiseShiftsMulti( final List< TilePair > overlappingTiles, final int iteration ) throws PipelineExecutionException, IOException
	{
		final String basePath = Paths.get( job.getArgs().inputTileConfigurations().get( 0 ) ).getParent().toString();
		final String iterationDirname = "iter" + iteration;
		final String previousIterationDirname = iteration == 0 ? null : "iter" + ( iteration - 1 );
		final String pairwiseFilename = "pairwise.json";

		Paths.get( basePath, iterationDirname ).toFile().mkdirs();

		final String pairwisePath = Paths.get( basePath, iterationDirname, pairwiseFilename ).toString();

		if ( iteration == 0 )
		{
			// use the pairwise file from the previous run in the old mode if exists
			if ( Files.exists( Paths.get( basePath, pairwiseFilename ) ) )
				Files.move( Paths.get( basePath, pairwiseFilename ), Paths.get( pairwisePath ) );
		}
		else
		{
			if ( job.getArgs().restitchingMode() == RestitchingMode.INCREMENTAL )
			{
				System.out.println( "Restitching only excluded pairs" );
				// use pairwise-used from the previous iteration, so they will not be restitched
				if ( !Files.exists( Paths.get( pairwisePath ) ) )
					Files.copy( Paths.get( basePath, previousIterationDirname, Utils.addFilenameSuffix( pairwiseFilename, "-used" ) ), Paths.get( pairwisePath ) );
			}
			else
			{
				System.out.println( "Full restitching" );
			}
		}

		// Try to load precalculated shifts for some pairs of tiles
		final List< SerializablePairWiseStitchingResult[] > pairwiseShiftsMulti = new ArrayList<>();
		try
		{
			System.out.println( "try to load pairwise results from disk" );
			pairwiseShiftsMulti.addAll( TileInfoJSONProvider.loadPairwiseShiftsMulti( pairwisePath ) );
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
				TileInfoJSONProvider.savePairwiseShiftsMulti( pairwiseShiftsMulti, pairwisePath );
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
			final String statsTileConfigurationPath = iteration == 0 ? null : Paths.get(
					basePath,
					previousIterationDirname,
					Utils.addFilenameSuffix(
							Paths.get( job.getArgs().inputTileConfigurations().get( 0 ) ).getFileName().toString(),
							"-stitched"
						)
				).toString();

			// Initiate the computation
			final List< SerializablePairWiseStitchingResult[] > adjacentShiftsMulti = computePairwiseShifts( pendingOverlappingTiles, statsTileConfigurationPath );
			pairwiseShiftsMulti.addAll( adjacentShiftsMulti );

			try {
				System.out.println( "Stitched all tiles pairwise, store this information on disk.." );
				TileInfoJSONProvider.savePairwiseShiftsMulti( pairwiseShiftsMulti, pairwisePath );
			} catch ( final IOException e ) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Computes the best possible pairwise shifts between every pair of tiles on a Spark cluster.
	 * It uses phase correlation for measuring similarity between two images.
	 */
	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > List< SerializablePairWiseStitchingResult[] > computePairwiseShifts( final List< TilePair > overlappingTiles, final String statsTileConfigurationPath ) throws PipelineExecutionException
	{
		// try to load the stitched tile configuration from the previous iteration
		final TileSearchRadiusEstimator searchRadiusEstimator;
		if ( statsTileConfigurationPath != null )
		{
			System.out.println( "=== Building prediction model based on previous stitching solution ===" );
			try
			{
				final TileInfo[] statsTiles = TileInfoJSONProvider.loadTilesConfiguration( statsTileConfigurationPath );
				System.out.println( "-- Creating search radius estimator using " + job.getTiles( 0 ).length + " stage tiles and " + statsTiles.length + " stitched tiles --" );
				searchRadiusEstimator = new TileSearchRadiusEstimator( job.getTiles( 0 ), statsTiles, job.getArgs().searchRadiusMultiplier() );
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

		// for splitting the overlap area into 1x1 or 2x2, etc. which leads to 1 or 4 matches per pair of tiles
		final int splitOverlapParts = job.getArgs().splitOverlapParts();

		System.out.println( "Broadcasting flatfield correction images" );
		final List< RandomAccessiblePairNullable< U, U > > flatfieldCorrectionForChannels = new ArrayList<>();
		for ( final String channelPath : job.getArgs().inputTileConfigurations() )
		{
			final String channelPathNoExt = channelPath.lastIndexOf( '.' ) != -1 ? channelPath.substring( 0, channelPath.lastIndexOf( '.' ) ) : channelPath;
			// use it as a folder with the input file's name
			flatfieldCorrectionForChannels.add(
					FlatfieldCorrection.loadCorrectionImages(
							channelPathNoExt + "-flatfield/S.tif",
							channelPathNoExt + "-flatfield/T.tif",
							job.getDimensionality()
						)
				);
		}
		final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedFlatfieldCorrectionForChannels = sparkContext.broadcast( flatfieldCorrectionForChannels );

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
		final Broadcast< List< Map< String, TileInfo > > > broadcastedCoordsToTilesChannels = sparkContext.broadcast( coordsToTilesChannels );

		System.out.println( "Processing " + overlappingTiles.size() + " pairs..." );

		final LongAccumulator notEnoughNeighborsWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noOverlapWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noPeaksWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();

		final boolean isLocalSparkContext = sparkContext.isLocal();

		final JavaRDD< TilePair > rdd = sparkContext.parallelize( overlappingTiles, overlappingTiles.size() );
		final JavaRDD< SerializablePairWiseStitchingResult[] > pairwiseStitching = rdd.map( tilePair ->
			{
				// stats
				final TileSearchRadiusEstimator localSearchRadiusEstimator = broadcastedSearchRadiusEstimator.value();
				final SearchRadius searchRadius;

				final TileInfo[] tilePairArr = tilePair.toArray();
				final Interval[] overlaps = new Boundaries[ tilePairArr.length ];
				final ImagePlus[] imps = new ImagePlus[ tilePairArr.length ];

				final TileInfo fixedTile = tilePairArr[ 0 ], movingTile = tilePairArr[ 1 ];

				System.out.println( "Processing tile pair " + tilePair );

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
					final SearchRadius[] tilesSearchRadius = new SearchRadius[ tilePairArr.length ];
					for ( int j = 0; j < tilePairArr.length; j++ )
					{
						tilesSearchRadius[ j ] = localSearchRadiusEstimator.getSearchRadiusTreeWithinEstimationWindow( tilePairArr[ j ] );
						if ( tilesSearchRadius[ j ].getUsedPointsIndexes().size() < minNumNearestNeighbors )
						{
							notEnoughNeighborsWithinConfidenceIntervalPairsCount.add( 1 );

//								System.out.println( "Found " + searchRadiusEstimationWindow.getUsedPointsIndexes().size() + " neighbors within the search window but we require " + numNearestNeighbors + " nearest neighbors, perform a K-nearest neighbor search instead..." );
							System.out.println();
							System.out.println( tilePair + ": found " + tilesSearchRadius[ j ].getUsedPointsIndexes().size() + " neighbors within the search window of the " + ( j == 0 ? "fixed" : "moving" ) + " tile but we require at least " + minNumNearestNeighbors + " nearest neighbors, so ignore this tile pair for now" );
							System.out.println();

							final SerializablePairWiseStitchingResult[] invalidResult = new SerializablePairWiseStitchingResult[ splitOverlapParts ];
							for ( int i = 0; i < invalidResult.length; ++i )
							{
								invalidResult[ i ] = new SerializablePairWiseStitchingResult( tilePair, null, 0 );
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
							System.out.println( tilePair + ": found " + tilesSearchRadius[ j ].getUsedPointsIndexes().size() + " neighbors within the search window for the " + ( j == 0 ? "fixed" : "moving" ) + " tile, estimate search radius based on that" );
						}
					}

					System.out.println();
					System.out.println( tilePair + ": found search radiuses for both tiles in the pair, get a combined search radius for the moving tile" );
					System.out.println();

					searchRadius = localSearchRadiusEstimator.getCombinedCovariancesSearchRadius( tilesSearchRadius[ 0 ], tilesSearchRadius[ 1 ] );

					final Interval boundingBox = Intervals.smallestContainingInterval( searchRadius.getBoundingBox() );
					System.out.println( String.format( tilePair + ": estimated combined search radius for the moving tile. Bounding box: min=%s, max=%s, size=%s",
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
					final Boundaries testOverlap = TileOperations.getOverlappingRegion( tilePairArr[ 0 ], tilePairArr[ 1 ] );
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
					for ( int j = 0; j < tilePairArr.length; j++ )
					{
						System.out.println( "Prepairing #" + (j+1) + " of a pair,  padding ROI by " + Arrays.toString( job.getArgs().padding() ) + "(padding arg)" );
						final Boundaries overlap = TileOperations.getOverlappingRegion( tilePairArr[ j ], tilePairArr[ ( j + 1 ) % tilePairArr.length ] );
						overlaps[ j ] = TileOperations.padInterval(
								overlap,
								new FinalDimensions( tilePairArr[ j ].getSize() ),
								job.getArgs().padding()
							);
					}
				}
				else
				{
					final Pair< Interval, Interval > overlapsAdjustedToSearchRadius = adjustOverlappingRegion( tilePair, searchRadius );
					if ( overlapsAdjustedToSearchRadius == null )
					{
						noOverlapWithinConfidenceIntervalPairsCount.add( 1 );
						System.out.println( tilePair + ": cannot find a non-empty overlap that covers the confidence range (The confidence range says there is no overlap?)" );

						final SerializablePairWiseStitchingResult[] invalidResult = new SerializablePairWiseStitchingResult[ splitOverlapParts ];
						for ( int i = 0; i < invalidResult.length; ++i )
						{
							invalidResult[ i ] = new SerializablePairWiseStitchingResult( tilePair, null, 0 );
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
					System.out.println( tilePair + ": overlap is <= 1px" );

					final SerializablePairWiseStitchingResult[] invalidResult = new SerializablePairWiseStitchingResult[ splitOverlapParts ];
					for ( int i = 0; i < invalidResult.length; ++i )
					{
						invalidResult[ i ] = new SerializablePairWiseStitchingResult( tilePair, null, 0 );
						invalidResult[ i ].setIsValidOverlap( false );
					}
					return invalidResult;
				}

				// prepare images
				for ( int j = 0; j < tilePairArr.length; j++ )
				{
					System.out.println( "Averaging corresponding tile images for " + job.getChannels() + " channels" );
//					final ComparableTuple< Integer > coordinates = new ComparableTuple<>( Conversions.toBoxedArray( Utils.getTileCoordinates( pair[ j ] ) ) );
					final String coordsStr = Utils.getTileCoordinatesString( tilePairArr[ j ] );
					int channelsUsed = 0;
					final ImagePlusImg< FloatType, ? > dst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( overlaps[ j ] ) );
					for ( int channel = 0; channel < job.getChannels(); channel++ )
					{
						final TileInfo tileInfo = broadcastedCoordsToTilesChannels.value().get( channel ).get( coordsStr );
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
							throw new PipelineExecutionException( tilePair + ": cannot find corresponding tile for this channel" );

						if ( tilePairArr[ j ].getIndex().intValue() != tileInfo.getIndex().intValue() )
							throw new PipelineExecutionException( tilePair + ": different indexes for the same stage position " + Utils.getTileCoordinatesString( tileInfo ) );

						// FIXME: throw exception in case some image files are missing (or, check for missing files beforehand)
						final ImagePlus imp = ImageImporter.openImage( tileInfo.getFilePath() );
						if ( imp == null )
							throw new PipelineExecutionException( "Image file does not exist: " + tileInfo.getFilePath() );
//						if ( imp != null )
						{
							// warn if image type and/or size do not match metadata
							if ( !ImageType.valueOf( imp.getType() ).equals( tileInfo.getType() ) )
								throw new PipelineExecutionException( String.format( "Image type %s does not match the value from metadata %s", ImageType.valueOf( imp.getType() ), tileInfo.getType() ) );
							if ( !Arrays.equals( Conversions.toLongArray( Utils.getImagePlusDimensions( imp ) ), tileInfo.getSize() ) )
								throw new PipelineExecutionException( String.format( "Image size %s does not match the value from metadata %s", Arrays.toString( Utils.getImagePlusDimensions( imp ) ), Arrays.toString( tileInfo.getSize() ) ) );

							final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );
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

							imp.close();
							++channelsUsed;
						}
					}

					if ( channelsUsed == 0 )
						throw new PipelineExecutionException( tilePair + ": images are missing in all channels" );

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
				Arrays.fill( roiPartsCount, splitOverlapParts );
				roiPartsCount[ shortEdgeDimension ] = 1;
				final List< TileInfo > roiParts = TileOperations.divideSpaceByCount( fullRoi, roiPartsCount );

				System.out.println( String.format( "Splitting the overlap in %d subintervals with grid of %s..", roiParts.size(), Arrays.toString( roiPartsCount ) ) );

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

//					if ( isLocalSparkContext )
//						PairwiseStitchingPerformer.setThreads( Runtime.getRuntime().availableProcessors() );
//					else
						PairwiseStitchingPerformer.setThreads( 1 );

					// for transforming 'overlap offset' to 'tile offset'
					final long[][] roiToTileOffset = new long[ 2 ][];
					for ( int i = 0; i < 2; ++i )
					{
						roiToTileOffset[ i ] = new long[ roiPartInterval.numDimensions() ];
						for ( int d = 0; d < roiToTileOffset[ i ].length; ++d )
							roiToTileOffset[ i ][ d ] = (i==0?1:-1) * ( overlaps[ i ].min( d ) + roiPartInterval.min( d ) );
					}

					// for transforming 'tile offset' to 'global offset'
					final double[] globalOffset = new double[ fixedTile.numDimensions() ];
					for ( int d = 0; d < globalOffset.length; ++d )
						globalOffset[ d ] = fixedTile.getPosition( d );

					// 'stage offset' of the moving tile relative to the fixed tile
					final double[] stageOffset = new double[ roiPartInterval.numDimensions() ];
					for ( int d = 0; d < stageOffset.length; ++d )
						stageOffset[ d ] = movingTile.getPosition( d ) - fixedTile.getPosition( d );

					final SerializablePairWiseStitchingResult[] results = PairwiseStitchingPerformer.stitchPairwise(
							roiPartImps[0], roiPartImps[1], null, null, null, null, timepoint, timepoint, job.getParams(), 1,
							searchRadius, roiToTileOffset, globalOffset, stageOffset,
							null, null // TODO: remove these unused parameters
						);

					final SerializablePairWiseStitchingResult result = results[ 0 ];

					if ( result == null )
					{
						noPeaksWithinConfidenceIntervalPairsCount.add( 1 );
						System.out.println( tilePair + ": no peaks found within the confidence interval" );

						final SerializablePairWiseStitchingResult invalidResult = new SerializablePairWiseStitchingResult( tilePair, null, 0 );
						invalidResult.setIsValidOverlap( false );
						roiPartsResults[ roiPartIndex ] = invalidResult;
					}
					else
					{
						// compute new tile offset
						for ( int i = 0; i < 2; ++i )
							for ( int d = 0; d < roiPartInterval.numDimensions(); ++d )
								result.getOffset()[ d ] += roiToTileOffset[ i ][ d ];

						// create point pair using center point of each ROI
						final Point fixedTilePoint, movingTilePoint;
						{
							final double[] fixedTileRoiCenter = new double[ roiPartsCount.length ], movingTileRoiCenter = new double[ roiPartsCount.length ];;
							for ( int d = 0; d < fixedTileRoiCenter.length; ++d )
							{
								fixedTileRoiCenter[ d ] = roiToTileOffset[ 0 ][ d ] + roiPartInterval.dimension( d ) / 2;
								movingTileRoiCenter[ d ] = fixedTileRoiCenter[ d ] - result.getOffset( d );
							}
							fixedTilePoint = new Point( fixedTileRoiCenter );
							movingTilePoint = new Point( movingTileRoiCenter );
						}
						final PointPair pointPair = new PointPair( fixedTilePoint, movingTilePoint );

						result.setTilePair( tilePair );
						result.setPointPair( pointPair );
						result.setVariance( variance );

						roiPartsResults[ roiPartIndex ] = result;
					}

					for ( int i = 0; i < 2; i++ )
						roiPartImps[ i ].close();
				}

				for ( int i = 0; i < 2; i++ )
					imps[ i ].close();

				System.out.println( "Stitched tile pair " + tilePair + ", got " + roiPartsResults.length + " matches" );
				return roiPartsResults;
			} );

		final List< SerializablePairWiseStitchingResult[] > stitchingResults = pairwiseStitching.collect();

		broadcastedFlatfieldCorrectionForChannels.destroy();
		broadcastedSearchRadiusEstimator.destroy();
		broadcastedCoordsToTilesChannels.destroy();

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
