package org.janelia.stitching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
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
import mpicbg.imglib.custom.OffsetConverter;
import mpicbg.imglib.custom.PointValidator;
import mpicbg.models.Point;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.exception.ImgLibException;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;
import net.imglib2.util.IntervalsNullable;
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
	private static class StitchingResult implements Serializable
	{
		private static final long serialVersionUID = -533794988639089455L;

		public final SerializablePairWiseStitchingResult shift;
		public final double[] searchRadiusLength;

		public StitchingResult( final SerializablePairWiseStitchingResult shift, final double[] searchRadiusLength )
		{
			this.shift = shift;
			this.searchRadiusLength = searchRadiusLength;
		}
	}

	private static final long serialVersionUID = -7152174064553332061L;

	public PipelineStitchingStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run() throws PipelineExecutionException
	{
		checkForMissingImages();

		// split tiles into 8 boxes
		final int[] tileBoxesGridSize = new int[ job.getDimensionality() ];
		Arrays.fill( tileBoxesGridSize, job.getArgs().splitOverlapParts() );
		final List< TileInfo > tileBoxes = splitTilesIntoBoxes( job.getTiles( 0 ), tileBoxesGridSize );

		// find pairs of tile boxes that overlap by more than 50% when transformed into relative coordinate space of the fixed original tile
		final List< TilePair > overlappingBoxes = findOverlappingTileBoxes( tileBoxes );
		System.out.println( "Overlapping box pairs count = " + overlappingBoxes.size() );

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
					preparePairwiseShifts( overlappingBoxes, iteration );
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
					final List< SerializablePairWiseStitchingResult > usedPairs = TileInfoJSONProvider.loadPairwiseShifts( usedPairsFilepath );
					final List< SerializablePairWiseStitchingResult > previousUsedPairs = TileInfoJSONProvider.loadPairwiseShifts( previousUsedPairsFilepath );

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

	/**
	 * Splits each tile into grid of smaller boxes, and for each box stores index mapping to the original tile.
	 *
	 * @param tiles
	 * @return
	 */
	private List< TileInfo > splitTilesIntoBoxes( final TileInfo[] tiles, final int[] gridSize )
	{
		final List< TileInfo > tileSplitBoxes = new ArrayList<>();
		for ( final TileInfo tile : tiles )
		{
			final Interval zeroMinTileInterval = new FinalInterval( tile.getSize() );
			final List< TileInfo > splitTile = TileOperations.divideSpaceByCount( zeroMinTileInterval, gridSize );
			for ( final TileInfo box : splitTile )
			{
				box.setOriginalTile( tile );
				box.setIndex( tileSplitBoxes.size() );
				tileSplitBoxes.add( box );
			}
		}
		return tileSplitBoxes;
	}

	/**
	 * Returns list of tile box pairs that are adjacent (overlap by more than 50%) in transformed space.
	 *
	 * @param tileBoxes
	 * @return
	 */
	private List< TilePair > findOverlappingTileBoxes( final List< TileInfo > tileBoxes )
	{
		final List< TilePair > overlappingBoxes = new ArrayList<>();
		for ( int i = 0; i < tileBoxes.size(); i++ )
		{
			for ( int j = i + 1; j < tileBoxes.size(); j++ )
			{
				if ( tileBoxes.get( i ).getOriginalTile().getIndex().intValue() != tileBoxes.get( j ).getOriginalTile().getIndex().intValue() )
				{
					final TilePair tileBoxPair = new TilePair( tileBoxes.get( i ), tileBoxes.get( j ) );
					final Interval fixedTileBoxInterval = tileBoxPair.getA().getBoundaries();
					final Interval movingInFixedTileBoxInterval = transformMovingTileBox( tileBoxPair );
					final Interval tileBoxesOverlap = IntervalsNullable.intersect( fixedTileBoxInterval, movingInFixedTileBoxInterval );
					if ( FilterAdjacentShifts.isAdjacent( getMinTileDimensions( tileBoxPair ), tileBoxesOverlap ) )
						overlappingBoxes.add( tileBoxPair );
				}
			}
		}
		return overlappingBoxes;
	}

	/**
	 * Returns an interval of the moving tile box being transformed into coordinate space of the fixed original tile.
	 * @param tileBoxPair
	 * @return
	 */
	private Interval transformMovingTileBox( final TilePair tileBoxPair )
	{
		final TileInfo fixedTileBox = tileBoxPair.getA(), movingTileBox = tileBoxPair.getB();
		final double[] movingMiddlePoint = getTileBoxMiddlePoint( movingTileBox );
		final double[] movingInFixedMiddlePoint = new double[ movingMiddlePoint.length ];
		final AffineTransform3D movingToFixed = new AffineTransform3D();
		movingToFixed.preConcatenate( movingTileBox.getOriginalTile().getTransform() ).preConcatenate( fixedTileBox.getOriginalTile().getTransform().inverse() );
		movingToFixed.apply( movingMiddlePoint, movingInFixedMiddlePoint );
		final RealInterval movingInFixedTileBoxRealInterval = getTileBoxInterval( movingInFixedMiddlePoint, movingTileBox.getSize() );
		return TileOperations.roundRealInterval( movingInFixedTileBoxRealInterval );
	}

	/**
	 * Returns middle point in a given tile box.
	 *
	 * @param tileBox
	 * @return
	 */
	private double[] getTileBoxMiddlePoint( final TileInfo tileBox )
	{
		final double[] middlePoint = new double[ tileBox.numDimensions() ];
		for ( int d = 0; d < middlePoint.length; ++d )
			middlePoint[ d ] = tileBox.getPosition( d ) + 0.5 * tileBox.getSize( d );
		return middlePoint;
	}

	/**
	 * Returns an interval of a given tile box with specified middle point.
	 *
	 * @param middlePoint
	 * @param boxSize
	 * @return
	 */
	private RealInterval getTileBoxInterval( final double[] middlePoint, final long[] boxSize )
	{
		final double[] min = new double[ middlePoint.length ], max = new double[ middlePoint.length ];
		for ( int d = 0; d < middlePoint.length; ++d )
		{
			min[ d ] = middlePoint[ d ] - 0.5 * boxSize[ d ];
			max[ d ] = middlePoint[ d ] + 0.5 * boxSize[ d ];
		}
		return new FinalRealInterval( min, max );
	}

	private Dimensions getMinTileDimensions( final TilePair pair )
	{
		final long[] minDimensions = new long[ Math.max( pair.getA().numDimensions(), pair.getB().numDimensions() ) ];
		for ( int d = 0; d < minDimensions.length; ++d )
			minDimensions[ d ] = Math.min( pair.getA().getSize( d ), pair.getB().getSize( d ) );
		return new FinalDimensions( minDimensions );
	}

	/**
	 * Warns if some of tile images can't be found.
	 *
	 * @throws PipelineExecutionException
	 */
	private void checkForMissingImages() throws PipelineExecutionException
	{
		final int[] imagesMissing = new int[ job.getChannels() ];
		for ( int channel = 0; channel < job.getChannels(); ++channel )
			for ( final TileInfo tile : job.getTiles( channel ) )
				if ( !Files.exists( Paths.get( tile.getFilePath() ) ) )
					++imagesMissing[ channel ];
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			System.out.println( imagesMissing[ channel ] + " images out of " + job.getTiles( channel ).length + " are missing in channel " + channel );
			if ( imagesMissing[ channel ] != 0 )
				throw new PipelineExecutionException( "missing images in ch" + channel );
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

	/**
	 * Initiates the computation for pairs that haven't been precomputed. Stores the pairwise file for this iteration of stitching.
	 *
	 * @param overlappingTiles
	 * @param iteration
	 * @throws PipelineExecutionException
	 * @throws IOException
	 */
	private void preparePairwiseShifts( final List< TilePair > overlappingTiles, final int iteration ) throws PipelineExecutionException, IOException
	{
		final String basePath = Paths.get( job.getArgs().inputTileConfigurations().get( 0 ) ).getParent().toString();
		final String iterationDirname = "iter" + iteration;
		final String previousIterationDirname = iteration == 0 ? null : "iter" + ( iteration - 1 );
		final String pairwiseFilename = "pairwise.json";
		Paths.get( basePath, iterationDirname ).toFile().mkdirs();
		final String pairwisePath = Paths.get( basePath, iterationDirname, pairwiseFilename ).toString();

		final List< SerializablePairWiseStitchingResult > pairwiseShifts = tryLoadPrecomputedShifts( basePath, iteration );
		final List< TilePair > pendingOverlappingTiles = removePrecomputedPendingPairs( pairwisePath, overlappingTiles, pairwiseShifts );

		if ( pendingOverlappingTiles.isEmpty() && !pairwiseShifts.isEmpty() )
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
			final List< StitchingResult > stitchingResults = computePairwiseShifts( pendingOverlappingTiles, statsTileConfigurationPath );

			// merge results with preloaded pairwise shifts
			for ( final StitchingResult result : stitchingResults )
				pairwiseShifts.add( result.shift );

//			saveSearchRadiusStats( stitchingResults, Paths.get( basePath, iterationDirname, "searchRadiusStats.txt" ).toString() );

			try {
				System.out.println( "Stitched all tiles pairwise, store this information on disk.." );
				TileInfoJSONProvider.savePairwiseShifts( pairwiseShifts, pairwisePath );
			} catch ( final IOException e ) {
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
		final String iterationDirname = "iter" + iteration;
		final String previousIterationDirname = iteration == 0 ? null : "iter" + ( iteration - 1 );
		final String pairwiseFilename = "pairwise.json";
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
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = new ArrayList<>();
		try
		{
			System.out.println( "try to load pairwise results from disk" );
			pairwiseShifts.addAll( TileInfoJSONProvider.loadPairwiseShifts( pairwisePath ) );
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
	private List< TilePair > removePrecomputedPendingPairs(
			final String pairwisePath,
			final List< TilePair > overlappingTiles,
			final List< SerializablePairWiseStitchingResult > pairwiseShifts ) throws PipelineExecutionException, IOException
	{
		// remove redundant pairs (that are not contained in the given overlappingTiles list)
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
		for ( final Iterator< SerializablePairWiseStitchingResult > it = pairwiseShifts.iterator(); it.hasNext(); )
		{
			final SerializablePairWiseStitchingResult result = it.next();
			final Integer[] indexes = new Integer[ 2 ];
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
			TileInfoJSONProvider.savePairwiseShifts( pairwiseShifts, pairwisePath );

		// find only pairs that need to be computed
		final List< TilePair > pendingOverlappingTiles = new ArrayList<>();

		// Create a cache to efficiently lookup the existing pairs of tiles loaded from disk
		final Map< Integer, Set< Integer > > cache = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult result : pairwiseShifts )
		{
			final Integer[] indexes = new Integer[ 2 ];
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

		return pendingOverlappingTiles;
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
		try ( final PrintWriter writer = new PrintWriter( searchRadiusStatsPath ) )
		{
			writer.println( "Tile1_index Tile1_grid_X Tile1_grid_Y Tile1_grid_Z Tile1_timestamp Tile2_index Tile2_grid_X Tile2_grid_Y Tile2_grid_Z Tile2_timestamp Radius_1st Radius_2nd Radius_3rd" );
			for ( final StitchingResult result : stitchingResults )
				writer.println(
						String.format(
								""
								+ "%d %d %d %d %d"
								+ " "
								+ "%d %d %d %d %d"
								+ " "
								+ "%.2f %.2f %.2f",

								result.shift.getTilePair().getA().getIndex(),
								Utils.getTileCoordinates( result.shift.getTilePair().getA() )[ 0 ],
								Utils.getTileCoordinates( result.shift.getTilePair().getA() )[ 1 ],
								Utils.getTileCoordinates( result.shift.getTilePair().getA() )[ 2 ],
								Utils.getTileTimestamp( result.shift.getTilePair().getA() ),

								result.shift.getTilePair().getB().getIndex(),
								Utils.getTileCoordinates( result.shift.getTilePair().getB() )[ 0 ],
								Utils.getTileCoordinates( result.shift.getTilePair().getB() )[ 1 ],
								Utils.getTileCoordinates( result.shift.getTilePair().getB() )[ 2 ],
								Utils.getTileTimestamp( result.shift.getTilePair().getB() ),

								result.searchRadiusLength != null ? result.searchRadiusLength[ 0 ] : -1,
								result.searchRadiusLength != null ? result.searchRadiusLength[ 1 ] : -1,
								result.searchRadiusLength != null ? result.searchRadiusLength[ 2 ] : -1
							)
					);
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
			final List< TilePair > overlappingBoxes,
			final String statsTileConfigurationPath ) throws PipelineExecutionException
	{
		final Broadcast< TileSearchRadiusEstimator > broadcastedSearchRadiusEstimator = sparkContext.broadcast( loadSearchRadiusEstimator( statsTileConfigurationPath ) );
		final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedFlatfieldCorrectionForChannels = sparkContext.broadcast( loadFlatfieldChannels() );
		final Broadcast< List< Map< String, TileInfo > > > broadcastedCoordsToTilesChannels = sparkContext.broadcast( getCoordsToTilesChannels() );

		System.out.println( "Processing " + overlappingBoxes.size() + " pairs..." );

		final LongAccumulator notEnoughNeighborsWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noOverlapWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noPeaksWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();

		final JavaRDD< TilePair > rdd = sparkContext.parallelize( overlappingBoxes, overlappingBoxes.size() );
		final JavaRDD< StitchingResult > pairwiseStitching = rdd.map( tileBoxPair ->
			{
				System.out.println( "Processing tile box pair " + tileBoxPair + " of tiles " + new TilePair( tileBoxPair.getA().getOriginalTile(), tileBoxPair.getB().getOriginalTile() ) );

				final Interval movingBoxInFixedSpace = transformMovingTileBox( tileBoxPair );

				// stats
				final TileSearchRadiusEstimator localSearchRadiusEstimator = broadcastedSearchRadiusEstimator.value();
				final SearchRadius searchRadius;
				if ( localSearchRadiusEstimator == null )
				{
					// FIXME: for testing purposes
//					searchRadius = null;

					// mean offset is the top-left coordinate of the moving box in the fixed space
					final double[] offsetsMeanValues = Intervals.minAsDoubleArray( movingBoxInFixedSpace );
					final double[][] offsetsCovarianceMatrix = new double[][] { new double[] { 1, 0, 0 }, new double[] { 0, 1, 0 }, new double[] { 0, 0, 1 } };
					final double sphereRadiusPixels = job.getArgs().searchRadiusMultiplier();
					searchRadius = new SearchRadius( sphereRadiusPixels, offsetsMeanValues, offsetsCovarianceMatrix );
				}
				else
				{
					// FIXME: for testing purposes
					throw new RuntimeException( "shouldn't happen" );
//					searchRadius = estimateSearchRadius( localSearchRadiusEstimator, tilePair );
//					if ( searchRadius == null )
//						return null;
				}

				// get ROIs in corresponding images
				final Interval[] overlapsInOriginalTileSpace = getOverlapIntervals( tileBoxPair, searchRadius );

				// prepare roi images
				final ImagePlus[] roiImps = prepareRoiImages(
						tileBoxPair,
						overlapsInOriginalTileSpace,
						broadcastedCoordsToTilesChannels.value(),
						broadcastedFlatfieldCorrectionForChannels.value()
					);

				// get required offsets for roi parts
				final OffsetConverter offsetConverter = getOffsetConverter( tileBoxPair, overlapsInOriginalTileSpace );

				final SerializablePairWiseStitchingResult pairwiseResult = stitchPairwise( tileBoxPair, roiImps, searchRadius, offsetConverter );

				final StitchingResult stitchingResult;
				if ( pairwiseResult == null )
				{
					// no matches were found
					stitchingResult = new StitchingResult( null, searchRadius != null ? searchRadius.getEllipseRadius() : null );
				}
				else
				{
					// put other properties and store the result

					stitchingResult = new StitchingResult( pairwiseResult, searchRadius != null ? searchRadius.getEllipseRadius() : null );
				}

				for ( int i = 0; i < 2; i++ )
					roiImps[ i ].close();

				System.out.println( "Stitched tile box pair " + tileBoxPair + " of tiles " + new TilePair( tileBoxPair.getA().getOriginalTile(), tileBoxPair.getB().getOriginalTile() ) );

				return stitchingResult;
			} );

		final List< StitchingResult > stitchingResults = pairwiseStitching.collect();

		broadcastedFlatfieldCorrectionForChannels.destroy();
		broadcastedSearchRadiusEstimator.destroy();
		broadcastedCoordsToTilesChannels.destroy();

		int validPairs = 0;
		for ( final StitchingResult result : stitchingResults )
		{
			final SerializablePairWiseStitchingResult shift = result.shift;
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

	/**
	 * Tries to load flatfield components for all channels.
	 *
	 * @return
	 */
	private < U extends NativeType< U > & RealType< U > > List< RandomAccessiblePairNullable< U, U > > loadFlatfieldChannels()
	{
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
		return searchRadiusEstimator;
	}

	private SearchRadius estimateSearchRadius( final TileSearchRadiusEstimator searchRadiusEstimator, final TilePair tilePair ) throws PipelineExecutionException
	{
		final TileInfo[] tilePairArr = tilePair.toArray();
		final int minNumNearestNeighbors = job.getArgs().minStatsNeighborhood();
		final SearchRadius[] tilesSearchRadius = new SearchRadius[ tilePairArr.length ];
		for ( int j = 0; j < tilePairArr.length; j++ )
		{
			tilesSearchRadius[ j ] = searchRadiusEstimator.getSearchRadiusTreeWithinEstimationWindow( tilePairArr[ j ] );
			if ( tilesSearchRadius[ j ].getUsedPointsIndexes().size() < minNumNearestNeighbors )
			{
				// TODO: pass actions to update accumulators
//				notEnoughNeighborsWithinConfidenceIntervalPairsCount.add( 1 );

//				System.out.println( "Found " + searchRadiusEstimationWindow.getUsedPointsIndexes().size() + " neighbors within the search window but we require " + numNearestNeighbors + " nearest neighbors, perform a K-nearest neighbor search instead..." );
				System.out.println();
				System.out.println( tilePair + ": found " + tilesSearchRadius[ j ].getUsedPointsIndexes().size() + " neighbors within the search window of the " + ( j == 0 ? "fixed" : "moving" ) + " tile but we require at least " + minNumNearestNeighbors + " nearest neighbors, so ignore this tile pair for now" );
				System.out.println();

				// TODO: should we do k-neighbors request instead of skipping this tile pair?
//				final SearchRadius searchRadiusNearestNeighbors = localSearchRadiusEstimator.getSearchRadiusTreeUsingKNearestNeighbors( movingTile, numNearestNeighbors );
//				if ( searchRadiusNearestNeighbors.getUsedPointsIndexes().size() != numNearestNeighbors )
//				{
//					if ( localSearchRadiusEstimator.getNumPoints() >= numNearestNeighbors )
//						throw new PipelineExecutionException( "Required " + numNearestNeighbors + " nearest neighbors, found only " + searchRadiusNearestNeighbors.getUsedPointsIndexes().size() );
//					else if ( searchRadiusNearestNeighbors.getUsedPointsIndexes().size() != localSearchRadiusEstimator.getNumPoints() )
//						throw new PipelineExecutionException( "Number of tiles in the stitched solution = " + localSearchRadiusEstimator.getNumPoints() + ", found " + searchRadiusNearestNeighbors.getUsedPointsIndexes().size() + " neighbors" );
//					else
//						System.out.println( "Got only " + localSearchRadiusEstimator.getNumPoints() + " neighbors as it is the size of the stitched solution" );
//				}
//				searchRadius = searchRadiusNearestNeighbors;
				return null;
			}
			else
			{
				System.out.println( tilePair + ": found " + tilesSearchRadius[ j ].getUsedPointsIndexes().size() + " neighbors within the search window for the " + ( j == 0 ? "fixed" : "moving" ) + " tile, estimate search radius based on that" );
			}
		}

		System.out.println();
		System.out.println( tilePair + ": found search radiuses for both tiles in the pair, get a combined search radius for the moving tile" );
		System.out.println();

		final SearchRadius searchRadius = searchRadiusEstimator.getCombinedCovariancesSearchRadius( tilesSearchRadius[ 0 ], tilesSearchRadius[ 1 ] );

		final Interval boundingBox = Intervals.smallestContainingInterval( searchRadius.getBoundingBox() );
		System.out.println( String.format( tilePair + ": estimated combined search radius for the moving tile. Bounding box: min=%s, max=%s, size=%s",
				Arrays.toString( Intervals.minAsIntArray( boundingBox ) ),
				Arrays.toString( Intervals.maxAsIntArray( boundingBox ) ),
				Arrays.toString( Intervals.dimensionsAsIntArray( boundingBox ) ) ) );

		return searchRadius;
	}

	/**
	 * Returns overlap intervals tile box pair in the coordinate space of each tile (useful for cropping)
	 *
	 * @param tileBoxPair
	 * @param searchRadius
	 * @return
	 */
	private Interval[] getOverlapIntervals( final TilePair tileBoxPair, final SearchRadius searchRadius )
	{
		final Interval[] tileBoxesInFixedSpace = new Interval[] { tileBoxPair.getA().getBoundaries(), transformMovingTileBox( tileBoxPair ) };
		final Interval overlapInFixedSpace = IntervalsNullable.intersect( tileBoxesInFixedSpace[ 0 ], tileBoxesInFixedSpace[ 1 ] );
		if ( overlapInFixedSpace == null )
			throw new RuntimeException( "boxes do not overlap" );

		final long[] originalMovingTileTopLeftCornerInFixedSpace = Intervals.minAsLongArray( IntervalsHelper.offset( tileBoxesInFixedSpace[ 1 ], Intervals.minAsLongArray( tileBoxPair.getB().getBoundaries() ) ) );
		final Interval[] originalTilesInFixedSpace = new Interval[] {
				new FinalInterval( tileBoxPair.getA().getOriginalTile().getSize() ),
				IntervalsHelper.translate( new FinalInterval( tileBoxPair.getB().getOriginalTile().getSize() ), originalMovingTileTopLeftCornerInFixedSpace ) };

		final Interval[] overlapsInOriginalTileSpace = new Interval[ 2 ];
		for ( int j = 0; j < 2; j++ )
			overlapsInOriginalTileSpace[ j ] = IntervalsHelper.offset( overlapInFixedSpace, Intervals.minAsLongArray( originalTilesInFixedSpace[ j ] ) );

		final long[] padding;
		if ( searchRadius == null )
		{
			padding = job.getArgs().padding();
			System.out.println( "Padding ROI by " + Arrays.toString( padding ) + " (padding arg)" );
		}
		else
		{
			final Interval searchRadiusBoundingBox = Intervals.smallestContainingInterval( searchRadius.getBoundingBox() );
			padding = new long[ searchRadiusBoundingBox.numDimensions() ];
			for ( int d = 0; d < padding.length; ++d )
				padding[ d ] = searchRadiusBoundingBox.dimension( d ) / 2;
			System.out.println( "Padding ROI by " + Arrays.toString( padding ) + " (half-size of the error ellipse) to capture the search radius entirely" );
		}

		final Interval[] paddedOverlapsInOriginalTileSpace = new Interval[ 2 ];
		for ( int j = 0; j < 2; ++j )
			paddedOverlapsInOriginalTileSpace[ j ] = TileOperations.padInterval(
					overlapsInOriginalTileSpace[ j ],
					originalTilesInFixedSpace[ j ],
					padding
				);
		return paddedOverlapsInOriginalTileSpace;
	}

	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > ImagePlus[] prepareRoiImages(
			final TilePair tileBoxPair,
			final Interval[] overlapsInOriginalTileSpace,
			final List< Map< String, TileInfo > > coordsToTilesChannels,
			final List< RandomAccessiblePairNullable< U, U > > flatfieldCorrectionChannels ) throws Exception
	{
		final TilePair originalTilePair = new TilePair( tileBoxPair.getA().getOriginalTile(), tileBoxPair.getB().getOriginalTile() );
		final double[] voxelDimensions = originalTilePair.getA().getPixelResolution();
		final double[] normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( voxelDimensions );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );
		final double blurSigma = job.getArgs().blurSigma();
		final double[] blurSigmas = new  double[ normalizedVoxelDimensions.length ];
		for ( int d = 0; d < blurSigmas.length; d++ )
			blurSigmas[ d ] = blurSigma / normalizedVoxelDimensions[ d ];

		final ImagePlus[] roiImps = new ImagePlus[ 2 ];
		final TileInfo[] originalTilePairArr = originalTilePair.toArray();
		for ( int j = 0; j < 2; j++ )
		{
			System.out.println( "Averaging corresponding tile images for " + job.getChannels() + " channels" );
			final String coordsStr = Utils.getTileCoordinatesString( originalTilePairArr[ j ] );
			int channelsUsed = 0;
			final ImagePlusImg< FloatType, ? > dst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( overlapsInOriginalTileSpace[ j ] ) );
			for ( int channel = 0; channel < job.getChannels(); channel++ )
			{
				final TileInfo tileInfo = coordsToTilesChannels.get( channel ).get( coordsStr );

				// skip if no tile exists for this channel at this particular stage position
				if ( tileInfo == null )
					throw new PipelineExecutionException( originalTilePair + ": cannot find corresponding tile for this channel" );

				if ( originalTilePairArr[ j ].getIndex().intValue() != tileInfo.getIndex().intValue() )
					throw new PipelineExecutionException( originalTilePair + ": different indexes for the same stage position " + Utils.getTileCoordinatesString( tileInfo ) );

				final ImagePlus imp = ImageImporter.openImage( tileInfo.getFilePath() );
				if ( imp == null )
					throw new PipelineExecutionException( "Image file does not exist: " + tileInfo.getFilePath() );

				// warn if image type and/or size do not match metadata
				if ( !ImageType.valueOf( imp.getType() ).equals( tileInfo.getType() ) )
					throw new PipelineExecutionException( String.format( "Image type %s does not match the value from metadata %s", ImageType.valueOf( imp.getType() ), tileInfo.getType() ) );
				if ( !Arrays.equals( Conversions.toLongArray( Utils.getImagePlusDimensions( imp ) ), tileInfo.getSize() ) )
					throw new PipelineExecutionException( String.format( "Image size %s does not match the value from metadata %s", Arrays.toString( Utils.getImagePlusDimensions( imp ) ), Arrays.toString( tileInfo.getSize() ) ) );

				// open image and crop roi
				final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );
				final RandomAccessibleInterval< T > imgCrop = Views.interval( img, overlapsInOriginalTileSpace[ j ] );

				// get flatfield-corrected source if the flatfields are provided
				final RandomAccessibleInterval< FloatType > sourceInterval;
				final RandomAccessiblePairNullable< U, U > flatfield = flatfieldCorrectionChannels.get( channel );
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

				// accumulate data in the output image
				final Cursor< FloatType > srcCursor = Views.flatIterable( sourceInterval ).cursor();
				final Cursor< FloatType > dstCursor = Views.flatIterable( dst ).cursor();
				while ( dstCursor.hasNext() || srcCursor.hasNext() )
					dstCursor.next().add( srcCursor.next() );

				imp.close();
				++channelsUsed;
			}

			if ( channelsUsed == 0 )
				throw new PipelineExecutionException( originalTilePair + ": images are missing in all channels" );

			// average output image over number of accumulated channels
			final FloatType denom = new FloatType( channelsUsed );
			final Cursor< FloatType > dstCursor = Views.iterable( dst ).cursor();
			while ( dstCursor.hasNext() )
				dstCursor.next().div( denom );

			// blur with requested sigma
			System.out.println( String.format( "Blurring the overlap area of size %s with sigmas=%s", Arrays.toString( Intervals.dimensionsAsLongArray( dst ) ), Arrays.toString( blurSigmas ) ) );
			blur( dst, blurSigmas );

			roiImps[ j ] = dst.getImagePlus();
			Utils.workaroundImagePlusNSlices( roiImps[ j ] );
		}
		return roiImps;
	}

	private < T extends NumericType< T > > void blur( final RandomAccessibleInterval< T > image, final double[] sigmas ) throws IncompatibleTypeException
	{
		final RandomAccessible< T > extendedImage = Views.extendMirrorSingle( image );
		Gauss3.gauss( sigmas, extendedImage, image, new SameThreadExecutorService() );
	}

	private List< TileInfo > divideRoiIntoParts( final long[] dimensions, final int shortestEdgeDimension, final int splitOverlapParts )
	{
		final Boundaries fullRoi = new Boundaries( dimensions );
		final int[] roiPartsCount = new int[ fullRoi.numDimensions() ];
		Arrays.fill( roiPartsCount, splitOverlapParts );
		roiPartsCount[ shortestEdgeDimension ] = 1;
		final List< TileInfo > roiParts = TileOperations.divideSpaceByCount( fullRoi, roiPartsCount );
		System.out.println( String.format( "Splitting the overlap in %d subintervals with grid of %s..", roiParts.size(), Arrays.toString( roiPartsCount ) ) );
		return roiParts;
	}

	private ImagePlus[] getRoiPartImages( final ImagePlus[] roiImps, final Interval roiPart ) throws ImgLibException
	{
		final ImagePlus[] roiPartImps = new ImagePlus[ 2 ];
		for ( int i = 0; i < 2; ++i )
		{
			// copy requested content into new image
			final RandomAccessibleInterval< FloatType > roiImg = ImagePlusImgs.from( roiImps[ i ] );
			final RandomAccessibleInterval< FloatType > roiPartImg = Views.offsetInterval( roiImg, roiPart );
			final ImagePlusImg< FloatType, ? > roiPartDst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( roiPart ) );
			final Cursor< FloatType > srcCursor = Views.flatIterable( roiPartImg ).cursor();
			final Cursor< FloatType > dstCursor = Views.flatIterable( roiPartDst ).cursor();
			while ( dstCursor.hasNext() || srcCursor.hasNext() )
				dstCursor.next().set( srcCursor.next() );
			roiPartImps[ i ] = roiPartDst.getImagePlus();
			Utils.workaroundImagePlusNSlices( roiPartImps[ i ] );
		}
		return roiPartImps;
	}

	private double computeVariance( final ImagePlus[] roiPartImps )
	{
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
		return variance;
	}

	/**
	 * Returns helper object containing required offset values for both tiles to be able to compute the shift vector.
	 *
	 * @param tileBoxPair
	 * @param overlapsInOriginalTileSpace
	 * @return
	 */
	private OffsetConverter getOffsetConverter( final TilePair tileBoxPair, final Interval[] overlapsInOriginalTileSpace )
	{
		final int dim = tileBoxPair.getA().numDimensions();

		// for transforming 'roi offset' to 'tile offset' (here tiles stand for boxes, and rois stand for extended overlapping intervals between the boxes)
		final long[][] roiToTileOffset = new long[ 2 ][];
		for ( int i = 0; i < 2; ++i )
		{
			roiToTileOffset[ i ] = new long[ dim ];
			for ( int d = 0; d < roiToTileOffset[ i ].length; ++d )
				roiToTileOffset[ i ][ d ] = overlapsInOriginalTileSpace[ i ].min( d ) - tileBoxPair.toArray()[ i ].getBoundaries().min( d );
		}

		// for transforming 'tile offset' to 'global offset' (here tile offset is in the coordinate space of the fixed box, and global space is of the original fixed tile)
		final double[] globalOffset = Intervals.minAsDoubleArray( tileBoxPair.getA().getBoundaries() );

		return new FinalOffsetConverter( roiToTileOffset, globalOffset );
	}

	private SerializablePairWiseStitchingResult stitchPairwise(
			final TilePair tileBoxPair,
			final ImagePlus[] roiImps,
			final PointValidator pointValidator,
			final OffsetConverter offsetConverter )
	{
		// compute variance within this ROI for both images
		final double variance = computeVariance( roiImps );

		final int timepoint = 1;
		final int numPeaks = 1;
		PairwiseStitchingPerformer.setThreads( 1 );

		final SerializablePairWiseStitchingResult[] results = PairwiseStitchingPerformer.stitchPairwise(
				roiImps[ 0 ], roiImps[ 1 ], timepoint, timepoint,
				job.getParams(), numPeaks,
				pointValidator, offsetConverter
			);

		final SerializablePairWiseStitchingResult result = results[ 0 ];

		if ( result == null )
		{
			// TODO: pass actions to update accumulators
//			noPeaksWithinConfidenceIntervalPairsCount.add( 1 );
			System.out.println( "no peaks found within the confidence interval" );
			return null;
		}
		else
		{
			// compute new tile offset
			final double[] tileOffset = offsetConverter.roiOffsetToTileOffset( Conversions.toDoubleArray( result.getOffset() ) );
			for ( int d = 0; d < tileOffset.length; ++d )
				result.getOffset()[ d ] = ( float ) tileOffset[ d ];

			// create point pair using center point of each ROI
			final Point fixedTileBoxCenterPoint = new Point( getTileBoxMiddlePoint( tileBoxPair.getA() ) );
			final double[] movingTileBoxCenter = new double[ result.getNumDimensions() ];
			for ( int d = 0; d < movingTileBoxCenter.length; ++d )
				movingTileBoxCenter[ d ] = fixedTileBoxCenterPoint.getL()[ d ] - result.getOffset( d );
			final Point movingTileBoxCenterPoint = new Point( movingTileBoxCenter );
			final PointPair pointPair = new PointPair( fixedTileBoxCenterPoint, movingTileBoxCenterPoint );

			result.setPointPair( pointPair );
			result.setTilePair( tileBoxPair );
			result.setVariance( variance );

			return result;
		}
	}

	/**
	 * Expands the overlaps with respect to the given search radius. Returns expanded ROIs in the coordinate space of each tile (useful for cropping).
	 *
	 * @param tileBoxesInFixedSpace
	 * @param combinedSearchRadius
	 * @param originalTilesInFixedSpace
	 * @return
	 */
	/*public static Interval[] adjustOverlappingRegion( final Interval[] tileBoxOverlaps, final SearchRadius combinedSearchRadius, final Interval[] originalTilesInFixedSpace )
	{
		// try all corners of the bounding box of the search radius and use the largest overlaps
		final Interval fixedTileBoxInFixedSpace = tileBoxesInFixedSpace[ 0 ], movingTileBoxInFixedSpace = tileBoxesInFixedSpace[ 1 ];

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

			final long[] testMovingTileBoxPositionInFixedSpace = new long[ searchRadiusBoundingBox.numDimensions() ];
			for ( int d = 0; d < cornersPos.length; ++d )
				testMovingTileBoxPositionInFixedSpace[ d ] = ( cornersPos[ d ] == 0 ? searchRadiusBoundingBox.min( d ) : searchRadiusBoundingBox.max( d ) );

			final Interval testMovingTileBoxInFixedSpace = IntervalsHelper.translate( new FinalInterval( movingTileBoxInFixedSpace ), testMovingTileBoxPositionInFixedSpace );
			final Interval testOverlapInFixedSpace = IntervalsNullable.intersect( fixedTileBoxInFixedSpace, testMovingTileBoxInFixedSpace );

			if ( testOverlapInFixedSpace != null )
			{
				final Interval[] testTileBoxesInFixedSpace = new Interval[] { tileBoxesInFixedSpace[ 0 ], };
				for ( int j = 0; j < 2; ++j )
				{
					final Interval testOverlap = Intervals.translate(interval, t, d)

					for ( int d = 0; d < searchRadiusBoundingBox.numDimensions(); ++d )
					{
						overlappingRegionMin[ d ] = Math.min( overlapPossibility.min( d ), overlappingRegionMin[ d ] );
						overlappingRegionMax[ d ] = Math.max( overlapPossibility.max( d ), overlappingRegionMax[ d ] );
					}
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

		final Interval maxOverlapInFixedSpace = new FinalInterval( overlappingRegionMin, overlappingRegionMax );

		final Interval[] maxOverlaps = new Interval[ 2 ];
		// TODO: fill
		return maxOverlaps;
	}*/
}
