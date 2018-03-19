package org.janelia.stitching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.janelia.util.Conversions;
import org.janelia.util.concurrent.SameThreadExecutorService;

import ij.ImagePlus;
import mpicbg.imglib.custom.OffsetConverter;
import mpicbg.imglib.custom.PointValidator;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsNullable;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;
import scala.Tuple2;

public class WarpedStitchingExecutor implements Serializable
{
	private static final int MAX_PARTITIONS = 15000;

	private static class StitchingResult implements Serializable
	{
		private static final long serialVersionUID = -533794988639089455L;

		public final SerializablePairWiseStitchingResult shift;
		public final double[] searchRadiusLength;

		public Map< Integer, long[] > globalTilePositionsDebug;

		public StitchingResult( final SerializablePairWiseStitchingResult shift, final double[] searchRadiusLength )
		{
			this.shift = shift;
			this.searchRadiusLength = searchRadiusLength;
		}
	}

	private static final long serialVersionUID = -7152174064553332061L;

	private final C1WarpedStitchingJob job;
	private transient final JavaSparkContext sparkContext;
	private final SerializableStitchingParameters stitchingParameters;

	public WarpedStitchingExecutor( final C1WarpedStitchingJob job, final JavaSparkContext sparkContext )
	{
		this.job = job;
		this.sparkContext = sparkContext;
		this.stitchingParameters = job.getParams();
	}

	public void run() throws PipelineExecutionException, IOException
	{
		final List< TilePair > overlappingBoxes = findOverlappingBoxes();
		preparePairwiseShifts( overlappingBoxes );

		final WarpedStitchingOptimizer optimizer = new WarpedStitchingOptimizer( job, sparkContext );
		optimizer.optimize();
	}

	private List< TilePair > findOverlappingBoxes() throws IOException
	{
		// split tiles into 8 boxes
		final int[] tileBoxesGridSize = new int[ job.getDimensionality() ];
		Arrays.fill( tileBoxesGridSize, job.getArgs().splitOverlapParts() );
		final List< TileInfo > tileBoxes = SplitTileOperations.splitTilesIntoBoxes( job.getTiles( 0 ), tileBoxesGridSize );
		System.out.println( "  " + tileBoxes.size() + " tiles boxes were generated" );

		// find bounding boxes of warped tiles
		final List< Tuple2< Integer, Interval > > tileWarpedBoundingBoxes = sparkContext
				.parallelize( Arrays.asList( job.getTiles( 0 ) ), Math.min( job.getTiles( 0 ).length, MAX_PARTITIONS ) )
				.mapToPair(
						tile ->
						{
							final String slab = job.getTileSlabMapping().getSlab( tile );
							return new Tuple2<>(
									tile.getIndex(),
									WarpedTileLoader.getBoundingBox(
											job.getTileSlabMapping().getSlabMin( slab ),
											tile,
											job.getTileSlabMapping().getSlabTransform( slab )
										)
								);
						}
					)
				.collect();
		System.out.println( "  got bounding boxes for warped tiles" );

		// find pairs of possibly overlapping tiles
		final Broadcast< List< Tuple2< Integer, Interval > > > tileWarpedBoundingBoxesBroadcast = sparkContext.broadcast( tileWarpedBoundingBoxes );
		final List< Tuple2< Integer, Integer > > possiblyOverlappingTiles = sparkContext
				.parallelize( tileWarpedBoundingBoxes, Math.min( tileWarpedBoundingBoxes.size(), MAX_PARTITIONS ) )
				.flatMap(
						tileWarpedBoundingBoxTuple ->
						{
							final int tileIndex = tileWarpedBoundingBoxTuple._1();
							final Interval tileWarpedBoundingBox = tileWarpedBoundingBoxTuple._2();
							final List< Tuple2< Integer, Integer > > possiblyOverlappingTilesWithCurrent = new ArrayList<>();
							for ( final Tuple2< Integer, Interval > otherTileWarpedBoundingBox : tileWarpedBoundingBoxesBroadcast.value() )
								if ( tileIndex < otherTileWarpedBoundingBox._1().intValue() && TileOperations.overlap( tileWarpedBoundingBox, otherTileWarpedBoundingBox._2() ) )
									possiblyOverlappingTilesWithCurrent.add( new Tuple2<>( tileIndex, otherTileWarpedBoundingBox._1() ) );
							return possiblyOverlappingTilesWithCurrent.iterator();
						}
					)
				.collect();
		tileWarpedBoundingBoxesBroadcast.destroy();
		System.out.println( "  got " + possiblyOverlappingTiles.size() + " possibly overlapping tile pairs" );

		final Set< Integer > possiblyOverlappingTilesCounter = new HashSet<>();
		for ( final Tuple2< Integer, Integer > possiblyOverlappingTilePair : possiblyOverlappingTiles )
			for ( final int tileIndex : new int[] { possiblyOverlappingTilePair._1(), possiblyOverlappingTilePair._2() } )
				possiblyOverlappingTilesCounter.add( tileIndex );
		System.out.println( "  Possibly overlapping tiles count: " + possiblyOverlappingTilesCounter.size() );

		// helper mapping: tile -> tile boxes
		final Map< Integer, List< TileInfo > > tileToBoxes = new HashMap<>();
		for ( final TileInfo tileBox : tileBoxes )
		{
			final int tileIndex = tileBox.getOriginalTile().getIndex();
			if ( !tileToBoxes.containsKey( tileIndex ) )
				tileToBoxes.put( tileIndex, new ArrayList<>() );
			tileToBoxes.get( tileIndex ).add( tileBox );
		}
		System.out.println( "  created helper mapping: tile -> tile boxes" );

		// find overlapping tile box pairs
		final Broadcast< Map< Integer, List< TileInfo > > > tileToBoxesBroadcast = sparkContext.broadcast( tileToBoxes );
		final List< TilePair > overlappingBoxes = sparkContext
				.parallelize( possiblyOverlappingTiles, Math.min( possiblyOverlappingTiles.size(), MAX_PARTITIONS ) )
				.flatMap(
						possiblyOverlappingTilePair ->
						{
							final List< TilePair > overlappingBoxesForTilePair = new ArrayList<>();
							for ( final TileInfo tileBox1 : tileToBoxesBroadcast.value().get( possiblyOverlappingTilePair._1() ) )
							{
								for ( final TileInfo tileBox2 : tileToBoxesBroadcast.value().get( possiblyOverlappingTilePair._2() ) )
								{
									final TilePair tileBoxPair = new TilePair( tileBox1, tileBox2 );

									if ( tileBoxPair.getA().getIndex().intValue() > tileBoxPair.getB().getIndex().intValue() )
										throw new RuntimeException( "should not happen: overlapping tile pairs are already sorted by its indexes" );
//									if ( tileBoxPair.getA().getIndex().intValue() > tileBoxPair.getB().getIndex().intValue() )
//										tileBoxPair.swap();

									if ( WarpedSplitTileOperations.isOverlappingTileBoxPair( tileBoxPair, !job.getArgs().useAllPairs(), job.getTileSlabMapping() ) )
										overlappingBoxesForTilePair.add( tileBoxPair );
								}
							}
							return overlappingBoxesForTilePair.iterator();
						}
					)
				.collect();
		tileToBoxesBroadcast.destroy();
		System.out.println( "  collected " + overlappingBoxes.size() + " overlapping pairs" );

		final Set< Integer > overlappingTilesCounter = new HashSet<>();
		for ( final TilePair overlappingTileBoxPair : overlappingBoxes )
			for ( final TileInfo tileBox : overlappingTileBoxPair.toArray() )
				overlappingTilesCounter.add( tileBox.getOriginalTile().getIndex() );
		System.out.println( "  Overlapping tiles (based on overlapping tile boxes) count: " + overlappingTilesCounter.size() );

		return overlappingBoxes;
	}


	/**
	 * Initiates the computation for pairs that have not been precomputed. Stores the pairwise file for this iteration of stitching.
	 */
	private void preparePairwiseShifts( final List< TilePair > overlappingTiles ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final String basePath = job.getBasePath();
		final String pairwiseFilename = "pairwise.json";
		final String pairwisePath = PathResolver.get( basePath, pairwiseFilename );

		// FIXME: replaces checking contents of the pairwise file by simply checking its existence
		if ( dataProvider.fileExists( URI.create( pairwisePath ) ) )
		{
			System.out.println( "pairwise.json file exists, don't recompute shifts" );
			return;
		}

		final List< SerializablePairWiseStitchingResult > pairwiseShifts = tryLoadPrecomputedShifts( basePath );
		final List< TilePair > pendingOverlappingTiles = removePrecomputedPendingPairs( pairwisePath, overlappingTiles, pairwiseShifts );

		if ( pendingOverlappingTiles.isEmpty() && !pairwiseShifts.isEmpty() )
		{
			// If we're able to load precalculated pairwise results, save some time skipping this step and jump to the global optimization
			System.out.println( "Successfully loaded all pairwise results from disk!" );
		}
		else
		{
			// Initiate the computation
			final List< StitchingResult > stitchingResults = computePairwiseShifts( pendingOverlappingTiles );

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
	 */
	private List< SerializablePairWiseStitchingResult > tryLoadPrecomputedShifts( final String basePath ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final String pairwiseFilename = "pairwise.json";
		final String pairwisePath = PathResolver.get( basePath, pairwiseFilename );

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
	private List< TilePair > removePrecomputedPendingPairs(
			final String pairwisePath,
			final List< TilePair > overlappingTiles,
			final List< SerializablePairWiseStitchingResult > pairwiseShifts ) throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
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
			TileInfoJSONProvider.savePairwiseShifts( pairwiseShifts, dataProvider.getJsonWriter( URI.create( pairwisePath ) ) );

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
		final DataProvider dataProvider = job.getDataProvider();
		try ( final OutputStream out = dataProvider.getOutputStream( URI.create( searchRadiusStatsPath ) ) )
		{
			try ( final PrintWriter writer = new PrintWriter( out ) )
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
		}
		catch ( final Exception e )
		{
			throw new PipelineExecutionException( "Can't write search radius stats: " + e.getMessage(), e );
		}
	}

	/**
	 * Computes the best possible pairwise shifts between every pair of tiles on a Spark cluster.
	 * It uses phase correlation for measuring similarity between two images.
	 * @throws IOException
	 */
	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > List< StitchingResult > computePairwiseShifts(
			final List< TilePair > overlappingBoxes ) throws PipelineExecutionException, IOException
	{
		final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedFlatfieldCorrectionForChannels = sparkContext.broadcast( loadFlatfieldChannels() );
		final Broadcast< List< Map< String, TileInfo > > > broadcastedCoordsToTilesChannels = sparkContext.broadcast( getCoordsToTilesChannels() );

		// validate that coords->tiles mapping is correct
		for ( final Entry< String, TileInfo > entry : broadcastedCoordsToTilesChannels.value().get( 0 ).entrySet() )
		{
			final TileInfo correspondingTile = broadcastedCoordsToTilesChannels.value().get( 1 ).get( entry.getKey() );
			if ( entry.getValue().getIndex().intValue() != correspondingTile.getIndex().intValue() )
				throw new RuntimeException( "coords->tiles mapping is inconsistent" );
		}

		System.out.println( "Processing " + overlappingBoxes.size() + " pairs..." );

		final LongAccumulator notEnoughNeighborsWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noOverlapWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();
		final LongAccumulator noPeaksWithinConfidenceIntervalPairsCount = sparkContext.sc().longAccumulator();

		final JavaRDD< StitchingResult > pairwiseStitching = sparkContext.parallelize( overlappingBoxes, Math.min( overlappingBoxes.size(), MAX_PARTITIONS ) ).map( tileBoxPair ->
			{
				System.out.println( "Processing tile box pair " + tileBoxPair + " of tiles " + new TilePair( tileBoxPair.getA().getOriginalTile(), tileBoxPair.getB().getOriginalTile() ) );

				// both tile boxes are transformed into the global coordinate space
				final Pair< Interval, Interval > transformedTileBoxPairGlobalSpace = WarpedSplitTileOperations.transformTileBoxPair( tileBoxPair, job.getTileSlabMapping() );

				// map them into the fixed tile box coordinate space (that is, transformedTileBoxPair.getA() will have zero min in the target space)
				final Pair< Interval, Interval > transformedTileBoxPairFixedBoxSpace = SplitTileOperations.globalToFixedBoxSpace( transformedTileBoxPairGlobalSpace );

				// validate that they actually intersect
				if ( IntervalsNullable.intersect( transformedTileBoxPairFixedBoxSpace.getA(), transformedTileBoxPairFixedBoxSpace.getB() ) == null )
					throw new RuntimeException( "should not happen: only overlapping tile box pairs were selected" );

				// find their 'mean' offset around which an error ellipse will be defined (as intervals are now defined in the fixed tile box space, it is just the position of the moving tile box in this space)
				final long[] meanOffset = Intervals.minAsLongArray( transformedTileBoxPairFixedBoxSpace.getB() );

				// define search radius in the fixed tile box space
				final double[][] offsetsCovarianceMatrix = new double[][] { new double[] { 1, 0, 0 }, new double[] { 0, 1, 0 }, new double[] { 0, 0, 1 } };
				final double sphereRadiusPixels = job.getArgs().searchRadiusMultiplier();
				final SearchRadius searchRadius = new SearchRadius( sphereRadiusPixels, Conversions.toDoubleArray( meanOffset ), offsetsCovarianceMatrix );



				// FIXME -- test without rematching
				/*if ( meanOffset.length > 0 )
				{
					final SerializablePairWiseStitchingResult pairwiseResult = new SerializablePairWiseStitchingResult(
							tileBoxPair,
							Conversions.toFloatArray( Conversions.toDoubleArray( meanOffset ) ),
							1.f, 1.f, 1000.
						);
					final StitchingResult result = new StitchingResult( pairwiseResult, searchRadius != null ? searchRadius.getEllipseRadius() : null );
					result.globalTilePositionsDebug = new TreeMap<>();
					result.globalTilePositionsDebug.put( tileBoxPair.getA().getOriginalTile().getIndex(), Intervals.minAsLongArray( transformedTileBoxPairGlobalSpace.getA() ) );
					result.globalTilePositionsDebug.put( tileBoxPair.getB().getOriginalTile().getIndex(), Intervals.minAsLongArray( transformedTileBoxPairGlobalSpace.getB() ) );
					return result;
				}*/



				// get ROIs in the fixed tile box space and moving tile box space, respectively
				final Pair< Interval, Interval > adjustedOverlaps = SplitTileOperations.getAdjustedOverlapIntervals( transformedTileBoxPairFixedBoxSpace, searchRadius );
				if ( adjustedOverlaps == null )
					throw new RuntimeException( "should not happen: only overlapping tile box pairs were selected, thus adjusted overlaps should always be non-empty too" );

				// get ROI in the full tiles for cropping
				final Pair< Interval, Interval > adjustedOverlapsInFullTile = SplitTileOperations.getOverlapsInFullTile( tileBoxPair, adjustedOverlaps );

				// prepare ROI images
				final ImagePlus[] roiImps = prepareRoiImages(
						new TilePair( tileBoxPair.getA().getOriginalTile(), tileBoxPair.getB().getOriginalTile() ),
						adjustedOverlapsInFullTile,
						broadcastedCoordsToTilesChannels.value(),
						broadcastedFlatfieldCorrectionForChannels.value()
					);

				// get required offsets for roi parts
				final OffsetConverter offsetConverter = SplitTileOperations.getOffsetConverter( adjustedOverlaps );

				final SerializablePairWiseStitchingResult pairwiseResult = stitchPairwise( tileBoxPair, roiImps, searchRadius, offsetConverter );

				// compute variance within ROI for both images
				if ( pairwiseResult != null )
					pairwiseResult.setVariance( computeVariance( roiImps ) );

				for ( int i = 0; i < 2; i++ )
					roiImps[ i ].close();

				System.out.println( "Stitched tile box pair " + tileBoxPair + " of tiles " + new TilePair( tileBoxPair.getA().getOriginalTile(), tileBoxPair.getB().getOriginalTile() ) );

				return new StitchingResult( pairwiseResult, searchRadius != null ? searchRadius.getEllipseRadius() : null );
			} );

		final List< StitchingResult > stitchingResults = pairwiseStitching.collect();

		broadcastedFlatfieldCorrectionForChannels.destroy();
		broadcastedCoordsToTilesChannels.destroy();


		// ----------------------
		// FIXME: log tile global positions into file
		/*final Map< Integer, long[] > tilesGlobalSpacePosition = new TreeMap<>();
		for ( final StitchingResult result : stitchingResults )
		{
			for ( final Entry< Integer, long[] > entry : result.globalTilePositionsDebug.entrySet() )
			{
				if ( !tilesGlobalSpacePosition.containsKey( entry.getKey() ) )
				{
					tilesGlobalSpacePosition.put( entry.getKey(), entry.getValue() );
				}
				else
				{
					final Interval existingInterval = IntervalsHelper.translate(
							new FinalInterval( result.shift.getTilePair().getA().getOriginalTile().getSize() ),
							tilesGlobalSpacePosition.get( entry.getKey() )
						);
					final Interval currentInterval = IntervalsHelper.translate(
							new FinalInterval( result.shift.getTilePair().getA().getOriginalTile().getSize() ),
							entry.getValue()
						);
					if ( !Intervals.equals( existingInterval, currentInterval ) )
						throw new RuntimeException( "different resulting intervals" );
				}
			}
		}
//		try ( final PrintWriter debugWriter = new PrintWriter( "/nrs/saalfeld/igor/illumination-correction/Sample1_C1/stitching/restitching-affine/debug-positions.txt" ) )
//		{
//			for ( final Entry< Integer, long[] > entry : tilesGlobalSpacePosition.entrySet() )
//			{
//				final List< String > posStr = new ArrayList<>();
//				for ( int d = 0; d < entry.getValue().length; ++d )
//					posStr.add( Long.toString( entry.getValue()[ d ] ) );
//				debugWriter.println( entry.getKey() + " " + String.join( " ", posStr ) );
//			}
//		}
		for ( int ch = 0; ch < job.getChannels(); ++ch )
		{
			final List< TileInfo > debugTiles = new ArrayList<>();
			for ( final TileInfo tile : job.getTiles( ch ) )
			{
				final TileInfo debugTile = tile.clone();
//				debugTile.setPosition( Conversions.toDoubleArray( tilesGlobalSpacePosition.get( debugTile.getIndex() ) ) );
//				debugTile.setTransform( null );
				final double[] position = Conversions.toDoubleArray( tilesGlobalSpacePosition.get( debugTile.getIndex() ) );
				final AffineTransform3D translationTransform = new AffineTransform3D();
				translationTransform.setTranslation( position );
				debugTile.setTransform( translationTransform );
				debugTiles.add( debugTile );
			}
			TileInfoJSONProvider.saveTilesConfiguration(
					debugTiles.toArray( new TileInfo[ 0 ] ),
					job.getDataProvider().getJsonWriter( URI.create( "/nrs/saalfeld/igor/illumination-correction/Sample1_C1/stitching/restitching-affine/10-11z_21-22x_21-22y/fixed/without-rematching/without-tile-boxes/debug/debug-ch" + ch + ".json" ) )
				);
		}*/
		// ----------------------


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
		for ( int ch = 0; ch < job.getChannels(); ++ch )
		{
			try
			{
				flatfieldCorrectionForChannels.add(
						FlatfieldCorrection.loadCorrectionImages(
								dataProvider,
								job.getFlatfieldPath( ch ),
								job.getDimensionality()
							)
					);
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
	 * @throws IOException
	 */
	private List< Map< String, TileInfo > > getCoordsToTilesChannels() throws PipelineExecutionException, IOException
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

	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > ImagePlus[] prepareRoiImages(
			final TilePair originalTilePair,
			final Pair< Interval, Interval > overlapsInOriginalTileSpace,
			final List< Map< String, TileInfo > > coordsToTilesChannels,
			final List< RandomAccessiblePairNullable< U, U > > flatfieldCorrectionChannels ) throws Exception
	{
		final DataProvider dataProvider = job.getDataProvider();
		final double[] voxelDimensions = originalTilePair.getA().getPixelResolution();
		final double[] normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( voxelDimensions );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );
		final double blurSigma = job.getArgs().blurSigma();
		final double[] blurSigmas = new  double[ normalizedVoxelDimensions.length ];
		for ( int d = 0; d < blurSigmas.length; d++ )
			blurSigmas[ d ] = blurSigma / normalizedVoxelDimensions[ d ];

		final ImagePlus[] roiImps = new ImagePlus[ 2 ];
		final TileInfo[] originalTilePairArr = originalTilePair.toArray();
		final Interval[] overlapsInOriginalTileSpaceArr = new Interval[] { overlapsInOriginalTileSpace.getA(), overlapsInOriginalTileSpace.getB() };

		final int numChannels = job.getChannels();
		for ( int j = 0; j < 2; j++ )
		{
			System.out.println( "Averaging corresponding tile images for " + numChannels + " channels" );
			final String coordsStr = Utils.getTileCoordinatesString( originalTilePairArr[ j ] );
			int channelsUsed = 0;
			final ImagePlusImg< FloatType, ? > dst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( overlapsInOriginalTileSpaceArr[ j ] ) );
			for ( int channel = 0; channel < numChannels; channel++ )
			{
				final TileInfo tileInfo = coordsToTilesChannels.get( channel ).get( coordsStr );

				if ( tileInfo == null )
					throw new PipelineExecutionException( originalTilePair + ": cannot find corresponding tile for this channel" );

				if ( originalTilePairArr[ j ].getIndex().intValue() != tileInfo.getIndex().intValue() )
					throw new PipelineExecutionException( originalTilePair + ": different indexes for the same stage position " + Utils.getTileCoordinatesString( tileInfo ) );

				// open image
				final RandomAccessibleInterval< T > img = TileLoader.loadTile( tileInfo, dataProvider );
				if ( img == null )
					throw new PipelineExecutionException( "Cannot load tile image: " + tileInfo.getFilePath() );

				// warn if image type and/or size do not match metadata
				final T type = Util.getTypeFromInterval( img );
				if ( !type.getClass().equals( tileInfo.getType().getType().getClass() ) )
					throw new PipelineExecutionException( String.format( "Image type %s does not match the value from metadata %s", type.getClass().getName(), tileInfo.getType() ) );
				if ( !Arrays.equals( Intervals.dimensionsAsLongArray( img ), tileInfo.getSize() ) )
					throw new PipelineExecutionException( String.format( "Image size %s does not match the value from metadata %s", Arrays.toString( Intervals.dimensionsAsLongArray( img ) ), Arrays.toString( tileInfo.getSize() ) ) );

				// crop ROI
				final RandomAccessibleInterval< T > imgCrop = Views.interval( img, overlapsInOriginalTileSpaceArr[ j ] );

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

	private SerializablePairWiseStitchingResult stitchPairwise(
			final TilePair tileBoxPair,
			final ImagePlus[] roiImps,
			final PointValidator pointValidator,
			final OffsetConverter offsetConverter )
	{
		final int timepoint = 1;
		final int numPeaks = 1;
		PairwiseStitchingPerformer.setThreads( 1 );

		final SerializablePairWiseStitchingResult[] results = PairwiseStitchingPerformer.stitchPairwise(
				roiImps[ 0 ], roiImps[ 1 ], timepoint, timepoint,
				stitchingParameters, numPeaks,
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
			// compute new offset between original tiles
			final double[] originalTileOffset = offsetConverter.roiOffsetToTileOffset( Conversions.toDoubleArray( result.getOffset() ) );
			for ( int d = 0; d < originalTileOffset.length; ++d )
				result.getOffset()[ d ] = ( float ) originalTileOffset[ d ];

//			final PointPair pointPair = SplitTileOperations.createPointPair( tileBoxPair, originalTileOffset );
//			result.setPointPair( pointPair );

			result.setTilePair( tileBoxPair );

			return result;
		}
	}
}
