package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.img.TpsTransformWrapper;
import net.imglib2.Cursor;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.SerializableFinalInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsNullable;
import net.imglib2.view.Views;
import scala.Tuple2;

public class WarpedTileCounter implements Serializable
{
	private static final long serialVersionUID = -3670611071620152571L;

	private static final int MAX_PARTITIONS = 15000;

	private transient final JavaSparkContext sparkContext;
	private final Map< String, TileInfo[] > slabsTiles;
	private final Map< String, double[] > slabsMin;
	private final Map< String, TpsTransformWrapper > slabsTransforms;
	private final double[] voxelDimensions;
	private final int outputCellSize;
	private final String outputPath;

	double[] normalizedVoxelDimensions;

	public WarpedTileCounter(
			final JavaSparkContext sparkContext,
			final Map< String, TileInfo[] > slabsTiles,
			final Map< String, double[] > slabsMin,
			final Map< String, TpsTransformWrapper > slabsTransforms,
			final double[] voxelDimensions,
			final int outputCellSize,
			final String outputPath )
	{
		this.sparkContext = sparkContext;
		this.slabsTiles = slabsTiles;
		this.slabsMin = slabsMin;
		this.slabsTransforms = slabsTransforms;
		this.voxelDimensions = voxelDimensions;
		this.outputCellSize = outputCellSize;
		this.outputPath = outputPath;
	}

	public void run() throws PipelineExecutionException
	{
		try {
			runImpl();
		} catch (final IOException e) {
			e.printStackTrace();
			throw new PipelineExecutionException( e.getMessage(), e.getCause() );
		}
	}

	private void runImpl() throws PipelineExecutionException, IOException
	{
		final String baseExportPath = Paths.get( outputPath, "used-tiles.n5" ).toString();
		final N5Writer n5 = N5.openFSWriter( baseExportPath );

		normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( voxelDimensions );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );

		// loop over channels
		final String outputPath = "data";

		// Generate export of the first scale level
		if ( !n5.datasetExists( outputPath ) )
		{
			countTilesWarp( baseExportPath, outputPath );
		}

		System.out.println( "Done" );
	}


	private int[] getOptimalCellSize()
	{
		final int[] cellSize = new int[ voxelDimensions.length ];
		for ( int d = 0; d < cellSize.length; d++ )
			cellSize[ d ] = ( int ) Math.round( outputCellSize / normalizedVoxelDimensions[ d ] );
		return cellSize;
	}

	private Map< Integer, Interval > getTilesWarpedBoundingBoxes( final Map< String, TileInfo[] > slabTiles )
	{
		final List< Tuple2< String, TileInfo > > flattenedSlabTiles = new ArrayList<>();
		for ( final Entry< String, TileInfo[] > entry : slabTiles.entrySet() )
			for ( final TileInfo slabTile : entry.getValue() )
				flattenedSlabTiles.add( new Tuple2<>( entry.getKey(), slabTile ) );

		final Map< Integer, Interval > boxes = sparkContext
				.parallelize( flattenedSlabTiles )
				.mapToPair( slabAndTile
						-> new Tuple2<>(
								slabAndTile._2().getIndex(),
								WarpedTileLoader.getBoundingBox(
										slabsMin.get( slabAndTile._1() ),
										slabAndTile._2(),
										slabsTransforms.get( slabAndTile._1() )
									)
							)
					)
				.collectAsMap();

		final Map< Integer, Interval > serializableBoxes = new HashMap<>();
		for ( final Entry< Integer, Interval > entry : boxes.entrySet() )
			serializableBoxes.put( entry.getKey(), new SerializableFinalInterval( entry.getValue() ) );
		return serializableBoxes;
	}

	private void countTilesWarp( final String baseExportPath, final String outputPath ) throws IOException
	{
		final int[] cellSize = getOptimalCellSize();

		final Map< Integer, Interval > tileWarpedBoundingBoxes = getTilesWarpedBoundingBoxes( slabsTiles );

		final Map< Integer, TileInfo > tilesMap = new HashMap<>();
		final Map< Integer, String > slabsMap = new HashMap<>();
		for ( final Entry< String, TileInfo[] > entry : slabsTiles.entrySet() )
		{
			for ( final TileInfo tile : entry.getValue() )
			{
				tilesMap.put( tile.getIndex(), tile );
				slabsMap.put( tile.getIndex(), entry.getKey() );
			}
		}

		final Interval boundingBox = TileOperations.getCollectionBoundaries( tileWarpedBoundingBoxes.values().toArray( new Interval[ 0 ] ) );
		final long[] offset = Intervals.minAsLongArray( boundingBox );
		final long[] dimensions = Intervals.dimensionsAsLongArray( boundingBox );

		// use the size of the tile as a bigger cell to minimize the number of loads for each image
//		final int[] biggerCellSize = new int[ cellSize.length ];
//		for ( int d = 0; d < biggerCellSize.length; ++d )
//			biggerCellSize[ d ] = cellSize[ d ] * ( int ) Math.ceil( ( double ) new ArrayList<>( tileWarpedBoundingBoxes.values() ).get( 0 ).dimension( d ) / cellSize[ d ] ) / 2;
		final int[] biggerCellSize = new int[] { cellSize[ 0 ] * 2, cellSize[ 1 ] * 2, cellSize[ 2 ] * 4 };

		final List< TileInfo > biggerCells = TileOperations.divideSpace( boundingBox, new FinalDimensions( biggerCellSize ) );

		N5.openFSWriter( baseExportPath ).createDataset( outputPath, Intervals.dimensionsAsLongArray( boundingBox ), cellSize, DataType.INT32, CompressionType.GZIP );

		final List< long[] > gridCoords = sparkContext.parallelize( biggerCells, Math.min( biggerCells.size(), MAX_PARTITIONS ) ).map( biggerCell ->
			{
				final Boundaries biggerCellBox = biggerCell.getBoundaries();

				final long[] biggerCellOffsetCoordinates = new long[ biggerCellBox.numDimensions() ];
				for ( int d = 0; d < biggerCellOffsetCoordinates.length; d++ )
					biggerCellOffsetCoordinates[ d ] = biggerCellBox.min( d ) - offset[ d ];

				final long[] biggerCellGridPosition = new long[ biggerCell.numDimensions() ];
				final CellGrid cellGrid = new CellGrid( dimensions, cellSize );
				cellGrid.getCellPosition( biggerCellOffsetCoordinates, biggerCellGridPosition );

				return biggerCellGridPosition;
			} ).collect();
		final long[] gridOffset = new long[ biggerCellSize.length ];
		Arrays.fill( gridOffset, Long.MAX_VALUE );
		for ( final long[] gridCoord : gridCoords )
			for ( int d = 0; d < gridOffset.length; ++d )
				gridOffset[ d ] = Math.min( gridCoord[ d ], gridOffset[ d ] );

		System.out.println();
		System.out.println( "Adjusted intermediate cell size to " + Arrays.toString( biggerCellSize ) + " (for faster processing)" );
		System.out.println( "Grid offset = " + Arrays.toString( gridOffset ) );
		System.out.println();

		sparkContext.parallelize( biggerCells, Math.min( biggerCells.size(), MAX_PARTITIONS ) ).foreach( biggerCell ->
			{
				final Boundaries biggerCellBox = biggerCell.getBoundaries();
				final Set< Integer > tileIndexesWithinCell = TileOperations.findTilesWithinSubregion( tileWarpedBoundingBoxes, biggerCellBox );
				if ( tileIndexesWithinCell.isEmpty() )
					return;

				final List< TileInfo > tilesWithinCell = new ArrayList<>();
				for ( final Integer tileIndex : tileIndexesWithinCell )
					tilesWithinCell.add( tilesMap.get( tileIndex ) );

				final RandomAccessibleInterval< IntType > outImg = countWarpedTiles(
						biggerCellBox,
						tilesWithinCell,
						slabsMap,
						slabsMin,
						slabsTransforms
					);

				final long[] biggerCellOffsetCoordinates = new long[ biggerCellBox.numDimensions() ];
				for ( int d = 0; d < biggerCellOffsetCoordinates.length; d++ )
					biggerCellOffsetCoordinates[ d ] = biggerCellBox.min( d ) - offset[ d ];

				final long[] biggerCellGridPosition = new long[ biggerCell.numDimensions() ];
				final CellGrid cellGrid = new CellGrid( dimensions, cellSize );
				cellGrid.getCellPosition( biggerCellOffsetCoordinates, biggerCellGridPosition );

				final long[] biggerCellGridOffsetPosition = new long[ biggerCellGridPosition.length ];
				for ( int d = 0; d < gridOffset.length; ++d )
					biggerCellGridOffsetPosition[ d ] = biggerCellGridPosition[ d ] - gridOffset[ d ];

				final N5Writer n5Local = N5.openFSWriter( baseExportPath );
				N5Utils.saveBlock( outImg, n5Local, outputPath, biggerCellGridOffsetPosition );
			}
		);
	}

	public static RandomAccessibleInterval< IntType > countWarpedTiles(
			final Interval targetInterval,
			final List< TileInfo > tilesWithinCell,
			final Map< Integer, String > slabsMap,
			final Map< String, double[] > slabsMin,
			final Map< String, TpsTransformWrapper > slabsTransforms ) throws Exception
	{
		// initialize output image
		final RandomAccessibleInterval< IntType > out = ArrayImgs.ints( Intervals.dimensionsAsLongArray( targetInterval ) );

		// initialize helper image for hard-cut fusion strategy
		final RandomAccessibleInterval< FloatType > maxMinDistances = ArrayImgs.floats( Intervals.dimensionsAsLongArray( targetInterval ) );

		for ( final TileInfo tile : tilesWithinCell )
		{
			final String slab = slabsMap.get( tile.getIndex() );
			final double[] slabMin = slabsMin.get( slab );
			final TpsTransformWrapper slabTransform = slabsTransforms.get( slab );

			final Interval warpedTileInterval = WarpedTileLoader.getBoundingBox( slabMin, tile, slabTransform );

			final FinalRealInterval intersection = IntervalsNullable.intersectReal( warpedTileInterval, targetInterval );
			if ( intersection == null )
				throw new IllegalArgumentException( "tilesWithinCell contains a tile that doesn't intersect with the target interval: " + "Warped tile " + tile.getIndex() +  "; " + "Output cell " + " at " + Arrays.toString( Intervals.minAsIntArray( targetInterval ) ) + " of size " + Arrays.toString( Intervals.dimensionsAsIntArray( targetInterval ) ) );

			final long[] offset = new long[ targetInterval.numDimensions() ];
			final long[] minIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			final long[] maxIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			for ( int d = 0; d < minIntersectionInTargetInterval.length; ++d )
			{
				offset[ d ] = warpedTileInterval.min( d ) - targetInterval.min( d );
				minIntersectionInTargetInterval[ d ] = ( long ) Math.floor( intersection.realMin( d ) ) - targetInterval.min( d );
				maxIntersectionInTargetInterval[ d ] = ( long ) Math.ceil ( intersection.realMax( d ) ) - targetInterval.min( d );
			}
			final Interval intersectionIntervalInTargetInterval = new FinalInterval( minIntersectionInTargetInterval, maxIntersectionInTargetInterval );

			final RandomAccessibleInterval< IntType > outInterval = Views.interval( out, intersectionIntervalInTargetInterval ) ;
			final RandomAccessibleInterval< FloatType > maxMinDistanceInterval = Views.interval( maxMinDistances, intersectionIntervalInTargetInterval ) ;

			final Cursor< IntType > outCursor = Views.flatIterable( outInterval ).cursor();
			final Cursor< FloatType > maxMinDistanceCursor = Views.flatIterable( maxMinDistanceInterval ).cursor();

			final IntType outValue = new IntType( tile.getIndex() + 1 );

			while ( outCursor.hasNext() || maxMinDistanceCursor.hasNext() )
			{
				// move all cursors forward
				outCursor.fwd();
				maxMinDistanceCursor.fwd();

				// get global position
				final double[] globalPosition = new double[ outCursor.numDimensions() ];
				outCursor.localize( globalPosition );
				for ( int d = 0; d < globalPosition.length; ++d )
					globalPosition[ d ] += targetInterval.min( d );

				// get local position inside tile, or null if outside
				final double[] positionInsideTile = getPositionInsideTile( globalPosition, tile, slabMin, slabTransform );
				if ( positionInsideTile == null )
					continue;

				// update the distance and the value if needed
				double minDistance = Double.MAX_VALUE;
				for ( int d = 0; d < positionInsideTile.length; ++d )
				{
					final double dist = Math.min(
							positionInsideTile[ d ],
							tile.getSize( d ) - 1 - positionInsideTile[ d ]
						);
					minDistance = Math.min( dist, minDistance );
				}
				if ( minDistance >= maxMinDistanceCursor.get().get() )
				{
					maxMinDistanceCursor.get().setReal( minDistance );
					outCursor.get().set( outValue );
				}
			}
		}

		return out;
	}

	private static double[] getPositionInsideTile(
			final double[] globalPositionPixels,
			final TileInfo tile,
			final double[] slabMin,
			final TpsTransformWrapper slabTransform )
	{
		// get warped global position in physical units
		final double[] warpedGlobalPositionPhysicalUnits = new double[ globalPositionPixels.length ];
		for ( int d = 0; d < warpedGlobalPositionPhysicalUnits.length; ++d )
			warpedGlobalPositionPhysicalUnits[ d ] = globalPositionPixels[ d ] * tile.getPixelResolution( d );

		// get unwarped slab position in physical units
		final double[] unwarpedSlabPositionPhysicalUnits = new double[ globalPositionPixels.length ];
		slabTransform.applyInverse( unwarpedSlabPositionPhysicalUnits, warpedGlobalPositionPhysicalUnits );

		// get unwarped slab position in pixels
		final double[] unwarpedSlabPositionPixels = new double[ globalPositionPixels.length ];
		for ( int d = 0; d < unwarpedSlabPositionPixels.length; ++d )
			unwarpedSlabPositionPixels[ d ] = unwarpedSlabPositionPhysicalUnits[ d ] / tile.getPixelResolution( d );

		// get unwarped local (tile) position in pixels
		final double[] unwarpedLocalPositionPixels = new double[ globalPositionPixels.length ];
		for ( int d = 0; d < unwarpedLocalPositionPixels.length; ++d )
			unwarpedLocalPositionPixels[ d ] = unwarpedSlabPositionPixels[ d ] - tile.getPosition( d );

		// check that the point is within tile
		boolean insideTile = true;
		for ( int d = 0; d < unwarpedLocalPositionPixels.length; ++d )
			insideTile &= unwarpedLocalPositionPixels[ d ] >= 0 && unwarpedLocalPositionPixels[ d ] <= tile.getSize( d ) - 1;

		return insideTile ? unwarpedLocalPositionPixels : null;
	}
}
