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
import org.apache.spark.broadcast.Broadcast;
import org.janelia.flatfield.FlatfieldCorrection;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadataWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.N5DownsamplingSpark;
import org.janelia.util.Conversions;

import bdv.img.TpsTransformWrapper;
import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.Interval;
import net.imglib2.SerializableFinalInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;
import net.imglib2.view.RandomAccessiblePairNullable;
import scala.Tuple2;

public class WarpedExporter< T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > implements Serializable
{
	private static final long serialVersionUID = -3670611071620152571L;

	private static final int MAX_PARTITIONS = 15000;

	private transient final JavaSparkContext sparkContext;
	private final List< Map< String, TileInfo[] > > slabsTilesChannels;
	private final Map< String, double[] > slabsMin;
	private final Map< String, TpsTransformWrapper > slabsTransforms;
	private final double[] voxelDimensions;
	private final int outputCellSize;
	private final String outputPath;
	private final String flatfieldPath;
	private final Set< Integer > filteredTiles;

	double[] normalizedVoxelDimensions;

	Broadcast< RandomAccessiblePairNullable< U, U > > broadcastedFlatfieldCorrection;

	transient Interval subinterval = null;

	public WarpedExporter(
			final JavaSparkContext sparkContext,
			final List< Map< String, TileInfo[] > > slabsTilesChannels,
			final Map< String, double[] > slabsMin,
			final Map< String, TpsTransformWrapper > slabsTransforms,
			final double[] voxelDimensions,
			final int outputCellSize,
			final String outputPath,
			final String flatfieldPath,
			final Set< Integer > filteredTiles )
	{
		this.sparkContext = sparkContext;
		this.slabsTilesChannels = slabsTilesChannels;
		this.slabsMin = slabsMin;
		this.slabsTransforms = slabsTransforms;
		this.voxelDimensions = voxelDimensions;
		this.outputCellSize = outputCellSize;
		this.outputPath = outputPath;
		this.flatfieldPath = flatfieldPath;
		this.filteredTiles = filteredTiles;
	}

	public void setSubInterval( final Interval subinterval )
	{
		this.subinterval = subinterval;
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
		final String baseExportPath = Paths.get( outputPath, "export.n5" ).toString();
		final N5Writer n5 = N5.openFSWriter( baseExportPath );

		int[][] downsamplingFactors = null;

		normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( voxelDimensions );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );

		// loop over channels
		for ( int channel = 0; channel < slabsTilesChannels.size(); channel++ )
		{
			System.out.println( "Processing channel #" + channel );

			final String channelGroupPath = "c" + channel;
			n5.createGroup( channelGroupPath );
			final String fullScaleOutputPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, 0 );

			// Generate export of the first scale level
			if ( !n5.datasetExists( fullScaleOutputPath ) )
			{
				// prepare flatfield correction images
				// use it as a folder with the input file's name
				if ( flatfieldPath != null )
				{
					final RandomAccessiblePairNullable< U, U >  flatfieldCorrection = FlatfieldCorrection.loadCorrectionImages(
							Paths.get( flatfieldPath, "ch" + channel + "-flatfield", "S.tif" ).toString(),
							Paths.get( flatfieldPath, "ch" + channel + "-flatfield", "T.tif" ).toString(),
							C1WarpedMetadata.NUM_DIMENSIONS
						);
					System.out.println( "[Flatfield correction] Broadcasting flatfield correction images" );
					broadcastedFlatfieldCorrection = sparkContext.broadcast( flatfieldCorrection );
				}
				else
				{
					broadcastedFlatfieldCorrection = sparkContext.broadcast( null );
				}

				fuseWarp( baseExportPath, fullScaleOutputPath, slabsTilesChannels.get( channel ) );

				broadcastedFlatfieldCorrection.destroy();
			}

			// Generate lower scale levels
			if ( !n5.datasetExists( N5ExportMetadata.getScaleLevelDatasetPath( channel, 1 ) ) )
				downsamplingFactors = N5DownsamplingSpark.downsampleIsotropic(
						sparkContext,
						() -> N5.openFSWriter( baseExportPath ),
						fullScaleOutputPath,
						new FinalVoxelDimensions( "um", voxelDimensions )
					);
		}

		System.out.println( "All channels have been exported" );

		final double[][] scalesDouble = new double[ downsamplingFactors.length ][];
		for ( int s = 0; s < downsamplingFactors.length; ++s )
			scalesDouble[ s ] = Conversions.toDoubleArray( downsamplingFactors[ s ] );

		final N5ExportMetadataWriter exportMetadata = N5ExportMetadata.openForWriting( N5.openFSWriter( baseExportPath ) );
		exportMetadata.setDefaultScales( scalesDouble );
		exportMetadata.setDefaultPixelResolution( new FinalVoxelDimensions( "um", voxelDimensions ) );
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

	private void fuseWarp( final String baseExportPath, final String fullScaleOutputPath, final Map< String, TileInfo[] > slabTiles ) throws IOException
	{
		final int[] cellSize = getOptimalCellSize();

		final Map< Integer, Interval > tileWarpedBoundingBoxes = getTilesWarpedBoundingBoxes( slabTiles );

		final Map< Integer, TileInfo > tilesMap = new HashMap<>();
		final Map< Integer, String > slabsMap = new HashMap<>();
		for ( final Entry< String, TileInfo[] > entry : slabTiles.entrySet() )
		{
			for ( final TileInfo tile : entry.getValue() )
			{
				tilesMap.put( tile.getIndex(), tile );
				slabsMap.put( tile.getIndex(), entry.getKey() );
			}
		}

		final Interval fullBoundingBox = TileOperations.getCollectionBoundaries( tileWarpedBoundingBoxes.values().toArray( new Interval[ 0 ] ) );

		final Map< Integer, Interval > filteredTilesWarpedBoundingBoxes;
		final Interval filteredBoundingBox;
		if ( filteredTiles != null )
		{
			filteredTilesWarpedBoundingBoxes = new HashMap<>();
			for ( final Integer filteredTileIndex : filteredTiles )
				filteredTilesWarpedBoundingBoxes.put( filteredTileIndex, tileWarpedBoundingBoxes.get( filteredTileIndex ) );
			filteredBoundingBox = TileOperations.getCollectionBoundaries( filteredTilesWarpedBoundingBoxes.values().toArray( new Interval[ 0 ] ) );
		}
		else
		{
			filteredTilesWarpedBoundingBoxes = tileWarpedBoundingBoxes;
			filteredBoundingBox = fullBoundingBox;
		}

		final long[] offset = Intervals.minAsLongArray( filteredBoundingBox );
		final Interval boundingBox = subinterval != null ? IntervalsHelper.translate( subinterval, offset ) : filteredBoundingBox;
		final long[] dimensions = Intervals.dimensionsAsLongArray( boundingBox );

		// use the size of the tile as a bigger cell to minimize the number of loads for each image
//		final int[] biggerCellSize = new int[ cellSize.length ];
//		for ( int d = 0; d < biggerCellSize.length; ++d )
//			biggerCellSize[ d ] = cellSize[ d ] * ( int ) Math.ceil( ( double ) new ArrayList<>( tileWarpedBoundingBoxes.values() ).get( 0 ).dimension( d ) / cellSize[ d ] );
		final int[] biggerCellSize = new int[] { cellSize[ 0 ] * 2, cellSize[ 1 ] * 2, cellSize[ 2 ] * 4 };

		final List< TileInfo > biggerCells = TileOperations.divideSpace( boundingBox, new FinalDimensions( biggerCellSize ) );

		N5.openFSWriter( baseExportPath ).createDataset( fullScaleOutputPath, Intervals.dimensionsAsLongArray( boundingBox ), cellSize, getN5DataType( new ArrayList<>( slabTiles.values() ).get( 0 )[ 0 ].getType() ), CompressionType.GZIP );

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
				final Set< Integer > tileIndexesWithinCell = TileOperations.findTilesWithinSubregion( filteredTilesWarpedBoundingBoxes, biggerCellBox );
				if ( tileIndexesWithinCell.isEmpty() )
					return;

				final List< TileInfo > tilesWithinCell = new ArrayList<>();
				for ( final Integer tileIndex : tileIndexesWithinCell )
					tilesWithinCell.add( tilesMap.get( tileIndex ) );

				final ImagePlusImg< T, ? > outImg = FusionPerformer.fuseWarpedTilesWithinCellUsingMaxMinDistance(
						biggerCellBox,
						tilesWithinCell,
						slabsMap,
						slabsMin,
						slabsTransforms,
						broadcastedFlatfieldCorrection.value()
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
				N5Utils.saveBlock( outImg, n5Local, fullScaleOutputPath, biggerCellGridOffsetPosition );
			}
		);
	}

	private DataType getN5DataType( final ImageType imageType )
	{
		switch ( imageType )
		{
		case GRAY8:
			return DataType.UINT8;
		case GRAY16:
			return DataType.UINT16;
		case GRAY32:
			return DataType.FLOAT32;
		default:
			return null;
		}
	}
}
