package org.janelia.stitching;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.CloudURI;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.flatfield.FlatfieldCorrection;
import org.janelia.fusion.FusionPerformer;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadataWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5NonIsotropicScalePyramidSpark3D;
import org.janelia.util.Conversions;

import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;
import net.imglib2.view.RandomAccessiblePairNullable;

/**
 * Fuses a set of tiles within a set of small square cells using linear blending.
 * Saves fused tile configuration on the disk.
 *
 * @author Igor Pisarev
 */

public class PipelineFusionStepExecutor< T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > extends PipelineStepExecutor
{
	private static final long serialVersionUID = -8151178964876747760L;

	private static final int MAX_PARTITIONS = 15000;

	final TreeMap< Integer, long[] > levelToImageDimensions = new TreeMap<>(), levelToCellSize = new TreeMap<>();

	double[] normalizedVoxelDimensions;

	Broadcast< Map< Integer, Set< Integer > > > broadcastedPairwiseConnectionsMap;
	Broadcast< RandomAccessiblePairNullable< U, U > > broadcastedFlatfieldCorrection;

	public PipelineFusionStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run() throws PipelineExecutionException
	{
		try {
			runImpl();
		} catch (final IOException e) {
			e.printStackTrace();
			throw new PipelineExecutionException( e.getMessage(), e.getCause() );
		}
	}

	// TODO: add comprehensive support for 4D images where multiple channels are encoded as the 4th dimension
	private void runImpl() throws PipelineExecutionException, IOException
	{
		for ( int channel = 0; channel < job.getChannels(); channel++ )
			TileOperations.translateTilesToOriginReal( job.getTiles( channel ) );

		final String overlapsPathSuffix = job.getArgs().exportOverlaps() ? "-overlaps" : "";

		final DataProvider dataProvider = job.getDataProvider();
		final DataAccessType dataAccessType = dataProvider.getType();

		// determine the best location for storing the export files (near the tile configurations by default)
		// for cloud backends, the export is stored in a separate bucket to be compatible with n5-viewer
		String baseExportPath = null;
		if ( dataAccessType == DataAccessType.FILESYSTEM )
		{
			for ( final String inputFilePath : job.getArgs().inputTileConfigurations() )
			{
				final String inputFolderPath = PathResolver.getParent( inputFilePath );
				if ( baseExportPath == null )
				{
					baseExportPath = inputFolderPath;
				}
				else if ( !baseExportPath.equals( inputFolderPath ) )
				{
					// go one level upper since channels are stored in individual subfolders
					baseExportPath = PathResolver.getParent( inputFolderPath );
					break;
				}
			}
			baseExportPath = PathResolver.get( baseExportPath, "export.n5" + overlapsPathSuffix );
		}
		else
		{
			for ( final String inputFilePath : job.getArgs().inputTileConfigurations() )
			{
				final String bucket = new CloudURI( URI.create( inputFilePath ) ).getBucket();
				if ( baseExportPath == null )
				{
					baseExportPath = bucket;
				}
				else if ( !baseExportPath.equals( bucket ) )
				{
					throw new PipelineExecutionException( "cannot generate export bucket name" );
				}
			}
			final String exportBucket = baseExportPath + "-export-n5" + overlapsPathSuffix;
			baseExportPath = DataProviderFactory.createBucketUri( dataAccessType, exportBucket ).toString();
		}

		// FIXME: allow it for now
//		if ( Files.exists( Paths.get( baseExportPath ) ) )
//			throw new PipelineExecutionException( "Export path already exists: " + baseExportPath );

		final String n5ExportPath = baseExportPath;
		final N5Writer n5 = dataProvider.createN5Writer( URI.create( n5ExportPath ), N5ExportMetadata.getGsonBuilder() );

		List< String > downsampledDatasets = null;

		final double[] voxelDimensions = job.getPixelResolution();
		normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( voxelDimensions );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );

		// loop over channels
		for ( int ch = 0; ch < job.getChannels(); ch++ )
		{
			final int channel = ch;
			System.out.println( "Processing channel #" + channel );

			final String absoluteChannelPath = job.getArgs().inputTileConfigurations().get( channel );
			final String absoluteChannelPathNoExt = absoluteChannelPath.lastIndexOf( '.' ) != -1 ? absoluteChannelPath.substring( 0, absoluteChannelPath.lastIndexOf( '.' ) ) : absoluteChannelPath;

			n5.createGroup( N5ExportMetadata.getChannelGroupPath( channel ) );

			// special mode which allows to export only overlaps of tile pairs that have been used for final stitching
			final Map< Integer, Set< Integer > > pairwiseConnectionsMap = getPairwiseConnectionsMap( absoluteChannelPath );
			if ( pairwiseConnectionsMap != null )
				System.out.println( "[Export overlaps mode] Broadcasting pairwise connections map" );
			broadcastedPairwiseConnectionsMap = sparkContext.broadcast( pairwiseConnectionsMap );

			// prepare flatfield correction images
			// use it as a folder with the input file's name
			final RandomAccessiblePairNullable< U, U >  flatfieldCorrection = FlatfieldCorrection.loadCorrectionImages(
					dataProvider,
					absoluteChannelPathNoExt,
					job.getDimensionality()
				);
			if ( flatfieldCorrection != null )
				System.out.println( "[Flatfield correction] Broadcasting flatfield correction images" );
			broadcastedFlatfieldCorrection = sparkContext.broadcast( flatfieldCorrection );

			final String fullScaleOutputPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, 0 );

			// Generate export of the first scale level
			// FIXME: remove. But it saves time for now
			if ( !n5.datasetExists( fullScaleOutputPath ) )
				fuse( n5ExportPath, fullScaleOutputPath, job.getTiles( channel ) );

			// Generate lower scale levels
			// FIXME: remove. But it saves time for now
			if ( !n5.datasetExists( N5ExportMetadata.getScaleLevelDatasetPath( channel, 1 ) ) )
				downsampledDatasets = N5NonIsotropicScalePyramidSpark3D.downsampleNonIsotropicScalePyramid(
						sparkContext,
						() -> DataProviderFactory.createByType( dataAccessType ).createN5Writer( URI.create( n5ExportPath ) ),
						fullScaleOutputPath,
						voxelDimensions
					);

			broadcastedPairwiseConnectionsMap.destroy();
			broadcastedFlatfieldCorrection.destroy();
		}

		System.out.println( "All channels have been exported" );

		// TODO: remove and make n5-viewer to look for downsampling factors attributes by itself
		final double[][] scalesDouble = new double[ downsampledDatasets.size() + 1 ][];
		scalesDouble[ 0 ] = new double[ job.getDimensionality() ];
		Arrays.fill( scalesDouble[ 0 ], 1 );
		for ( int s = 0; s < downsampledDatasets.size(); ++s )
			scalesDouble[ s + 1 ] = Conversions.toDoubleArray( n5.getAttribute( downsampledDatasets.get( s ), "downsamplingFactors", int[].class ) );

		final N5ExportMetadataWriter exportMetadata = N5ExportMetadata.openForWriting( n5 );
		exportMetadata.setDefaultScales( scalesDouble );
		exportMetadata.setDefaultPixelResolution( new FinalVoxelDimensions( "um", voxelDimensions ) );
	}


	private int[] getOptimalCellSize()
	{
		final int[] cellSize = new int[ job.getDimensionality() ];
		for ( int d = 0; d < job.getDimensionality(); d++ )
			cellSize[ d ] = ( int ) Math.round( job.getArgs().fusionCellSize() / normalizedVoxelDimensions[ d ] );
		return cellSize;
	}

	private void fuse( final String n5ExportPath, final String fullScaleOutputPath, final TileInfo[] tiles ) throws IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final int[] cellSize = getOptimalCellSize();

		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( tiles );
		final Map< Integer, Interval > transformedTilesBoundingBoxes = new HashMap<>();
		for ( final TileInfo tile : tiles )
			transformedTilesBoundingBoxes.put( tile.getIndex(), TileOperations.estimateBoundingBox( tile ) );

		final Interval fullBoundingBox = TileOperations.getCollectionBoundaries( transformedTilesBoundingBoxes.values() );
		final long[] offset = Intervals.minAsLongArray( fullBoundingBox );
		final Interval roi = ( job.getArgs().minCoord() != null && job.getArgs().maxCoord() != null ) ? new FinalInterval( job.getArgs().minCoord(), job.getArgs().maxCoord() ) : null;
		final Interval boundingBox = roi != null ? IntervalsHelper.translate( roi, offset ) : fullBoundingBox;
		final long[] dimensions = Intervals.dimensionsAsLongArray( boundingBox );

		final N5Writer n5 = dataProvider.createN5Writer( URI.create( n5ExportPath ) );
		n5.createDataset(
				fullScaleOutputPath,
				dimensions,
				cellSize,
				N5Utils.dataType( ( T ) tiles[ 0 ].getType().getType() ),
				new GzipCompression()
			);

		// use the size of the tile as a bigger cell to minimize the number of loads for each image
		final int[] biggerCellSize = new int[ cellSize.length ];
		for ( int d = 0; d < biggerCellSize.length; ++d )
			biggerCellSize[ d ] = cellSize[ d ] * ( int ) Math.ceil( ( double ) tiles[ 0 ].getSize( d ) / cellSize[ d ] );

		final List< TileInfo > biggerCells = TileOperations.divideSpace( boundingBox, new FinalDimensions( biggerCellSize ) );

		// get cell grid coordinates to know the offset
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
				}
			).collect();
		final long[] gridOffset = new long[ biggerCellSize.length ];
		Arrays.fill( gridOffset, Long.MAX_VALUE );
		for ( final long[] gridCoord : gridCoords )
			for ( int d = 0; d < gridOffset.length; ++d )
				gridOffset[ d ] = Math.min( gridCoord[ d ], gridOffset[ d ] );

		System.out.println();
		System.out.println( "Adjusted intermediate cell size to " + Arrays.toString( biggerCellSize ) + " (for faster processing)" );
		System.out.println( "Grid offset = " + Arrays.toString( gridOffset ) );
		System.out.println();

		final Broadcast< Map< Integer, TileInfo > > broadcastedTilesMap = sparkContext.broadcast( tilesMap );
		final Broadcast< Map< Integer, Interval > > broadcastedTransformedTilesBoundingBoxes = sparkContext.broadcast( transformedTilesBoundingBoxes );

		sparkContext.parallelize( biggerCells, Math.min( biggerCells.size(), MAX_PARTITIONS ) ).foreach( biggerCell ->
			{
				final Boundaries biggerCellBox = biggerCell.getBoundaries();
				final Set< Integer > tileIndexesWithinCell = TileOperations.findTilesWithinSubregion( broadcastedTransformedTilesBoundingBoxes.value(), biggerCellBox );
				if ( tileIndexesWithinCell.isEmpty() )
					return;

				final List< TileInfo > tilesWithinCell = new ArrayList<>();
				for ( final Integer tileIndex : tileIndexesWithinCell )
					tilesWithinCell.add( broadcastedTilesMap.value().get( tileIndex ) );

				final long[] biggerCellOffsetCoordinates = new long[ biggerCellBox.numDimensions() ];
				for ( int d = 0; d < biggerCellOffsetCoordinates.length; d++ )
					biggerCellOffsetCoordinates[ d ] = biggerCellBox.min( d ) - offset[ d ];

				final long[] biggerCellGridPosition = new long[ biggerCell.numDimensions() ];
				final CellGrid cellGrid = new CellGrid( dimensions, cellSize );
				cellGrid.getCellPosition( biggerCellOffsetCoordinates, biggerCellGridPosition );

				final long[] biggerCellGridOffsetPosition = new long[ biggerCellGridPosition.length ];
				for ( int d = 0; d < gridOffset.length; ++d )
					biggerCellGridOffsetPosition[ d ] = biggerCellGridPosition[ d ] - gridOffset[ d ];

				final DataProvider dataProviderLocal = job.getDataProvider();
				final ImagePlusImg< T, ? > outImg = FusionPerformer.fuseTilesWithinCell(
						dataProviderLocal,
						job.getArgs().fusionMode(),
						tilesWithinCell,
						biggerCellBox,
						broadcastedFlatfieldCorrection.value(),
						broadcastedPairwiseConnectionsMap.value()
					);
				final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( n5ExportPath ) );
				N5Utils.saveBlock( outImg, n5Local, fullScaleOutputPath, biggerCellGridOffsetPosition );
			}
		);

		broadcastedTilesMap.destroy();
		broadcastedTransformedTilesBoundingBoxes.destroy();
	}

	private Map< Integer, Set< Integer > > getPairwiseConnectionsMap( final String channelPath ) throws PipelineExecutionException
	{
		if ( !job.getArgs().exportOverlaps() )
			return null;

		final DataProvider dataProvider = job.getDataProvider();
		final Map< Integer, Set< Integer > > pairwiseConnectionsMap = new HashMap<>();
		try
		{
			final String pairwiseShiftsPath = PathResolver.get( PathResolver.getParent( channelPath ), "pairwise-stitched.json" );
			final List< SerializablePairWiseStitchingResult[] > pairwiseShifts = TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( URI.create( pairwiseShiftsPath ) ) );
			for ( final SerializablePairWiseStitchingResult[] pairwiseShiftMulti : pairwiseShifts )
			{
				final SerializablePairWiseStitchingResult pairwiseShift = pairwiseShiftMulti[ 0 ];
				if ( pairwiseShift.getIsValidOverlap() )
				{
					final TileInfo[] pairArr = pairwiseShift.getTilePair().toArray();
					for ( int i = 0; i < 2; ++i )
					{
						if ( !pairwiseConnectionsMap.containsKey( pairArr[ i ].getIndex() ) )
							pairwiseConnectionsMap.put( pairArr[ i ].getIndex(), new HashSet<>() );
						pairwiseConnectionsMap.get( pairArr[ i ].getIndex() ).add( pairArr[ ( i + 1 ) % 2 ].getIndex() );
					}
				}
			}
		}
		catch ( final IOException e )
		{
			throw new PipelineExecutionException( "--overlaps mode is requested but pairwise-stitched.json file is not available", e );
		}

		return pairwiseConnectionsMap;
	}
}
