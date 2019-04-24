package org.janelia.stitching;

import java.io.IOException;
import java.net.URI;
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
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.flatfield.FlatfieldCorrection;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadataWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5NonIsotropicScalePyramidSpark;
import org.janelia.stitching.FusionPerformer.FusionMode;
import org.janelia.stitching.TileLoader.TileType;
import org.janelia.util.Conversions;

import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
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

	private static final int MIN_BLOCK_SIZE = 64;

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
		final DataProviderType dataProviderType = dataProvider.getType();

		// check if stage configuration is supplied and if it was intended
		for ( final String inputFilePath : job.getArgs().inputTileConfigurations() )
			if ( !inputFilePath.substring( 0, inputFilePath.lastIndexOf( '.' ) ).endsWith( "-final" ) && !job.getArgs().allowFusingStage() )
				throw new RuntimeException( "The filename of the input configuration indicates that stitching has not been performed. If you intend to export the initial (stage) configuration, supply an additional parameter '--fusestage'." );

		// determine the best location for storing the export files (near the tile configurations by default)
		// for cloud backends, the export is stored in a separate bucket to be compatible with n5-viewer
		String baseExportPath = null;
		if ( dataProviderType == DataProviderType.FILESYSTEM )
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
			baseExportPath = DataProviderFactory.createBucketUri( dataProviderType, exportBucket ).toString();
		}

		final String n5ExportPath = baseExportPath;

		if ( dataProvider.createN5Reader( n5ExportPath ).exists( "/" ) )
		{
			throw new PipelineExecutionException( "Export path already exists: " + n5ExportPath + System.lineSeparator() +
					"Aborting to prevent possible overwriting of useful data. Please make sure everything is correct, and in case it was intended, delete the existing export first and run it again." );
		}

		final N5Writer n5 = dataProvider.createN5Writer( n5ExportPath, N5ExportMetadata.getGsonBuilder() );

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
			final String absoluteChannelPathNoFinal = Utils.removeFilenameSuffix( absoluteChannelPath, "-final" ); // adjust the path in order to use original flatfields

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
					absoluteChannelPathNoFinal,
					job.getDimensionality()
				);
			if ( flatfieldCorrection != null )
				System.out.println( "[Flatfield correction] Broadcasting flatfield correction images" );
			broadcastedFlatfieldCorrection = sparkContext.broadcast( flatfieldCorrection );

			final String fullScaleOutputPath = N5ExportMetadata.getScaleLevelDatasetPath( channel, 0 );

			// Generate export of the first scale level
			fuse( n5ExportPath, fullScaleOutputPath, job.getTiles( channel ) );

			// Generate lower scale levels
			downsampledDatasets = N5NonIsotropicScalePyramidSpark.downsampleNonIsotropicScalePyramid(
					sparkContext,
					() -> DataProviderFactory.create( dataProviderType ).createN5Writer( n5ExportPath ),
					fullScaleOutputPath,
					voxelDimensions,
					false // not a power of two scale pyramid
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

	private boolean isTileConfigurationN5( final TileInfo[] tiles )
	{
		return TileLoader.getTileType( tiles[ 0 ], job.getDataProvider() ) == TileType.N5_DATASET;
	}

	private int[] getOptimalCellSize( final TileInfo[] tiles ) throws IOException
	{
		final int[] tileN5BlockSize = isTileConfigurationN5( tiles ) ? TileLoader.getTileN5DatasetAttributes( tiles[ 0 ], job.getDataProvider() ).getBlockSize() : null;
		final int[] cellSize = new int[ job.getDimensionality() ];
		for ( int d = 0; d < job.getDimensionality(); d++ )
		{
			cellSize[ d ] = Math.max(
					( int ) Math.round( job.getArgs().fusionCellSize() / normalizedVoxelDimensions[ d ] ),
					tileN5BlockSize != null ? tileN5BlockSize[ d ] : MIN_BLOCK_SIZE // set the output block size to be not smaller than input tile block size (if stored as N5)
				);
		}
		return cellSize;
	}

	private int[] getProcessingCellSize( final TileInfo[] tiles ) throws IOException
	{
		final int[] processingCellSize = getOptimalCellSize( tiles );
		if ( !isTileConfigurationN5( tiles ) )
		{
			// When stored as .tif, full tile image will be loaded each time. Reduce number of reads by increasing processing block size
			final int increaseFactor = 2;
			for ( int d = 0; d < processingCellSize.length; ++d )
				processingCellSize[ d ] *= increaseFactor * Math.round( normalizedVoxelDimensions[ d ] );
		}
		return processingCellSize;
	}

	private void fuse( final String n5ExportPath, final String fullScaleOutputPath, final TileInfo[] tiles ) throws IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		final int[] cellSize = getOptimalCellSize( tiles );

		final Boundaries boundingBox;
		if ( job.getArgs().minCoord() != null && job.getArgs().maxCoord() != null )
			boundingBox = new Boundaries( job.getArgs().minCoord(), job.getArgs().maxCoord() );
		else
			boundingBox = TileOperations.getCollectionBoundaries( tiles );

		final long[] offset = Intervals.minAsLongArray( boundingBox );
		final long[] dimensions = Intervals.dimensionsAsLongArray( boundingBox );

		final N5Writer n5 = dataProvider.createN5Writer( n5ExportPath );
		n5.createDataset(
				fullScaleOutputPath,
				Intervals.dimensionsAsLongArray( boundingBox ),
				cellSize,
				N5Utils.dataType( ( T ) tiles[ 0 ].getType().getType() ),
				new GzipCompression()
			);

		final int[] processingCellSize = getProcessingCellSize( tiles );
		System.out.println( "Fusing tile configuration into an N5 dataset with block size " + Arrays.toString( cellSize ) + " using processing block size " + Arrays.toString( processingCellSize ) );
		final List< TileInfo > processingCells = TileOperations.divideSpace( boundingBox, new FinalDimensions( processingCellSize ) );

		sparkContext.parallelize( processingCells, Math.min( processingCells.size(), MAX_PARTITIONS ) ).foreach( cell ->
			{
				final List< TileInfo > tilesWithinCell = TileOperations.findTilesWithinSubregion( tiles, cell );
				if ( tilesWithinCell.isEmpty() )
					return;

				final Boundaries cellBox = cell.getBoundaries();
				final long[] cellOffsetCoordinates = new long[ cellBox.numDimensions() ];
				for ( int d = 0; d < cellOffsetCoordinates.length; d++ )
					cellOffsetCoordinates[ d ] = cellBox.min( d ) - offset[ d ];

				final long[] cellGridPosition = new long[ cell.numDimensions() ];
				final CellGrid cellGrid = new CellGrid( dimensions, cellSize );
				cellGrid.getCellPosition( cellOffsetCoordinates, cellGridPosition );

				final DataProvider dataProviderLocal = job.getDataProvider();
				final ImagePlusImg< T, ? > outImg = FusionPerformer.fuseTilesWithinCell(
						dataProviderLocal,
						job.getArgs().blending() ? FusionMode.BLENDING : FusionMode.MAX_MIN_DISTANCE,
						tilesWithinCell,
						cellBox,
						broadcastedFlatfieldCorrection.value(),
						broadcastedPairwiseConnectionsMap.value()
					);
				final N5Writer n5Local = dataProviderLocal.createN5Writer( n5ExportPath );
				N5Utils.saveBlock( outImg, n5Local, fullScaleOutputPath, cellGridPosition );
			}
		);
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
			final List< SerializablePairWiseStitchingResult[] > pairwiseShifts = TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( pairwiseShiftsPath ) );
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
