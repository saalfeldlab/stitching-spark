package org.janelia.stitching;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.bdv.fusion.CellFileImageMetaData;
import org.janelia.flatfield.FlatfieldCorrection;

import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
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

public class PipelineFusionStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -8151178964876747760L;

	final TreeMap< Integer, long[] > levelToImageDimensions = new TreeMap<>(), levelToCellSize = new TreeMap<>();

	public PipelineFusionStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run() throws PipelineExecutionException
	{
		runImpl();
	}
	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void runImpl() throws PipelineExecutionException
	{
		for ( int channel = 0; channel < job.getChannels(); channel++ )
			TileOperations.translateTilesToOriginReal( job.getTiles( channel ) );

		// TODO: add comprehensive support for 4D images where multiple channels are encoded as the 4th dimension
		/*final ImagePlus testImp = ImageImporter.openImage( job.getTiles()[ 0 ].getFilePath() );
		Utils.workaroundImagePlusNSlices( testImp );
		final int channels = testImp.getNChannels();
		testImp.close();*/

		final VoxelDimensions voxelDimensions = job.getArgs().voxelDimensions();
		final double[] normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( voxelDimensions );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );

		final List< CellFileImageMetaData > exports = new ArrayList<>();
		final String overlapsPathSuffix = job.getArgs().exportOverlaps() ? "-overlaps" : "";

		for ( int ch = 0; ch < job.getChannels(); ch++ )
		{
			final int channel = ch;

			// special mode which allows to export only overlaps of tile pairs that have been used for final stitching
			final Map< Integer, Set< Integer > > pairwiseConnectionsMap;
			if ( job.getArgs().exportOverlaps() )
			{
				pairwiseConnectionsMap = new HashMap<>();
				try
				{
					final List< SerializablePairWiseStitchingResult > pairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( Utils.addFilenameSuffix( job.getArgs().inputTileConfigurations().get( channel ), "_pairwise" ) );
					for ( final SerializablePairWiseStitchingResult pairwiseShift : pairwiseShifts )
					{
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
					throw new PipelineExecutionException( "--overlaps mode is requested but the pairwise shifts file is not available", e );
				}
			}
			else
			{
				 pairwiseConnectionsMap = null;
			}
			if ( pairwiseConnectionsMap != null )
				System.out.println( "[Export overlaps mode] Broadcasting pairwise connections map" );
			final Broadcast< Map< Integer, Set< Integer > > > broadcastedPairwiseConnectionsMap = sparkContext.broadcast( pairwiseConnectionsMap );

			// prepare flatfield correction images
			final String flatfieldCorrectionBasePath = Paths.get( job.getArgs().inputTileConfigurations().get( channel ) ).getParent().toString();
			final RandomAccessiblePairNullable< U, U >  flatfieldCorrection = FlatfieldCorrection.loadCorrectionImages(
					flatfieldCorrectionBasePath + "/v.tif",
					flatfieldCorrectionBasePath + "/z.tif"
				);
			if ( flatfieldCorrection != null )
				System.out.println( "[Flatfield correction] Broadcasting flatfield correction images" );
			final Broadcast< RandomAccessiblePairNullable< U, U > > broadcastedFlatfieldCorrection = sparkContext.broadcast( flatfieldCorrection );

			System.out.println( "Processing channel #" + channel );
			final String baseOutputFolder = job.getBaseFolder() + "/channel" + channel + overlapsPathSuffix;
			final String exportFolder = baseOutputFolder + "/fused";

			int level = 0;
			String lastLevelTmpPath = null;
			TileInfo[] lastLevelCells = job.getTiles( channel );

			final TreeMap< Integer, int[] > levelToDownsampleFactors = new TreeMap<>(), levelToCellSize = new TreeMap<>();
			long minDimension = 0, maxDimension = 0;
			do
			{
				final int[] fullDownsampleFactors = new int[ job.getDimensionality() ];
				final int[] downsampleFactors = new int[ job.getDimensionality() ];
				final int[] singleCellSize = new int[ job.getDimensionality() ];
				final int[] cellSize = new int[ job.getDimensionality() ];
				final int[] upscaledCellSize = new int[ job.getDimensionality() ];
				for ( int d = 0; d < job.getDimensionality(); d++ )
				{
					final int isotropicScaling = ( int ) Math.round( ( 1 << level ) / normalizedVoxelDimensions[ d ] );
					singleCellSize[ d ] = ( int ) Math.round( job.getArgs().fusionCellSize() / normalizedVoxelDimensions[ d ] );
					fullDownsampleFactors[ d ] = Math.max( isotropicScaling, 1 );
					downsampleFactors[ d ] = ( d == 2 ? fullDownsampleFactors[ d ] : ( level == 0 ? 1 : 2 ) );
					cellSize[ d ] = singleCellSize[ d ] * ( d == 2 ? ( 1 << level ) / downsampleFactors[ d ] : 1 );
					upscaledCellSize[ d ] = downsampleFactors[ d ] * cellSize[ d ];
				}

				System.out.println( "Processing level " + level + ", fullDownsamplingFactors=" + Arrays.toString(fullDownsampleFactors)+", fromTmpStep="+Arrays.toString(downsampleFactors));
				System.out.println( "cell size set to " + Arrays.toString(cellSize) +",  upscaled target cell size: " + Arrays.toString(upscaledCellSize) );

				final int currLevel = level;
				final String levelFolder = exportFolder + "/" + level;

				final String levelConfigurationOutputPath = baseOutputFolder + "/" + Utils.addFilenameSuffix( Paths.get( job.getArgs().inputTileConfigurations().get( channel ) ).getFileName().toString(), "-scale" + level );

				if ( Files.exists( Paths.get( levelConfigurationOutputPath ) ) )
				{	// load current scale level if exists
					try
					{
						lastLevelCells = TileInfoJSONProvider.loadTilesConfiguration( levelConfigurationOutputPath );
					}
					catch ( final IOException e )
					{
						throw new PipelineExecutionException( e.getMessage() );
					}

					if ( job.getDimensionality() < 3 || downsampleFactors[ 2 ] == 1 )
						lastLevelTmpPath = levelConfigurationOutputPath;
					else
						lastLevelTmpPath = Utils.addFilenameSuffix( levelConfigurationOutputPath, "-xy" );

					System.out.println( "Loaded configuration file for level " + level );
				}
				else
				{	// otherwise generate it
					System.out.println( "Output configuration file doesn't exist for level " + level + ", generating..." );

					final String currLevelTmpPath;
					final TileInfo[] smallerCells;

					if ( job.getDimensionality() < 3 || downsampleFactors[ 2 ] == 1 )
					{	// use previous scale level as downsampled in XY if zFactor is still 1
						currLevelTmpPath = levelConfigurationOutputPath;
						smallerCells = lastLevelCells;
					}
					else
					{	// otherwise load last precomputed tmp level
						currLevelTmpPath = Utils.addFilenameSuffix( levelConfigurationOutputPath, "-xy" );
						final String levelTmpFolder = levelFolder + "-xy";

						final TileInfo[] lastLevelTmpCells;
						if ( Files.exists( Paths.get( currLevelTmpPath ) ) )
						{	// check if downsampled in XY images for the current scale are already exported
							try
							{
								smallerCells = TileInfoJSONProvider.loadTilesConfiguration( currLevelTmpPath );
								System.out.println( "Loaded precomputed tmp images for current scale" );
							}
							catch ( final IOException e )
							{
								throw new PipelineExecutionException( e.getMessage() );
							}
						}
						else
						{	// generate tmp images for current scale
							try
							{
								lastLevelTmpCells = TileInfoJSONProvider.loadTilesConfiguration( lastLevelTmpPath );
							}
							catch ( final IOException e )
							{
								throw new PipelineExecutionException( e.getMessage() );
							}

							final int[] tmpDownsampleFactors = new int[] { 2, 2, 1 };
							final int[] lastLevelTmpCellSize = levelToCellSize.get( level - 1 );
							final int[] upscaledTmpCellSize = lastLevelTmpCellSize.clone();
							for ( int d = 0; d < singleCellSize.length; d++ )
								upscaledTmpCellSize[ d ] *= tmpDownsampleFactors[ d ];

							final Boundaries tmpSpace = TileOperations.getCollectionBoundaries( lastLevelTmpCells );
							final List< TileInfo > tmpNewCells = TileOperations.divideSpace( tmpSpace, new FinalDimensions( upscaledTmpCellSize ) );

							System.out.println( " --- Precomputing cells downsampled in XY with factors=" + Arrays.toString(tmpDownsampleFactors) );

							final JavaRDD< TileInfo > rdd = sparkContext.parallelize( tmpNewCells );
							final JavaRDD< TileInfo > fused = rdd.map( cell ->
								{
									final List< TileInfo > tilesWithinCell = TileOperations.findTilesWithinSubregion( lastLevelTmpCells, cell );
									if ( tilesWithinCell.isEmpty() )
										return null;

									// Check in advance for non-null size after downsampling
									for ( int d = 0; d < cell.numDimensions(); d++ )
										if ( cell.getSize( d ) / tmpDownsampleFactors[ d ] <= 0 )
											return null;

									//System.out.println( "There are " + tilesWithinCell.size() + " tiles within the cell #"+cell.getIndex() );

									final Boundaries cellBox = cell.getBoundaries();
									final long[] downscaledCellPos = new long[ cellBox.numDimensions() ];
									for ( int d = 0; d < downscaledCellPos.length; d++ )
										downscaledCellPos[ d ] = cellBox.min( d ) / tmpDownsampleFactors[ d ];

									final long[] cellIndices = new long[ downscaledCellPos.length ];
									for ( int d = 0; d < downscaledCellPos.length; d++ )
										cellIndices[ d ] = downscaledCellPos[ d ] / lastLevelTmpCellSize[ d ];

									final String outFilepath = String.format( "%s/%d/%d/%d.tif", levelTmpFolder, cellIndices.length > 2 ? cellIndices[ 2 ] : 0, cellIndices[ 1 ], cellIndices[ 0 ] );
									Paths.get( outFilepath ).getParent().toFile().mkdirs();
									cell.setFilePath( outFilepath );

									final ImagePlusImg< T, ? > outImg = FusionPerformer.fuseTilesWithinCellSimpleWithDownsampling( tilesWithinCell, cell, tmpDownsampleFactors );

									for ( int d = 0; d < cell.numDimensions(); d++ )
									{
										cell.setPosition( d, downscaledCellPos[ d ] );
										cell.setSize( d, cellBox.dimension( d ) / tmpDownsampleFactors[ d ] );
									}

									Utils.saveTileImageToFile( cell, outImg );

									return cell;
								} );

							final ArrayList< TileInfo > output = new ArrayList<>( fused.collect() );
							output.removeAll( Collections.singleton( null ) );

							System.out.println( "Obtained " + output.size() + " tmp output cells (downsampled in XY)" );
							smallerCells = output.toArray( new TileInfo[ 0 ] );
						}

						try
						{
							TileInfoJSONProvider.saveTilesConfiguration( smallerCells, currLevelTmpPath );
						}
						catch ( final IOException e )
						{
							e.printStackTrace();
						}

						for ( int d = 0; d < 2; d++ )
						{
							downsampleFactors[ d ] = 1;
							upscaledCellSize[ d ] = cellSize[ d ];
						}
					}

					lastLevelTmpPath = currLevelTmpPath;

					final Boundaries space = TileOperations.getCollectionBoundaries( smallerCells );
					System.out.println( "New (tmp downsampled in XY) space is " + Arrays.toString( Intervals.dimensionsAsLongArray( space ) ) );
					System.out.println( "Using tmp output to produce downsampled result with factors=" + Arrays.toString(downsampleFactors) + ", upscaledCellSize="+Arrays.toString(upscaledCellSize) );

					final List< TileInfo > newLevelCells = TileOperations.divideSpace( space, new FinalDimensions( upscaledCellSize ) );
					System.out.println( "There are " + newLevelCells.size() + " cells on the current scale level");

					final JavaRDD< TileInfo > rdd = sparkContext.parallelize( newLevelCells );
					final JavaRDD< TileInfo > fused = rdd.map( cell ->
						{
							final List< TileInfo > tilesWithinCell = TileOperations.findTilesWithinSubregion( smallerCells, cell );
							if ( tilesWithinCell.isEmpty() )
								return null;

							// Check for non-null size after downsampling in advance
							for ( int d = 0; d < cell.numDimensions(); d++ )
								if ( cell.getSize( d ) / downsampleFactors[ d ] <= 0 )
									return null;

							//System.out.println( "There are " + tilesWithinCell.size() + " tiles within the cell #"+cell.getIndex() );

							final Boundaries cellBox = cell.getBoundaries();
							final long[] downscaledCellPos = new long[ cellBox.numDimensions() ];
							for ( int d = 0; d < downscaledCellPos.length; d++ )
								downscaledCellPos[ d ] = cellBox.min( d ) / downsampleFactors[ d ];

							final long[] cellIndices = new long[ downscaledCellPos.length ];
							for ( int d = 0; d < downscaledCellPos.length; d++ )
								cellIndices[ d ] = downscaledCellPos[ d ] / cellSize[ d ];

							final String outFilepath = String.format( "%s/%d/%d/%d.tif", levelFolder, cellIndices.length > 2 ? cellIndices[ 2 ] : 0, cellIndices[ 1 ], cellIndices[ 0 ] );
							Paths.get( outFilepath ).getParent().toFile().mkdirs();
							cell.setFilePath( outFilepath );

							final ImagePlusImg< T, ? > outImg;
							if ( currLevel == 0 )
							{
								// 'channel' version with virtual image loader was necessary for the Zeiss dataset
//								outImg = FusionPerformer.fuseTilesWithinCellUsingMaxMinDistance(
//										tilesWithinCell,
//										cellBox,
//										new NLinearInterpolatorFactory(),
//										channel );
								outImg = ( ImagePlusImg ) FusionPerformer.fuseTilesWithinCellUsingMaxMinDistance(
										tilesWithinCell,
										cellBox,
										broadcastedFlatfieldCorrection.value(),
										broadcastedPairwiseConnectionsMap.value() );
							}
							else
							{
								outImg = FusionPerformer.fuseTilesWithinCellSimpleWithDownsampling( tilesWithinCell, cell, downsampleFactors );

								for ( int d = 0; d < cell.numDimensions(); d++ )
								{
									cell.setPosition( d, downscaledCellPos[ d ] );
									cell.setSize( d, cellBox.dimension( d ) / downsampleFactors[ d ] );
								}
							}

							Utils.saveTileImageToFile( cell, outImg );

							return cell;
						} );

					final ArrayList< TileInfo > output = new ArrayList<>( fused.collect() );
					output.removeAll( Collections.singleton( null ) );

					if ( output.isEmpty() )
					{
						System.out.println( "Resulting space is empty, stop generating scales" );
						break;
					}
					else
					{
						System.out.println( "Obtained " + output.size() + " output non-empty cells" );
					}

					lastLevelCells = output.toArray( new TileInfo[ 0 ] );

					try
					{
						TileInfoJSONProvider.saveTilesConfiguration( lastLevelCells, levelConfigurationOutputPath );
					}
					catch ( final IOException e )
					{
						e.printStackTrace();
					}
				}

				final Boundaries lastLevelSpace = TileOperations.getCollectionBoundaries( lastLevelCells );
				minDimension = Long.MAX_VALUE;
				maxDimension = Long.MIN_VALUE;
				for ( int d = 0; d < lastLevelSpace.numDimensions(); d++ )
				{
					minDimension = Math.min( lastLevelSpace.dimension( d ), minDimension );
					maxDimension = Math.max( lastLevelSpace.dimension( d ), maxDimension );
				}

				levelToDownsampleFactors.put( level, fullDownsampleFactors.length == 3 ? fullDownsampleFactors : new int[] { fullDownsampleFactors[ 0 ], fullDownsampleFactors[ 1 ], 1 } );
				levelToCellSize.put( level, cellSize.length == 3 ? cellSize : new int[] { cellSize[ 0 ], cellSize[ 1 ], 1 } );

				System.out.println( "Processed level " + level + " of size " + Arrays.toString( lastLevelSpace.getDimensions() ) );

				level++;
			}
			while ( minDimension > 1 && maxDimension > job.getArgs().fusionCellSize() * 4 );

			final double[][] transform = null;	// TODO: can specify transform if needed

			final Dimensions imageDimensions;
			{
				final Dimensions imageDimensionsTemp = TileOperations.getCollectionBoundaries( job.getTiles( channel ) );
				if ( job.getDimensionality() < 3 )
					imageDimensions = new FinalDimensions( imageDimensionsTemp.dimension( 0 ), imageDimensionsTemp.dimension( 1 ), 1 );
				else
					imageDimensions = imageDimensionsTemp;
			}

			final CellFileImageMetaData export = new CellFileImageMetaData(
					exportFolder + "/%1$d/%4$d/%3$d/%2$d.tif",
					//Utils.getImageType( Arrays.asList( job.getTiles() ) ).toString(),
					// TODO: can't derive from the tiles anymore since we convert the image to FloatType with illumination correction
					ImageType.GRAY32.toString(),
					Intervals.dimensionsAsLongArray( imageDimensions ),
					levelToDownsampleFactors,
					levelToCellSize,
					transform,
					voxelDimensions );
			try
			{
				TileInfoJSONProvider.saveMultiscaledExportMetadata( export, Utils.addFilenameSuffix( job.getArgs().inputTileConfigurations().get( channel ), "-export-channel" + channel + overlapsPathSuffix ) );
			}
			catch ( final IOException e )
			{
				e.printStackTrace();
			}

			exports.add( export );

			broadcastedPairwiseConnectionsMap.destroy();
			broadcastedFlatfieldCorrection.destroy();
		}

		System.out.println( "All channels have been exported" );
		try
		{
			TileInfoJSONProvider.saveMultiscaledExportMetadataList( exports, Paths.get( job.getArgs().inputTileConfigurations().get( 0 ) ).getParent().toString() +  "/export" + overlapsPathSuffix +".json" );
		}
		catch ( final IOException e )
		{
			e.printStackTrace();
		}
	}
}
