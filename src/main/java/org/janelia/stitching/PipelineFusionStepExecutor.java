package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.bdv.fusion.CellFileImageMetaData;
import org.janelia.util.ImageImporter;

import ij.ImagePlus;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;

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


/*
	@Override
	public void run() throws PipelineExecutionException
	{
		TileOperations.translateTilesToOriginReal( job.getTiles() );

//		for ( int channelExport = 2; channelExport < 4; channelExport++ )
//			exportOverlapsForFusion(
//					TileOperations.divideSpaceBySize(
//							TileOperations.getCollectionBoundaries( job.getTiles() ),
//							job.getArgs().fusionCellSize() ),
//					job.getArgs().inputFilePath() + "-overlaps/channel" + channelExport,
//					channelExport );
//		if ( job.getTiles() != null )
//		{
//			System.out.println( "Done, faking an error in order to exit" );
//			throw new PipelineExecutionException( "Fake exception" );
//		}

		final ImagePlus testImp = ImageImporter.openImage( job.getTiles()[ 0 ].getFilePath() );
		Utils.workaroundImagePlusNSlices( testImp );
		final int channels = testImp.getNChannels();
		testImp.close();

		for ( int ch = 0; ch < channels; ch++ )
		{
			final int channel = ch;
			System.out.println( "Processing channel #" + ch );
			final String baseOutputFolder = job.getBaseFolder() + "/channel" + ch + "/fused";

			int level = 0;
			TileInfo[] lastLevelCells = job.getTiles();

			do
			{
				System.out.println( "Processing level " + level + "...");

				final Boundaries lastLevelSpace = TileOperations.getCollectionBoundaries( lastLevelCells );
				final List< TileInfo > newLevelCells = TileOperations.divideSpaceBySize( lastLevelSpace, job.getArgs().fusionCellSize() * (level > 0 ? 2 : 1) );
				System.out.println( "There are " + newLevelCells.size() + " cells on the current scale level");

				final List< List< TileInfo > > combinedNewLevelCells = new ArrayList<>();
				if ( level == 0 )
				{
					final Map< Integer, List< TileInfo > > tilesToCombinedCells = new TreeMap<>();
					for ( final TileInfo cell : newLevelCells )
					{
						final List< TileInfo > tilesWithinCell = TileOperations.findTilesWithinSubregion( job.getTiles(), cell );
						if ( tilesWithinCell.size() <= 0 )
							continue;
						else if ( tilesWithinCell.size() > 1 )
							combinedNewLevelCells.add( Arrays.asList( cell ) );
						else
						{
							final int tileIndex = tilesWithinCell.get( 0 ).getIndex();
							if ( !tilesToCombinedCells.containsKey( tileIndex ) )
								tilesToCombinedCells.put( tileIndex, new ArrayList<>() );

							tilesToCombinedCells.get( tileIndex ).add( cell );
						}
					}
					combinedNewLevelCells.addAll( tilesToCombinedCells.values() );
				}
				else
				{
					for ( final TileInfo cell : newLevelCells )
						combinedNewLevelCells.add( Arrays.asList( cell ) );
				}
				System.out.println( "There are " + combinedNewLevelCells.size() + " combined cell items");

				final int currLevel = level;
				final String levelFolder = baseOutputFolder + "/" + level;

				final String levelConfigurationOutputPath = job.getBaseFolder() + "/channel" + ch + "/" + Utils.addFilenameSuffix( Paths.get( job.getArgs().inputFilePath() ).getFileName().toString(), "-scale" + level );
				try
				{
					final TileInfo[] exportedLevelCells = TileInfoJSONProvider.loadTilesConfiguration( levelConfigurationOutputPath );
					System.out.println( "Loaded configuration file for level " + level );

					lastLevelCells = exportedLevelCells;
					level++;
					continue;

				} catch (final Exception e) {
					System.out.println( "Output configuration file doesn't exist for level " + level + ", generating..." );
				}

				final TileInfo[] smallerCells = lastLevelCells;
				final JavaRDD< List< TileInfo > > rdd = sparkContext.parallelize( combinedNewLevelCells );
				final JavaRDD< TileInfo > fused = rdd.flatMap(
						new FlatMapFunction< List< TileInfo >, TileInfo >()
						{
							private static final long serialVersionUID = -8401196579319920787L;

							@Override
							public List< TileInfo > call( final List< TileInfo > cells ) throws Exception
							{
								// it's guaranteed that for all individual cells in the given list the tiles set is the same, but check this one more time
								List< TileInfo > tilesWithinCell = null;
								for ( final TileInfo cell : cells )
								{
									final List< TileInfo > tilesWithinCurrentCell = TileOperations.findTilesWithinSubregion( smallerCells, cell );
									if ( tilesWithinCell == null )
										tilesWithinCell = tilesWithinCurrentCell;
									else if ( new HashSet<>( tilesWithinCurrentCell ).retainAll( tilesWithinCell ) )
										throw new PipelineExecutionException( "Incorrectly combined non-overlapping regions" );
								}
								if ( tilesWithinCell.isEmpty() )
									return null;

								// Check in advance for non-null size after downsampling
								if ( currLevel != 0 )
									for ( int d = 0; d < cells.get( 0 ).numDimensions(); d++ )
										if ( cells.get( 0 ).getSize( d ) < 2 )
											return null;

								final Boundaries cellBox = TileOperations.getCollectionBoundaries( cells.toArray( new TileInfo[ 0 ] ) );
								final long[] downscaledCellPos = cellBox.getMin();
								if ( currLevel != 0 )
									for ( int d = 0; d < downscaledCellPos.length; d++ )
										downscaledCellPos[ d ] /= 2;

								final int[] cellIndices = new int[ downscaledCellPos.length ];
								for ( int d = 0; d < downscaledCellPos.length; d++ )
									cellIndices[ d ] = ( int ) ( downscaledCellPos[ d ] / job.getArgs().fusionCellSize() );
								final String innerFolder = ( cells.get( 0 ).numDimensions() > 2 ? cellIndices[ 2 ] + "/" : "" ) + cellIndices[ 1 ];
								final String outFilename = cellIndices[ 0 ] + ".tif";

								new File( levelFolder + "/" + innerFolder ).mkdirs();
								for ( final TileInfo cell : cells )
									cell.setFilePath( levelFolder + "/" + innerFolder + "/" + outFilename );

								if ( tilesWithinCell.size() == 1)
									System.out.println( "Tile " + tilesWithinCell.iterator().next().getIndex() + ": copying " + cells.size() + " inner cells" );
								else
									System.out.println( "There are " + tilesWithinCell.size() + " tiles within the cell #"+cells.get( 0 ).getIndex() );

								final Map< Integer, Img< ? extends NumericType > > outImgs;
								if ( currLevel == 0 )
								{
									if ( tilesWithinCell.size() == 1 )
									{
										outImgs = ( Map ) FusionPerformer.copyMultipleCells(
												tilesWithinCell.iterator().next(),
												cells,
												channel );
									}
									else
									{
										if ( cells.size() != 1 )
											throw new PipelineExecutionException( "Expected single output cell for MaxMinDistance fusion mode" );

										outImgs = Collections.singletonMap(
												cells.get( 0 ).getIndex(),
												FusionPerformer.fuseTilesWithinCellUsingMaxMinDistance(
														tilesWithinCell,
														cells.get( 0 ),
														channel,
														job.getArgs().inputFilePath() + "-overlaps/channel" + channel )
												);
									}
								}
								else
								{
									outImgs = Collections.singletonMap(
											cells.get( 0 ).getIndex(),
											FusionPerformer.fuseTilesWithinCellSimpleWithDownsampling( tilesWithinCell, cells.get( 0 ), 2 ) );

									for ( final TileInfo cell : cells )
										for ( int d = 0; d < cell.numDimensions(); d++ )
										{
											cell.setPosition( d, downscaledCellPos[ d ] );
											cell.setSize( d, cellBox.dimension( d ) / 2 );
										}
								}

								for ( final TileInfo cell : cells )
									Utils.saveTileImageToFile( cell, outImgs.get( cell.getIndex() ) );

								return cells;
							}
						});

				final ArrayList< TileInfo > output = new ArrayList<>( fused.collect() );
				output.removeAll( Collections.singleton( null ) );
				System.out.println( "Obtained " + output.size() + " output non-empty cells" );

				lastLevelCells = output.toArray( new TileInfo[ 0 ] );

				try
				{
					TileInfoJSONProvider.saveTilesConfiguration( lastLevelCells, levelConfigurationOutputPath );
				}
				catch ( final IOException e )
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				level++;
			}
			while ( lastLevelCells.length > 1 ); // until the whole image fits into a single cell

			final int[] cellDimensions = new int[ job.getDimensionality() ];
			Arrays.fill( cellDimensions, job.getArgs().fusionCellSize() );
			final MultiscaledExportMetadata export = new MultiscaledExportMetadata(
					baseOutputFolder,
					level,
					TileOperations.getCollectionBoundaries( job.getTiles() ).getDimensions(),
					cellDimensions);
			try
			{
				TileInfoJSONProvider.saveMultiscaledExportMetadata( export, Utils.addFilenameSuffix( job.getArgs().inputFilePath(), "-export-channel" + ch ) );
			}
			catch ( final IOException e )
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}



	public < T extends RealType< T > & NativeType< T > > void exportOverlapsForFusion(
			final List< TileInfo > cells,
			final String outputPath,
			final int channel )
	{
		new File( outputPath ).mkdirs();
		sparkContext.parallelize( Arrays.asList( job.getTiles() ) ).foreach( tile ->
		{
			final List< TileInfo > tileCells = TileOperations.findTilesWithinSubregion( cells.toArray( new TileInfo[ 0 ] ), tile );

			System.out.println( "Tile " + tile.getIndex() + ": covered by " + tileCells.size() + " cells" );
			// TODO: for now, remove trivial cells to process them later
			Iterator< TileInfo > iter = tileCells.iterator();
			while ( iter.hasNext() )
				if ( TileOperations.findTilesWithinSubregion( job.getTiles(), iter.next() ).size() == 1 )
					iter.remove();

			System.out.println( "Tile " + tile.getIndex() + ": " + tileCells.size() + " of these cells are non-trivial" );

			// remove cells that are already exported
			iter = tileCells.iterator();
			while ( iter.hasNext() )
				if ( Files.exists( Paths.get( outputPath + "/" + String.format( "tile%d-cell%d", tile.getIndex(), iter.next().getIndex() ) + ".tif" ) ) )
					iter.remove();

			if ( tileCells.isEmpty() )
			{
				System.out.println( "Tile " + tile.getIndex() + ": all cells are already exported" );
				return;
			}

			System.out.println( "Tile " + tile.getIndex() + ": removed existing cells, pending generation of " + tileCells.size() + " cells" );

			// sort cells by their Z coordinate
			if ( tile.numDimensions() > 2 )
				Collections.sort( tileCells, ( a, b ) -> Long.compare( ( long ) a.getPosition( 2 ), ( long ) b.getPosition( 2 ) ) );

			for ( int j = 1; j < tileCells.size(); j++ )
				if ( tileCells.get( j - 1 ).getPosition( 2 ) > tileCells.get( j ).getPosition( 2 ) )
					throw new PipelineExecutionException( "Violated Z order" );

			final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
			Utils.workaroundImagePlusNSlices( imp );
			final VirtualStackImageLoader< T, ?, ? > loader = ( VirtualStackImageLoader ) VirtualStackImageLoader.createUnsignedShortInstance( imp );
			final RandomAccessibleInterval< T > inputRai = loader.getSetupImgLoader( channel ).getImage( 0 );
			final RealRandomAccessible< T > interpolatedTile = Views.interpolate( Views.extendBorder( inputRai ), new NLinearInterpolatorFactory<>() );
			final AbstractTranslation translation = ( tile.numDimensions() == 3 ? new Translation3D( tile.getPosition() ) : new Translation2D( tile.getPosition() ) );
			final RandomAccessible< T > translatedInterpolatedTile = RealViews.affine( interpolatedTile, translation );

			final T type = ( T ) ImageType.valueOf( imp.getType() ).getType();
			final Boundaries tileBoundaries = tile.getBoundaries();

			System.out.println( "Tile " + tile.getIndex() + ": opened image (tile boundaries at " + Arrays.toString( tileBoundaries.getMin() ) + " of size " + Arrays.toString( tileBoundaries.getDimensions() ) + ")" );
			long z = Long.MIN_VALUE;
			int cellsRemaining = tileCells.size();

			for ( final TileInfo cell : tileCells )
			{
				final Boundaries cellBoundaries = cell.getBoundaries();

				if ( cellBoundaries.min( 2 ) != z )
				{
					z = cellBoundaries.min( 2 );
					System.out.println( "Tile " + tile.getIndex() + ": processing slize with z="+z + " (cellsRemaining="+cellsRemaining+")" );
				}
				cellsRemaining--;

				final Img< T > out = new ImagePlusImgFactory< T >().create( cellBoundaries.getDimensions(), type.createVariable() );
				final RandomAccessibleInterval< T > cellImg = Views.translate( out, cellBoundaries.getMin() );
				final FinalInterval intersection = Intervals.intersect( tileBoundaries, cellImg );

				final IterableInterval< T > tileSource = Views.flatIterable( Views.interval( translatedInterpolatedTile, intersection ) );
				final IterableInterval< T > cellBox = Views.flatIterable( Views.interval( cellImg, intersection ) );

				final Cursor< T > source = tileSource.cursor();
				final Cursor< T > target = cellBox.cursor();

				while ( source.hasNext() )
					target.next().set( source.next() );

				final ImagePlus impOut = ImageJFunctions.wrap( out, String.format( "tile%d-cell%d", tile.getIndex(), cell.getIndex() ) );
				Utils.workaroundImagePlusNSlices( impOut );
				IJ.saveAsTiff( impOut, outputPath + "/" + impOut.getTitle() + ".tif" );
				impOut.close();
			}
		} );
	}
*/




// this was the second attempt finding appropriate divisible scale in order to generate a new one
/*
	@Override
	public void run()
	{
		TileOperations.translateTilesToOriginReal( job.getTiles() );

		final ImagePlus testImp = ImageImporter.openImage( job.getTiles()[ 0 ].getFilePath() );
		Utils.workaroundImagePlusNSlices( testImp );
		final int channels = testImp.getNChannels();
		testImp.close();

		final VoxelDimensions voxelDimensions = job.getArgs().voxelDimensions();
		final double[] normalizedVoxelDimensions = new double[ voxelDimensions.numDimensions() ];
		double voxelDimensionsMinValue = Double.MAX_VALUE;
		for ( int d = 0; d < normalizedVoxelDimensions.length; d++ )
			voxelDimensionsMinValue = Math.min( voxelDimensions.dimension( d ), voxelDimensionsMinValue );
		for ( int d = 0; d < normalizedVoxelDimensions.length; d++ )
			normalizedVoxelDimensions[ d ] = voxelDimensions.dimension( d ) / voxelDimensionsMinValue;
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );

		for ( int ch = 0; ch < channels; ch++ )
		{
			final int channel = ch;

			System.out.println( "Processing channel #" + channel );
			final String baseOutputFolder = job.getBaseFolder() + "/channel" + channel + "/fused";

			int level = 0;
			final List< TileInfo[] > lastLevelCells = new ArrayList<>();
			lastLevelCells.add( job.getTiles() );
			final Boundaries fullScaleSpace = TileOperations.getCollectionBoundaries( job.getTiles() );

			final TreeMap< Integer, long[] > levelToImageDimensions = new TreeMap<>(), levelToCellSize = new TreeMap<>();
			long minDimension = 0, maxDimension = 0;
			do
			{
				final int[] downsampleFactors = new int[ fullScaleSpace.numDimensions() ];
				final long[] cellSize = new long[ fullScaleSpace.numDimensions() ];
				final long[] upscaledCellSize = new long[ fullScaleSpace.numDimensions() ];
				for ( int d = 0; d < cellSize.length; d++ )
				{
					final long isotropicScaling = Math.round( ( 1 << level ) / normalizedVoxelDimensions[ d ] );
					final long singleCellSize = Math.round( job.getArgs().fusionCellSize() / normalizedVoxelDimensions[ d ] );
					downsampleFactors[ d ] = ( int ) Math.max( isotropicScaling, 1 );
					cellSize[ d ] = singleCellSize * ( 1 << level ) / downsampleFactors[ d ];
					upscaledCellSize[ d ] = downsampleFactors[ d ] * cellSize[ d ];
				}
				System.out.println( "Processing level " + level + ", downsamplingFactors=" + Arrays.toString(downsampleFactors));
				System.out.println( "cell size set to " + Arrays.toString(cellSize) +",  upscaled target cell size: " + Arrays.toString(upscaledCellSize) );

				// TODO: for now all scale levels are based on the original scale. Optimize by reusing an appropriate intermediate scale level in order to generate a new one
				final ArrayList< TileInfo > newLevelCells = TileOperations.divideSpace( fullScaleSpace, new FinalDimensions( upscaledCellSize ) );

				System.out.println( "There are " + newLevelCells.size() + " cells on the current scale level");

				final int currLevel = level;
				final String levelFolder = baseOutputFolder + "/" + level;

				final String levelConfigurationOutputPath = job.getBaseFolder() + "/channel" + channel + "/" + Utils.addFilenameSuffix( Paths.get( job.getArgs().inputFilePath() ).getFileName().toString(), "-scale" + level );
				try
				{
					final TileInfo[] exportedLevelCells = TileInfoJSONProvider.loadTilesConfiguration( levelConfigurationOutputPath );
					System.out.println( "Loaded configuration file for level " + level );

					lastLevelCells.add( exportedLevelCells );
				}
				catch ( final Exception e )
				{
					System.out.println( "Output configuration file doesn't exist for level " + level + ", generating..." );

					int highestDivisibleScale;
					for ( highestDivisibleScale = lastLevelCells.size() - 1; highestDivisibleScale > 0; highestDivisibleScale-- )
					{
						boolean divisible = true;
						for ( int d = 0; d < cellSize.length; d++ )
							divisible &= ( cellSize[ d ] % levelToCellSize.get( highestDivisibleScale - 1 )[ d ] == 0 );

						if ( divisible )
							break;
					}
					final TileInfo[] smallerCells = lastLevelCells.get( highestDivisibleScale );
					System.out.println( "Using " + (highestDivisibleScale-1) + " as an input for level " + level + " (input="+Arrays.toString(levelToCellSize.get( highestDivisibleScale-1 ))+", output="+Arrays.toString(cellSize));

					final JavaRDD< TileInfo > rdd = sparkContext.parallelize( newLevelCells );
					final JavaRDD< TileInfo > fused = rdd.map(
							new Function< TileInfo, TileInfo >()
							{
								private static final long serialVersionUID = -8401196579319920787L;

								@Override
								public TileInfo call( final TileInfo cell ) throws Exception
								{
									final List< TileInfo > tilesWithinCell = TileOperations.findTilesWithinSubregion( smallerCells, cell );
									if ( tilesWithinCell.isEmpty() )
										return null;

									// Check in advance for non-null size after downsampling
									for ( int d = 0; d < cell.numDimensions(); d++ )
										if ( cell.getSize( d ) / downsampleFactors[ d ] <= 0 )
											return null;

									System.out.println( "There are " + tilesWithinCell.size() + " tiles within the cell #"+cell.getIndex() );

									final Boundaries cellBox = cell.getBoundaries();
									final long[] downscaledCellPos = new long[ cellBox.numDimensions() ];
									for ( int d = 0; d < downscaledCellPos.length; d++ )
										downscaledCellPos[ d ] = cellBox.min( d ) / downsampleFactors[ d ];

									final long[] cellIndices = new long[ downscaledCellPos.length ];
									for ( int d = 0; d < downscaledCellPos.length; d++ )
										cellIndices[ d ] = downscaledCellPos[ d ] / cellSize[ d ];
									final String innerFolder = ( cell.numDimensions() > 2 ? cellIndices[ 2 ] + "/" : "" ) + cellIndices[ 1 ];
									final String outFilename = cellIndices[ 0 ] + ".tif";

									new File( levelFolder + "/" + innerFolder ).mkdirs();
									cell.setFilePath( levelFolder + "/" + innerFolder + "/" + outFilename );

									final Img< ? extends NumericType > outImg;
									if ( currLevel == 0 )
									{
										outImg = FusionPerformer.fuseTilesWithinCellUsingMaxMinDistance(
												tilesWithinCell,
												cellBox,
												new NLinearInterpolatorFactory(),
												channel );
									}
									else
									{
										try
										{
											outImg = FusionPerformer.fuseTilesWithinCellSimpleWithDownsampling( tilesWithinCell, cell, downsampleFactors );
										}
										catch (final Exception e)
										{
											throw new Exception( "Something wrong with cell " +cell.getIndex() );
										}

										for ( int d = 0; d < cell.numDimensions(); d++ )
										{
											cell.setPosition( d, downscaledCellPos[ d ] );
											cell.setSize( d, cellBox.dimension( d ) / downsampleFactors[ d ] );
										}
									}

									Utils.saveTileImageToFile( cell, outImg );

									return cell;
								}
							});

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

					lastLevelCells.add( output.toArray( new TileInfo[ 0 ] ) );

					try
					{
						TileInfoJSONProvider.saveTilesConfiguration( lastLevelCells.get( lastLevelCells.size() - 1 ), levelConfigurationOutputPath );
					}
					catch ( final IOException e1 )
					{
						e1.printStackTrace();
					}
				}

				final Boundaries lastLevelSpace = TileOperations.getCollectionBoundaries( lastLevelCells.get( lastLevelCells.size() - 1 ) );
				minDimension = Long.MAX_VALUE;
				maxDimension = Long.MIN_VALUE;
				for ( int d = 0; d < lastLevelSpace.numDimensions(); d++ )
				{
					minDimension = Math.min( lastLevelSpace.dimension( d ), minDimension );
					maxDimension = Math.max( lastLevelSpace.dimension( d ), maxDimension );
				}

				levelToCellSize.put( level, cellSize );
				levelToImageDimensions.put( level, lastLevelSpace.getDimensions() );

				System.out.println( "Processed level " + level + " of size " + Arrays.toString( lastLevelSpace.getDimensions() ) );

				level++;
			}
			while ( minDimension > 1 && maxDimension > 1000 );
			//while ( lastLevelCells.length > 1 ); // until the whole image fits into a single cell

			final double[] offset = null; 		// TODO: set offset if needed
			final double[][] transform = null;	// TODO: can specify unshearing transform if needed

			final MultiscaledExportMetadata export = new MultiscaledExportMetadata(
					baseOutputFolder + "/%1$d/%4$d/%3$d/%2$d.tif",
					Utils.getImageType( Arrays.asList( lastLevelCells.get( lastLevelCells.size() - 1 ) ) ).toString(),
					level,
					offset,
					transform,
					levelToImageDimensions,
					levelToCellSize,
					voxelDimensions );
			try
			{
				TileInfoJSONProvider.saveMultiscaledExportMetadata( export, Utils.addFilenameSuffix( job.getArgs().inputFilePath(), "-export-channel" + ch ) );
			}
			catch ( final IOException e )
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
*/


	@Override
	public void run() throws PipelineExecutionException
	{
		runImpl();
	}
	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void runImpl() throws PipelineExecutionException
	{
		TileOperations.translateTilesToOriginReal( job.getTiles() );

		// TODO: add option for passing separate JSON files as channels
		final ImagePlus testImp = ImageImporter.openImage( job.getTiles()[ 0 ].getFilePath() );
		Utils.workaroundImagePlusNSlices( testImp );
		final int channels = testImp.getNChannels();
		testImp.close();

		final VoxelDimensions voxelDimensions = job.getArgs().voxelDimensions();
		final double[] normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( voxelDimensions );
		System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );

		final List< CellFileImageMetaData > exports = new ArrayList<>();

		System.out.println( "Broadcasting illumination correction images" );
		final ImagePlus vImp = ImageImporter.openImage( Paths.get( job.getArgs().inputFilePath() ).getParent().toString() + "/illumination-correction/v-highres.tif" );
		final ImagePlus zImp = ImageImporter.openImage( Paths.get( job.getArgs().inputFilePath() ).getParent().toString() + "/illumination-correction/z-avg.tif" );
		if ( vImp != null )
			Utils.workaroundImagePlusNSlices( vImp );

		final Broadcast< RandomAccessibleInterval< U > > vBroadcast = sparkContext.broadcast( vImp != null ? ImagePlusImgs.from( vImp ) : null );
		final Broadcast< RandomAccessibleInterval< U > > zBroadcast = sparkContext.broadcast( zImp != null ? ImagePlusImgs.from( zImp ) : null );

		for ( int ch = 0; ch < channels; ch++ )
		{
			final int channel = ch;

			System.out.println( "Processing channel #" + channel );
			final String baseOutputFolder = job.getBaseFolder() + "/channel" + channel + "/fused";

			int level = 0;
			String lastLevelTmpPath = null;
			TileInfo[] lastLevelCells = job.getTiles();

			final TreeMap< Integer, int[] > levelToDownsampleFactors = new TreeMap<>(), levelToCellSize = new TreeMap<>();
			long minDimension = 0, maxDimension = 0;
			do
			{
				final int[] fullDownsampleFactors = new int[ job.getDimensionality() ];
				final int[] downsampleFactors = new int[ job.getDimensionality() ];
				final int[] singleCellSize = new int[ job.getDimensionality() ];
				final int[] cellSize = new int[ job.getDimensionality() ];
				final int[] upscaledCellSize = new int[ job.getDimensionality() ];
				for ( int d = 0; d < cellSize.length; d++ )
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
				final String levelFolder = baseOutputFolder + "/" + level;

				final String levelConfigurationOutputPath = job.getBaseFolder() + "/channel" + channel + "/" + Utils.addFilenameSuffix( Paths.get( job.getArgs().inputFilePath() ).getFileName().toString(), "-scale" + level );

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

					if ( downsampleFactors[ 2 ] == 1 )
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

					if ( downsampleFactors[ 2 ] == 1 || job.getDimensionality() < 3 )
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

									System.out.println( "There are " + tilesWithinCell.size() + " tiles within the cell #"+cell.getIndex() );

									final Boundaries cellBox = cell.getBoundaries();
									final long[] downscaledCellPos = new long[ cellBox.numDimensions() ];
									for ( int d = 0; d < downscaledCellPos.length; d++ )
										downscaledCellPos[ d ] = cellBox.min( d ) / tmpDownsampleFactors[ d ];

									final long[] cellIndices = new long[ downscaledCellPos.length ];
									for ( int d = 0; d < downscaledCellPos.length; d++ )
										cellIndices[ d ] = downscaledCellPos[ d ] / lastLevelTmpCellSize[ d ];
									final String innerFolder = ( cell.numDimensions() > 2 ? cellIndices[ 2 ] + "/" : "" ) + cellIndices[ 1 ];
									final String outFilename = cellIndices[ 0 ] + ".tif";

									new File( levelTmpFolder + "/" + innerFolder ).mkdirs();
									cell.setFilePath( levelTmpFolder + "/" + innerFolder + "/" + outFilename );

									final ImagePlusImg< T, ? > outImg;
									try
									{
										outImg = ( ImagePlusImg< T, ? > ) FusionPerformer.fuseTilesWithinCellSimpleWithDownsampling( tilesWithinCell, cell, tmpDownsampleFactors );
									}
									catch ( final Exception e )
									{
										throw new Exception( "Something wrong with cell " +cell.getIndex() );
									}

									for ( int d = 0; d < cell.numDimensions(); d++ )
									{
										cell.setPosition( d, downscaledCellPos[ d ] );
										cell.setSize( d, cellBox.dimension( d ) / tmpDownsampleFactors[ d ] );
									}

									Utils.saveTileImageToFile( cell, outImg );

									return cell;
								});

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

					final ArrayList< TileInfo > newLevelCells = TileOperations.divideSpace( space, new FinalDimensions( upscaledCellSize ) );
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

							System.out.println( "There are " + tilesWithinCell.size() + " tiles within the cell #"+cell.getIndex() );

							final Boundaries cellBox = cell.getBoundaries();
							final long[] downscaledCellPos = new long[ cellBox.numDimensions() ];
							for ( int d = 0; d < downscaledCellPos.length; d++ )
								downscaledCellPos[ d ] = cellBox.min( d ) / downsampleFactors[ d ];

							final long[] cellIndices = new long[ downscaledCellPos.length ];
							for ( int d = 0; d < downscaledCellPos.length; d++ )
								cellIndices[ d ] = downscaledCellPos[ d ] / cellSize[ d ];
							final String innerFolder = ( cell.numDimensions() > 2 ? cellIndices[ 2 ] + "/" : "" ) + cellIndices[ 1 ];
							final String outFilename = cellIndices[ 0 ] + ".tif";

							new File( levelFolder + "/" + innerFolder ).mkdirs();
							cell.setFilePath( levelFolder + "/" + innerFolder + "/" + outFilename );

							final ImagePlusImg< T, ? > outImg;
							if ( currLevel == 0 )
							{
								// 'channel' version with virtual image loader was necessary for the Zeiss dataset
//								outImg = FusionPerformer.fuseTilesWithinCellUsingMaxMinDistance(
//										tilesWithinCell,
//										cellBox,
//										new NLinearInterpolatorFactory(),
//										channel );
								outImg = FusionPerformer.fuseTilesWithinCellUsingMaxMinDistance(
										tilesWithinCell,
										cellBox,
										new NLinearInterpolatorFactory(),
										vBroadcast.value(),
										zBroadcast.value() );
							}
							else
							{
								try
								{
									outImg = ( ImagePlusImg< T, ? > ) FusionPerformer.fuseTilesWithinCellSimpleWithDownsampling( tilesWithinCell, cell, downsampleFactors );
								}
								catch ( final Exception e )
								{
									throw new Exception( "Something wrong with cell " +cell.getIndex() );
								}

								for ( int d = 0; d < cell.numDimensions(); d++ )
								{
									cell.setPosition( d, downscaledCellPos[ d ] );
									cell.setSize( d, cellBox.dimension( d ) / downsampleFactors[ d ] );
								}
							}

							Utils.saveTileImageToFile( cell, outImg );

							return cell;
						});

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

				levelToDownsampleFactors.put( level, fullDownsampleFactors );
				levelToCellSize.put( level, cellSize );

				System.out.println( "Processed level " + level + " of size " + Arrays.toString( lastLevelSpace.getDimensions() ) );

				level++;
			}
			while ( minDimension > 1 && maxDimension > job.getArgs().fusionCellSize() * 4 );

			final double[][] transform = null;	// TODO: can specify transform if needed

			final CellFileImageMetaData export = new CellFileImageMetaData(
					baseOutputFolder + "/%1$d/%4$d/%3$d/%2$d.tif",
					//Utils.getImageType( Arrays.asList( job.getTiles() ) ).toString(),
					// TODO: can't derive from the tiles anymore since we convert the image to FloatType with illumination correction
					ImageType.GRAY32.toString(),
					Intervals.dimensionsAsLongArray( TileOperations.getCollectionBoundaries( job.getTiles() ) ),
					levelToDownsampleFactors,
					levelToCellSize,
					transform,
					voxelDimensions );
			try
			{
				TileInfoJSONProvider.saveMultiscaledExportMetadata( export, Utils.addFilenameSuffix( job.getArgs().inputFilePath(), "-export-channel" + ch ) );
			}
			catch ( final IOException e )
			{
				e.printStackTrace();
			}

			exports.add( export );
		}

		System.out.println( "All channels have been exported" );
		try
		{
			TileInfoJSONProvider.saveMultiscaledExportMetadataList( exports, Utils.addFilenameSuffix( job.getArgs().inputFilePath(), "-export" ) );
		}
		catch ( final IOException e )
		{
			e.printStackTrace();
		}
	}
}
