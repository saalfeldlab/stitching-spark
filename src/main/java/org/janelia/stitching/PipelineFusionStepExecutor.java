package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import net.imglib2.img.Img;
import net.imglib2.type.numeric.NumericType;

/**
 * Fuses a set of tiles within a set of small square cells using linear blending.
 * Saves fused tile configuration on the disk.
 *
 * @author Igor Pisarev
 */

public class PipelineFusionStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -8151178964876747760L;

	public PipelineFusionStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}


	@Override
	public void run()
	{
		TileOperations.translateTilesToOrigin( job.getTiles() );

		final String baseOutputFolder = job.getBaseFolder() + "/fused";

		int level = 0;
		TileInfo[] lastLevelCells = job.getTiles();

		do
		{
			System.out.println( "Processing level " + level + "...");

			final Boundaries lastLevelSpace = TileOperations.getCollectionBoundaries( lastLevelCells );
			final ArrayList< TileInfo > newLevelCells = TileOperations.divideSpaceBySize( lastLevelSpace, job.getArgs().fusionCellSize() * (level > 0 ? 2 : 1) );

			System.out.println( "There are " + newLevelCells.size() + " cells on the current scale level");

			final int currLevel = level;
			final String levelFolder = baseOutputFolder + "/" + level;

			final String levelConfigurationOutputPath = Utils.addFilenameSuffix( job.getArgs().inputFilePath(), "-scale" + level );
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
			final JavaRDD< TileInfo > rdd = sparkContext.parallelize( newLevelCells );
			final JavaRDD< TileInfo > fused = rdd.map(
					new Function< TileInfo, TileInfo >()
					{
						private static final long serialVersionUID = -8401196579319920787L;

						@Override
						public TileInfo call( final TileInfo cell ) throws Exception
						{
							final ArrayList< TileInfo > tilesWithinCell = TileOperations.findTilesWithinSubregion( smallerCells, cell );
							if ( tilesWithinCell.isEmpty() )
								return null;

							// Check in advance for non-null size after downsampling
							if ( currLevel != 0 )
								for ( int d = 0; d < cell.numDimensions(); d++ )
									if ( cell.getSize( d ) < 2 )
										return null;

							final Boundaries cellBox = cell.getBoundaries();
							final long[] downscaledCellPos = cellBox.getMin();
							if ( currLevel != 0 )
								for ( int d = 0; d < downscaledCellPos.length; d++ )
									downscaledCellPos[ d ] /= 2;

							final int[] cellIndices = new int[ downscaledCellPos.length ];
							for ( int d = 0; d < downscaledCellPos.length; d++ )
								cellIndices[ d ] = ( int ) ( downscaledCellPos[ d ] / job.getArgs().fusionCellSize() );
							final String innerFolder = cellIndices[ 2 ] + "/" + cellIndices[ 1 ];
							final String outFilename = cellIndices[ 0 ] + ".tif";

							new File( levelFolder + "/" + innerFolder ).mkdirs();
							cell.setFilePath( levelFolder + "/" + innerFolder + "/" + outFilename );

							System.out.println( "There are " + tilesWithinCell.size() + " tiles within the cell #"+cell.getIndex() );

							Img< ? extends NumericType > outImg = null;
							if ( currLevel == 0 )
							{
								outImg = FusionPerformer.fuseTilesWithinCellUsingMinDistanceAverage( tilesWithinCell, cell );
							}
							else
							{
								try
								{
									outImg = FusionPerformer.fuseTilesWithinCellSimpleWithDownsampling( tilesWithinCell, cell, 2 );
								}
								catch(final Exception e) {
									throw new Exception( "Something wrong with cell " +cell.getIndex() );
								}

								for ( int d = 0; d < cell.numDimensions(); d++ )
								{
									cell.setPosition( d, downscaledCellPos[ d ] );
									cell.setSize( d, cellBox.dimension( d ) / 2 );
								}
							}

							Utils.saveTileImageToFile( cell, outImg );

							return cell;
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
			TileInfoJSONProvider.saveMultiscaledExportMetadata( export, Utils.addFilenameSuffix( job.getArgs().inputFilePath(), "-export" ) );
		}
		catch ( final IOException e )
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
