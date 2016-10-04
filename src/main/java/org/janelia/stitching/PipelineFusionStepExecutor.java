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
								outImg = FusionPerformer.fuseTilesWithinCellWithBlending( tilesWithinCell, cell );
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


	/*@Override
	public void run()
	{
		final Boundaries boundaries = TileOperations.getCollectionBoundaries( job.getTiles() );
		final ArrayList< TileInfo > cells = TileOperations.divideSpaceBySize( boundaries, job.getArgs().fusionCellSize() );
		fuse( cells );
	}

	private void fuse( final ArrayList< TileInfo > cells )
	{
		System.out.println( "There are " + cells.size() + " output cells in total" );

		final String fusedFolder = job.getBaseFolder() + "/fused";
		new File( fusedFolder ).mkdirs();

		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( cells );
		final JavaRDD< TileInfo > fused = rdd.map(
				new Function< TileInfo, TileInfo >()
				{
					private static final long serialVersionUID = 8324712817942470416L;

					@Override
					public TileInfo call( final TileInfo cell ) throws Exception
					{
						final ArrayList< TileInfo > tilesWithinCell = TileOperations.findTilesWithinSubregion( job.getTiles(), cell );
						if ( tilesWithinCell.isEmpty() )
							return null;

						cell.setFilePath( fusedFolder + "/" + job.getDatasetName() + "_tile" + cell.getIndex() + ".tif" );
						System.out.println( "Starting to fuse tiles within cell " + cell.getIndex() );

						fuseTilesWithinCell( tilesWithinCell, cell );

						System.out.println( "Completed for cell " + cell.getIndex() );

						return cell;
					}
				});

		final ArrayList< TileInfo > output = new ArrayList<>( fused.collect() );
		output.removeAll( Collections.singleton( null ) );
		System.out.println( "Obtained " + output.size() + " output non-empty cells" );

		try
		{
			final TileInfo[] newTiles = output.toArray( new TileInfo[ 0 ] );
			job.setTiles( newTiles );
			TileInfoJSONProvider.saveTilesConfiguration( newTiles, Utils.addFilenameSuffix( Utils.removeFilenameSuffix( job.getArgs().inputFilePath(), "_full" ), "_fused" ) );
		}
		catch ( final Exception e )
		{
			e.printStackTrace();
		}
	}

	@SuppressWarnings( "unchecked" )
	public < T extends RealType< T > & NativeType< T > > void fuseTilesWithinCell( final ArrayList< TileInfo > tiles, final TileInfo cell ) throws Exception
	{
		ImageType imageType = null;
		for ( final TileInfo tile : tiles )
		{
			if ( imageType == null )
				imageType = tile.getType();
			else if ( imageType != tile.getType() )
				throw new Exception( "Can't fuse images of different types" );
		}

		final Map< Integer, Dimensions > imageDimensions = new HashMap<>();
		for ( final TileInfo tile : tiles )
			imageDimensions.put( tile.getIndex(), tile.getBoundaries() );

		fuse( tiles, cell, ( T ) imageType.getType(), new CustomBlendingPixel( imageDimensions ) );
	}

	@SuppressWarnings( "unchecked" )
	private < T extends RealType< T > & NativeType< T > > void fuse( final ArrayList< TileInfo > tiles, final TileInfo cell, final T type, final PixelFusion pixelStrategy ) throws Exception
	{
		final Boundaries cellBoundaries = cell.getBoundaries();

		// Create pixels array
		System.out.println( "cell: " + Arrays.toString( cellBoundaries.getDimensions() ) );
		final ArrayList< PixelFusion > pixels = new ArrayList<>();
		int pixelsCount = 1;
		for ( int d = 0; d < cell.numDimensions(); d++ )
			pixelsCount *= cellBoundaries.dimension( d );
		for ( int i = 0; i < pixelsCount; i++ )
			pixels.add( pixelStrategy.copy() );
		final ListImg< PixelFusion > pixelsArr = new ListImg<>( pixels, cellBoundaries.getDimensions() );
		final RandomAccessibleInterval< PixelFusion > pixelsArrInterval = Views.translate( pixelsArr, cellBoundaries.getMin() );

		// Draw all intervals onto it one by one
		for ( int i = 0; i < tiles.size(); i++ )
		{
			// Open the image
			final TileInfo tile = tiles.get( i );
			System.out.println( "[Cell " + cell.getIndex() + "] Loading image " + ( i + 1 ) + " of " + tiles.size() );
			final ImagePlus imp = IJ.openImage( Utils.getAbsoluteImagePath( job, tile ) );
			Utils.workaroundImagePlusNSlices( imp );

			final Boundaries tileBoundaries = tile.getBoundaries();
			final FinalInterval intersection = Intervals.intersect( new FinalInterval( tileBoundaries.getMin(), tileBoundaries.getMax() ), pixelsArrInterval );
			final RealRandomAccessible< T > interpolatedTile = Views.interpolate( Views.extendBorder( ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp ) ), new NLinearInterpolatorFactory<>() );

			final Translation3D translation = new Translation3D( tile.getPosition() );
			final RandomAccessible< T > translatedInterpolatedTile = RealViews.affine( interpolatedTile, translation );

			final IterableInterval< T > tileSource = Views.flatIterable( Views.interval( translatedInterpolatedTile, intersection ) );
			final IterableInterval< PixelFusion > cellBox = Views.flatIterable( Views.interval( pixelsArrInterval, intersection ) );

			final Cursor< T > source = tileSource.cursor();
			final Cursor< PixelFusion > target = cellBox.cursor();

			final int tileIndex = tile.getIndex();
			final double[] tilePosition = tile.getPosition();
			final double[] localPosition = new double[ tilePosition.length ];
			while ( source.hasNext() )
			{
				final double val = source.next().getRealDouble();
				source.localize( localPosition );
				for ( int d = 0; d < localPosition.length; d++ )
					localPosition[ d ] -= tilePosition[ d ];
				target.next().addValue( val, tileIndex, localPosition );
			}

			imp.close();
		}

		// Create output image
		final Img< T > out = new ImagePlusImgFactory< T >().create( cellBoundaries.getDimensions(), type.createVariable() );
		final RandomAccessibleInterval< T > cellImg = Views.translate( out, cellBoundaries.getMin() );

		final IterableInterval< PixelFusion > pixelSource = Views.flatIterable( pixelsArrInterval );
		final IterableInterval< T > cellImgIterable = Views.flatIterable( cellImg );

		final Cursor< PixelFusion > source = pixelSource.cursor();
		final Cursor< T > target = cellImgIterable.cursor();

		while ( source.hasNext() )
			target.next().setReal( source.next().getValue() );

		final ImagePlus outImg = ImageJFunctions.wrap( out, "" );
		cell.setType( ImageType.valueOf( outImg.getType() ) );
		System.out.println( "Saving the resulting file for cell " + cell.getIndex() );
		IJ.saveAsTiff( outImg, cell.getFilePath() );
		outImg.close();
	}*/
}
