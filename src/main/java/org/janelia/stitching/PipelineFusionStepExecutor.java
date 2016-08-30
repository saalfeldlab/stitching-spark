package org.janelia.stitching;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import ij.IJ;
import ij.ImagePlus;
import mpicbg.stitching.fusion.PixelFusion;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.img.list.ListImg;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Translation3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

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
		final Boundaries boundaries = TileOperations.getCollectionBoundaries( job.getTiles() );
		final ArrayList< TileInfo > cells = TileOperations.divideSpaceBySize( boundaries, job.getArgs().fusionCellSize() );
		fuse( cells );
	}

	/**
	 * Fuses tile images within a set of cells on a Spark cluster.
	 */
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

	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses linear blending strategy on the borders.
	 */
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
	}
}
