package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
 * @author pisarevi
 *
 */

public class FusionPerformer
{

	private final StitchingJob job;

	public FusionPerformer( final StitchingJob job )
	{
		this.job = job;
	}

	@SuppressWarnings( "unchecked" )
	public < T extends RealType< T > & NativeType< T > > void fuseTilesWithinSubregion( final ArrayList< TileInfo > tiles, final TileInfo subregion ) throws Exception
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

		fuseSubregion( tiles, subregion, ( T ) imageType.getType(), new CustomBlendingPixel( imageDimensions ) );
	}

	@SuppressWarnings( "unchecked" )
	private < T extends RealType< T > & NativeType< T > > void fuseSubregion( final ArrayList< TileInfo > tiles, final TileInfo subregion, final T type, final PixelFusion pixelStrategy ) throws Exception
	{
		final Boundaries subregionBoundaries = subregion.getBoundaries();

		// Create pixels array
		System.out.println( "subregion: " + Arrays.toString( subregionBoundaries.getDimensions() ) );
		final ArrayList< PixelFusion > pixels = new ArrayList<>();
		int pixelsCount = 1;
		for ( int d = 0; d < subregion.numDimensions(); d++ )
			pixelsCount *= subregionBoundaries.dimension( d );
		for ( int i = 0; i < pixelsCount; i++ )
			pixels.add( pixelStrategy.copy() );
		final ListImg< PixelFusion > pixelsArr = new ListImg<>( pixels, subregionBoundaries.getDimensions() );
		final RandomAccessibleInterval< PixelFusion > pixelsArrInterval = Views.translate( pixelsArr, subregionBoundaries.getMin() );

		// Draw all intervals onto it one by one
		for ( int i = 0; i < tiles.size(); i++ )
		{
			// Open the image
			final TileInfo tile = tiles.get( i );
			System.out.println( "[Subregion " + subregion.getIndex() + "] Loading image " + ( i + 1 ) + " of " + tiles.size() );
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
		final Img< T > out = new ImagePlusImgFactory< T >().create( subregionBoundaries.getDimensions(), type.createVariable() );
		final RandomAccessibleInterval< T > cell = Views.translate( out, subregionBoundaries.getMin() );

		final IterableInterval< PixelFusion > pixelSource = Views.flatIterable( pixelsArrInterval );
		final IterableInterval< T > cellBox = Views.flatIterable( cell );

		final Cursor< PixelFusion > source = pixelSource.cursor();
		final Cursor< T > target = cellBox.cursor();

		while ( source.hasNext() )
			target.next().setReal( source.next().getValue() );

		final ImagePlus outImg = ImageJFunctions.wrap( out, "" );
		subregion.setType( ImageType.valueOf( outImg.getType() ) );
		System.out.println( "Saving the resulting file for subregion " + subregion.getIndex() );
		IJ.saveAsTiff( outImg, subregion.getFile() );
		outImg.close();
	}

	/*@SuppressWarnings( "unchecked" )
	private < T extends NumericType< T > & NativeType< T > > void fuseSubregion( final ArrayList< TileInfo > tiles, final TileInfo subregion, final T type ) throws Exception
	{
		final Boundaries subregionBoundaries = subregion.getBoundaries();

		// Create output image
		System.out.println( "subregion: " + Arrays.toString( subregionBoundaries.getDimensions() ) );
		final Img< T > out = new ImagePlusImgFactory< T >().create( subregionBoundaries.getDimensions(), type.createVariable() );
		final RandomAccessibleInterval< T > cell = Views.translate( out, subregionBoundaries.getMin() );

		// Draw all intervals onto it one by one
		for ( int i = 0; i < tiles.size(); i++ )
		{
			// Open the image
			final TileInfo tile = tiles.get( i );
			System.out.println( "[Subregion " + subregion.getIndex() + "] Loading image " + ( i + 1 ) + " of " + tiles.size() );
			final ImagePlus imp = IJ.openImage( Utils.getAbsoluteImagePath( job, tile ) );

			final Boundaries tileBoundaries = tile.getBoundaries();
			final FinalInterval intersection = Intervals.intersect( new FinalInterval( tileBoundaries.getMin(), tileBoundaries.getMax() ), cell );
			final RealRandomAccessible< T > interpolatedTile = Views.interpolate( Views.extendBorder( ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp ) ), new NLinearInterpolatorFactory<>() );

			final Translation3D translation = new Translation3D( tile.getPosition() );
			final RandomAccessible< T > translatedInterpolatedTile = RealViews.affine( interpolatedTile, translation );

			final IterableInterval< T > tileSource = Views.flatIterable( Views.interval( translatedInterpolatedTile, intersection ) );
			final IterableInterval< T > cellBox = Views.flatIterable( Views.interval( cell, intersection ) );

			final Cursor< T > source = tileSource.cursor();
			final Cursor< T > target = cellBox.cursor();

			while ( source.hasNext() )
				target.next().set( source.next() );

			imp.close();
		}

		final ImagePlus outImg = ImageJFunctions.wrap( out, "" );
		subregion.setType( ImageType.valueOf( outImg.getType() ) );
		System.out.println( "Saving the resulting file for subregion " + subregion.getIndex() );
		IJ.saveAsTiff( outImg, subregion.getFile() );
		outImg.close();
	}*/

	/*private < T extends NumericType< T > & NativeType< T > > void fuseSubregion( final ArrayList< TileInfo > tiles, final TileInfo subregion, final T type ) throws Exception
	{
		final Boundaries subregionBoundaries = subregion.getBoundaries();

		// Create output image
		System.out.println( "subregion: " + Arrays.toString( subregionBoundaries.getDimensions() ) );
		final Img< T > out = new ImagePlusImgFactory< T >().create( subregionBoundaries.getDimensions(), type.createVariable() );
		final RandomAccess< T > randomAccess = out.randomAccess();

		// Draw all intervals onto it one by one
		for ( int i = 0; i < tiles.size(); i++ )
		{
			// Open the image
			final TileInfo tile = tiles.get( i );
			System.out.println( "[Subregion " + subregion.getIndex() + "] Loading image " + ( i + 1 ) + " of " + tiles.size() );
			final ImagePlus img = IJ.openImage( Utils.getAbsoluteImagePath( job, tile ) );
			final Img< T > interval = ImageJFunctions.wrap( img );

			// Compute and apply the translation vector (offset within the subregion)
			final long[] tileSubregionOffset = new long[ tile.numDimensions() ];
			for ( int d = 0; d < tileSubregionOffset.length; d++ )
				tileSubregionOffset[ d ] = ( long ) Math.floor( tile.getPosition( d ) ) - subregionBoundaries.min( d );
			final RandomAccessibleInterval< T > translatedInterval = Views.translate( interval, tileSubregionOffset );

			// Crop the image (offset within the tile)
			final Boundaries tileBoundaries = tile.getBoundaries();
			final long[] tileImageOffset = new long[ tile.numDimensions() ];
			for ( int d = 0; d < tileImageOffset.length; d++ )
				tileImageOffset[ d ] = Math.max( 0, subregionBoundaries.min( d ) - tileBoundaries.min( d ) );

			final Boundaries tileBoundariesWithinSubregion = tile.getBoundaries();
			for ( int d = 0; d < subregion.numDimensions(); d++ )
			{
				tileBoundariesWithinSubregion.setMin( d, Math.max( tileBoundariesWithinSubregion.min( d ), subregionBoundaries.min( d ) ) - subregionBoundaries.min( d ) );
				tileBoundariesWithinSubregion.setMax( d, Math.min( tileBoundariesWithinSubregion.max( d ), subregionBoundaries.max( d ) ) - subregionBoundaries.min( d ) );
			}
			final IntervalView< T > croppedTranslatedIntervalView = Views.interval( translatedInterval, tileImageOffset, tileBoundariesWithinSubregion.getDimensions() );

			String debug = "Processing tile " + ( i + 1 ) + " of " + tiles.size() + " ";
			for ( int d = 0; d < croppedTranslatedIntervalView.numDimensions(); d++ )
				debug += "(" + ( croppedTranslatedIntervalView.min( d ) ) + "," + ( croppedTranslatedIntervalView.max( d ) ) + "),";
			System.out.println( debug );

			// Copy input to output
			final Cursor< T > cursorInput = croppedTranslatedIntervalView.localizingCursor();
			while ( cursorInput.hasNext() )
			{
				cursorInput.fwd();
				randomAccess.setPosition( cursorInput );
				randomAccess.get().set( cursorInput.get() );
			}

			img.close();
		}

		final ImagePlus outImg = ImageJFunctions.wrap( out, "" );
		subregion.setType( ImageType.valueOf( outImg.getType() ) );
		System.out.println( "Saving the resulting file for subregion " + subregion.getIndex() );
		IJ.saveAsTiff( outImg, subregion.getFile() );
		outImg.close();
	}*/
}
