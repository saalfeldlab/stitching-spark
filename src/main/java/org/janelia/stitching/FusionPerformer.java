package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bdv.export.Downsample;
import ij.IJ;
import ij.ImagePlus;
import mpicbg.stitching.fusion.PixelFusion;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.Img;
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

public class FusionPerformer
{

	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses simple pixel copying strategy.
	 */
	public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCellSimple( final List< TileInfo > tiles, final TileInfo cell ) throws Exception
	{
		return fuseTilesWithinCell( tiles, cell, null );
	}

	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses linear blending strategy on the borders.
	 */
	public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCellUsingBlending( final List< TileInfo > tiles, final TileInfo cell ) throws Exception
	{
		final Map< Integer, Dimensions > imageDimensions = new HashMap<>();
		for ( final TileInfo tile : tiles )
			imageDimensions.put( tile.getIndex(), tile.getBoundaries() );

		return fuseTilesWithinCell( tiles, cell, new CustomBlendingPixel( imageDimensions ) );
	}

	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses min-distance strategy within overlapped regions and takes an average when there are more than one value.
	 */
	public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCellUsingMinDistanceAverage( final List< TileInfo > tiles, final TileInfo cell ) throws Exception
	{
		final Map< Integer, RealInterval > imagesLocation = new HashMap<>();
		for ( final TileInfo tile : tiles )
		{
			final double[] max = new double[ tile.numDimensions() ];
			for ( int d = 0; d < max.length; d++ )
				max[ d ] = tile.getPosition( d ) + tile.getSize( d ) - 1;
			imagesLocation.put( tile.getIndex(), new FinalRealInterval( tile.getPosition(), max ) );
		}

		return fuseTilesWithinCell( tiles, cell, new MinDistanceAveragePixel( imagesLocation ) );
	}

	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses simple pixel copying strategy then downsamples the resulting image.
	 */
	public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCellSimpleWithDownsampling( final List< TileInfo > tiles, final TileInfo cell, final int downsampleFactor ) throws Exception
	{
		final ImageType imageType = getImageType( tiles );
		if ( imageType == null )
			throw new Exception( "Can't fuse images of different or unknown types" );

		cell.setType( imageType );
		final T type = ( T ) imageType.getType();
		final Img< T > fusedImg = fuseSimple( tiles, cell, type );

		final int[] outDimensions = new int[ cell.numDimensions() ];
		for ( int d = 0; d < outDimensions.length; d++ )
			outDimensions[ d ] = ( int ) Math.floor( ( ( double ) cell.getSize( d ) / downsampleFactor ) );

		final int[] dimFactors = new int[ cell.numDimensions() ];
		Arrays.fill( dimFactors, downsampleFactor );

		final Img< T > downsampledImg = new ImagePlusImgFactory< T >().create( outDimensions, type.createVariable() );
		Downsample.downsample( fusedImg, downsampledImg, dimFactors );

		return downsampledImg;
	}

	@SuppressWarnings( "unchecked" )
	public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCell( final List< TileInfo > tiles, final TileInfo cell, final PixelFusion pixelStrategy ) throws Exception
	{
		final ImageType imageType = getImageType( tiles );
		if ( imageType == null )
			throw new Exception( "Can't fuse images of different or unknown types" );

		cell.setType( imageType );
		final T type = ( T ) imageType.getType();
		if ( pixelStrategy == null )
			return fuseSimple( tiles, cell, type );
		else
			return fuseWithPixelStrategy( tiles, cell, type, pixelStrategy );
	}

	@SuppressWarnings( "unchecked" )
	private static < T extends RealType< T > & NativeType< T > > Img< T > fuseSimple( final List< TileInfo > tiles, final TileInfo cell, final T type ) throws Exception
	{
		System.out.println( "Fusing tiles within cell #" + cell.getIndex() + " of size " + Arrays.toString( cell.getSize() )+"..." );

		// Create output image
		final Boundaries cellBoundaries = cell.getBoundaries();
		final Img< T > out = new ImagePlusImgFactory< T >().create( cellBoundaries.getDimensions(), type.createVariable() );
		final RandomAccessibleInterval< T > cellImg = Views.translate( out, cellBoundaries.getMin() );

		for ( final TileInfo tile : tiles )
		{
			System.out.println( "Loading tile image " + tile.getFilePath() );

			final ImagePlus imp = IJ.openImage( tile.getFilePath() );
			Utils.workaroundImagePlusNSlices( imp );

			if ( imp == null )
				throw new Exception( "Can't open image: " + tile.getFilePath() );

			final Boundaries tileBoundaries = tile.getBoundaries();
			final FinalInterval intersection = Intervals.intersect( new FinalInterval( tileBoundaries.getMin(), tileBoundaries.getMax() ), cellImg );

			final RealRandomAccessible< T > interpolatedTile = Views.interpolate( Views.extendBorder( ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp ) ), new NLinearInterpolatorFactory<>() );

			final Translation3D translation = new Translation3D( tile.getPosition() );
			final RandomAccessible< T > translatedInterpolatedTile = RealViews.affine( interpolatedTile, translation );

			final IterableInterval< T > tileSource = Views.flatIterable( Views.interval( translatedInterpolatedTile, intersection ) );
			final IterableInterval< T > cellBox = Views.flatIterable( Views.interval( cellImg, intersection ) );

			final Cursor< T > source = tileSource.cursor();
			final Cursor< T > target = cellBox.cursor();

			while ( source.hasNext() )
				target.next().set( source.next() );
		}

		return out;
	}


	@SuppressWarnings( "unchecked" )
	private static < T extends RealType< T > & NativeType< T > > Img< T > fuseWithPixelStrategy( final List< TileInfo > tiles, final TileInfo cell, final T type, final PixelFusion pixelStrategy ) throws Exception
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
			final ImagePlus imp = IJ.openImage( tile.getFilePath() );
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

		return out;
	}


	private static ImageType getImageType( final List< TileInfo > tiles )
	{
		ImageType imageType = null;
		for ( final TileInfo tile : tiles )
		{
			if ( imageType == null )
				imageType = tile.getType();
			else if ( imageType != tile.getType() )
				return null;
		}
		return imageType;
	}
}
