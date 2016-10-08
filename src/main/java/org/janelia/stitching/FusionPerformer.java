package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bdv.export.Downsample;
import bdv.util.RealRandomAccessibleIntervalSource;
import ij.IJ;
import ij.ImagePlus;
import mpicbg.stitching.fusion.PixelFusion;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.img.list.ListImg;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AbstractTranslation;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Translation;
import net.imglib2.realtransform.Translation2D;
import net.imglib2.realtransform.Translation3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.Intervals;
import net.imglib2.util.LinAlgHelpers;
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
	 * Compute the intersection of two intervals.
	 * 
	 * Create a {@link FinalRealInterval} , which is the intersection of the input
	 * intervals (i.e., the area contained in both input intervals).
	 * 
	 * @param intervalA
	 *            input interval
	 * @param intervalB
	 *            input interval
	 * @return intersection of input intervals
	 */
	public static FinalRealInterval intersect( final RealInterval intervalA, final RealInterval intervalB )
	{
		assert intervalA.numDimensions() == intervalB.numDimensions();

		final int n = intervalA.numDimensions();
		final double[] min = new double[ n ];
		final double[] max = new double[ n ];
		for ( int d = 0; d < n; ++d )
		{
			min[ d ] = Math.max( intervalA.realMin( d ), intervalB.realMin( d ) );
			max[ d ] = Math.min( intervalA.realMax( d ), intervalB.realMax( d ) );
		}
		return new FinalRealInterval( min, max );
	}

	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses min-distance strategy within overlapped regions and takes an average when there are more than one value.
	 */
	public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCellUsingMaxMinDistance( 
			final List< TileInfo > tiles,
			final Interval targetInterval,
			final InterpolatorFactory< T, RandomAccessible< T > > interpolatorFactory ) throws Exception
	{
		final ImageType imageType = getImageType( tiles );
		if ( imageType == null )
			throw new Exception( "Can't fuse images of different or unknown types" );
		
		final ArrayImg< T, ? > out = new ArrayImgFactory< T >().create(
				Intervals.dimensionsAsLongArray( targetInterval ),
				( T )imageType.getType().createVariable() );
		final ArrayImg< DoubleType, DoubleArray > maxMinDistances = ArrayImgs.doubles(
				Intervals.dimensionsAsLongArray( targetInterval ) );
		
		for ( final TileInfo tile : tiles )
		{
			System.out.println( "Loading tile image " + tile.getFilePath() );

			final ImagePlus imp = IJ.openImage( tile.getFilePath() );
			Utils.workaroundImagePlusNSlices( imp );

			if ( imp == null )
				throw new Exception( "Can't open image: " + tile.getFilePath() );

			final FinalRealInterval intersection = intersect(
					new FinalRealInterval( tile.getPosition(), tile.getMax() ),
					targetInterval );

			final RandomAccessible< T > extendedTile = Views.extendBorder( ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp ) );
			final RealRandomAccessible< T > interpolatedTile = Views.interpolate( extendedTile, interpolatorFactory );
			
			final double[] offset = new double[ targetInterval.numDimensions() ];
			final Translation translation = new Translation( targetInterval.numDimensions() );
			final long[] minIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			final long[] maxIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			for ( int d = 0; d < minIntersectionInTargetInterval.length; ++d )
			{
				final double shiftInTargetInterval = intersection.realMin( d ) - targetInterval.min( d );
				minIntersectionInTargetInterval[ d ] = ( long )Math.floor( shiftInTargetInterval );
				maxIntersectionInTargetInterval[ d ] = ( long )Math.min( Math.ceil( intersection.realMax( d ) - targetInterval.min( d ) ), targetInterval.max( d ) );
				offset[ d ] = tile.getPosition( d ) - targetInterval.min( d );
				translation.set( offset[ d ], d );
			}
			
			final RandomAccessible< T > rasteredInterpolatedTile = Views.raster( RealViews.affine( interpolatedTile, translation ) );
			final IterableInterval< T > sourceInterval =
					Views.flatIterable(
							Views.interval(
								rasteredInterpolatedTile,
								minIntersectionInTargetInterval,
								maxIntersectionInTargetInterval ) );
			final IterableInterval< T > outInterval =
					Views.flatIterable(
							Views.interval(
								out,
								minIntersectionInTargetInterval,
								maxIntersectionInTargetInterval ) );
			final IterableInterval< DoubleType > maxMinDistancesInterval =
					Views.flatIterable(
							Views.interval(
							maxMinDistances,
								minIntersectionInTargetInterval,
								maxIntersectionInTargetInterval ) );
			
			final Cursor< T > sourceCursor = sourceInterval.localizingCursor();
			final Cursor< T > outCursor = outInterval.cursor();
			final Cursor< DoubleType > maxMinDistanceCursor = maxMinDistancesInterval.cursor();
			
			while ( sourceCursor.hasNext() )
			{
				sourceCursor.fwd();
				outCursor.fwd();
				final DoubleType maxMinDistance = maxMinDistanceCursor.next();
				double minDistance = Double.MAX_VALUE;
				for ( int d = 0; d < offset.length; ++d )
				{
					final double cursorPosition = sourceCursor.getDoublePosition( d );
					final double dx = Math.min(
							cursorPosition - offset[ d ],
							tile.getSize( d ) - 1 + offset[ d ] - cursorPosition );
					if ( dx < minDistance ) minDistance = dx;
				}
				if ( minDistance >= maxMinDistance.get() )
				{
					maxMinDistance.set( minDistance );
					outCursor.get().set( sourceCursor.get() );
				}
			}
		}

		return out;
		
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

			final AbstractTranslation translation = ( tile.numDimensions() == 3 ? new Translation3D( tile.getPosition() ) : new Translation2D( tile.getPosition() ) );
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

			final AbstractTranslation translation = ( tile.numDimensions() == 3 ? new Translation3D( tile.getPosition() ) : new Translation2D( tile.getPosition() ) );
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
