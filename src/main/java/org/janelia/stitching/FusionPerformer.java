package org.janelia.stitching;

import java.util.Arrays;
import java.util.List;

import bdv.export.Downsample;
import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
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
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.RealIntervals;
import net.imglib2.view.Views;

public class FusionPerformer
{

	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses simple pixel copying strategy.
	 */
	/*public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCellSimple( final List< TileInfo > tiles, final TileInfo cell ) throws Exception
	{
		return fuseTilesWithinCell( tiles, cell, null );
	}*/

	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses linear blending strategy on the borders.
	 */
	/*public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCellUsingBlending( final List< TileInfo > tiles, final TileInfo cell ) throws Exception
	{
		final Map< Integer, Dimensions > imageDimensions = new HashMap<>();
		for ( final TileInfo tile : tiles )
			imageDimensions.put( tile.getIndex(), tile.getBoundaries() );

		return fuseTilesWithinCell( tiles, cell, new CustomBlendingPixel( imageDimensions ) );
	}*/


	private static < T extends RealType< T > & NativeType< T > > ImagePlusImg< FloatType, ? > convertToFloat( final RandomAccessibleInterval< T > src )
	{
		final ImagePlusImg< FloatType, ? > dst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( src ) );

		final Cursor< T > srcCursor = Views.flatIterable( src ).cursor();
		final Cursor< FloatType > dstCursor = Views.flatIterable( dst ).cursor();

		while ( srcCursor.hasNext() || dstCursor.hasNext() )
			dstCursor.next().set( srcCursor.next().getRealFloat() );

		return dst;
	}

	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses min-distance strategy within overlapped regions and takes an average when there are more than one value.
	 */
	/*@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static < T extends RealType< T > & NativeType< T > > ImagePlusImg< FloatType, ? > fuseTilesWithinCellUsingMaxMinDistance(
			final List< TileInfo > tiles,
			final Interval targetInterval,
			final Class< ? extends InterpolatorFactory > interpolatorFactoryClass,
			final RandomAccessibleInterval< FloatType > v,
			final RandomAccessibleInterval< FloatType > z ) throws Exception
	{
		final ImageType imageType = Utils.getImageType( tiles );
		if ( imageType == null )
			throw new Exception( "Can't fuse images of different or unknown types" );

		final ImagePlusImg< FloatType, ? > out = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( targetInterval ) );
		final ArrayImg< DoubleType, DoubleArray > maxMinDistances = ArrayImgs.doubles(
				Intervals.dimensionsAsLongArray( targetInterval ) );

		for ( final TileInfo tile : tiles )
		{
			System.out.println( "Loading tile image " + tile.getFilePath() );

			final ImagePlus imp = IJ.openImage( tile.getFilePath() );
			Utils.workaroundImagePlusNSlices( imp );

			final FinalRealInterval intersection = RealIntervals.intersectReal(
					new FinalRealInterval( tile.getPosition(), tile.getMax() ),
					targetInterval );

			final double[] offset = new double[ targetInterval.numDimensions() ];
			final Translation translation = new Translation( targetInterval.numDimensions() );
			final long[] minIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			final long[] maxIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			for ( int d = 0; d < minIntersectionInTargetInterval.length; ++d )
			{
				final double shiftInTargetInterval = intersection.realMin( d ) - targetInterval.min( d );
				minIntersectionInTargetInterval[ d ] = ( long )Math.floor( shiftInTargetInterval );
				maxIntersectionInTargetInterval[ d ] = ( long )Math.min( Math.ceil( intersection.realMax( d ) ), targetInterval.max( d ) ) - targetInterval.min( d );
				offset[ d ] = tile.getPosition( d ) - targetInterval.min( d );
				translation.set( offset[ d ], d );
			}
			final Interval intersectionIntervalInTargetInterval = new FinalInterval( minIntersectionInTargetInterval, maxIntersectionInTargetInterval );

			final RandomAccessibleInterval< T > tileRaw = ImagePlusImgs.from( imp );
			final RandomAccessible< T > tileExtended = Views.extendBorder( tileRaw );
			final RealRandomAccessible< T > tileInterpolated = Views.interpolate( tileExtended, interpolatorFactoryClass.newInstance() );
			final RandomAccessible< T > tileRasteredInterpolated = Views.raster( RealViews.affine( tileInterpolated, translation ) );


			final RandomAccessible< FloatType > vExtended = Views.extendBorder( v );
			final RealRandomAccessible< FloatType > vInterpolated = Views.interpolate( vExtended, interpolatorFactoryClass.newInstance() );
			final RandomAccessible< FloatType > vRasteredInterpolated = Views.raster( RealViews.affine( vInterpolated, translation ) );

			final RandomAccessible< FloatType > zExtended = Views.extendBorder( Views.stack( z ) );
			final RealRandomAccessible< FloatType > zInterpolated = Views.interpolate( zExtended, interpolatorFactoryClass.newInstance() );
			final RandomAccessible< FloatType > zRasteredInterpolated = Views.raster( RealViews.affine( zInterpolated, translation ) );


			final IterableInterval< T > sourceInterval = Views.flatIterable( Views.interval( tileRasteredInterpolated, intersectionIntervalInTargetInterval ) );
			final IterableInterval< FloatType > outInterval = Views.flatIterable( Views.interval( out, intersectionIntervalInTargetInterval ) );
			final IterableInterval< DoubleType > maxMinDistancesInterval = Views.flatIterable( Views.interval( maxMinDistances, intersectionIntervalInTargetInterval ) );

			final Cursor< T > sourceCursor = sourceInterval.localizingCursor();
			final Cursor< FloatType > outCursor = outInterval.cursor();
			final Cursor< DoubleType > maxMinDistanceCursor = maxMinDistancesInterval.cursor();

			while ( sourceCursor.hasNext() || outCursor.hasNext() || maxMinDistanceCursor.hasNext() )
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
	}*/

	public static <
		T extends RealType< T > & NativeType< T >,
		U extends RealType< U > & NativeType< U > >
	ImagePlusImg< FloatType, ? > fuseTilesWithinCellUsingMaxMinDistance(
			final List< TileInfo > tiles,
			final Interval targetInterval,
			final InterpolatorFactory< FloatType, RandomAccessible< FloatType > > interpolatorFactory,
			final RandomAccessibleInterval< U > v,
			final RandomAccessibleInterval< U > z ) throws Exception
	{
		final ImageType imageType = Utils.getImageType( tiles );
		if ( imageType == null )
			throw new Exception( "Can't fuse images of different or unknown types" );

		final ImagePlusImg< FloatType, ? > out = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( targetInterval ) );
		final ArrayImg< DoubleType, DoubleArray > maxMinDistances = ArrayImgs.doubles(
				Intervals.dimensionsAsLongArray( targetInterval ) );

		for ( final TileInfo tile : tiles )
		{
			System.out.println( "Loading tile image " + tile.getFilePath() );

			final ImagePlus imp = IJ.openImage( tile.getFilePath() );
			Utils.workaroundImagePlusNSlices( imp );

			final FinalRealInterval intersection = RealIntervals.intersectReal(
					new FinalRealInterval( tile.getPosition(), tile.getMax() ),
					targetInterval );

			final double[] offset = new double[ targetInterval.numDimensions() ];
			final Translation translation = new Translation( targetInterval.numDimensions() );
			final long[] minIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			final long[] maxIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			for ( int d = 0; d < minIntersectionInTargetInterval.length; ++d )
			{
				final double shiftInTargetInterval = intersection.realMin( d ) - targetInterval.min( d );
				minIntersectionInTargetInterval[ d ] = ( long )Math.floor( shiftInTargetInterval );
				maxIntersectionInTargetInterval[ d ] = ( long )Math.min( Math.ceil( intersection.realMax( d ) ), targetInterval.max( d ) ) - targetInterval.min( d );
				offset[ d ] = tile.getPosition( d ) - targetInterval.min( d );
				translation.set( offset[ d ], d );
			}
			final Interval intersectionIntervalInTargetInterval = new FinalInterval( minIntersectionInTargetInterval, maxIntersectionInTargetInterval );

			final RandomAccessibleInterval< T > rawTile = ImagePlusImgs.from( imp );
			final ImagePlusImg< FloatType, ? > correctedTile =
					( v != null && z != null ) ? IlluminationCorrection.applyCorrection( rawTile, v, z ) : convertToFloat( rawTile );
			final RandomAccessible< FloatType > extendedTile = Views.extendBorder( correctedTile );
			final RealRandomAccessible< FloatType > interpolatedTile = Views.interpolate( extendedTile, interpolatorFactory );
			final RandomAccessible< FloatType > rasteredInterpolatedTile = Views.raster( RealViews.affine( interpolatedTile, translation ) );

			final IterableInterval< FloatType > sourceInterval = Views.flatIterable( Views.interval( rasteredInterpolatedTile, intersectionIntervalInTargetInterval ) );
			final IterableInterval< FloatType > outInterval = Views.flatIterable( Views.interval( out, intersectionIntervalInTargetInterval ) );
			final IterableInterval< DoubleType > maxMinDistancesInterval = Views.flatIterable( Views.interval( maxMinDistances, intersectionIntervalInTargetInterval ) );

			final Cursor< FloatType > sourceCursor = sourceInterval.localizingCursor();
			final Cursor< FloatType > outCursor = outInterval.cursor();
			final Cursor< DoubleType > maxMinDistanceCursor = maxMinDistancesInterval.cursor();

			while ( sourceCursor.hasNext() || outCursor.hasNext() || maxMinDistanceCursor.hasNext() )
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


	// TODO: 'channel' version + virtual image loader was needed for the Zeiss dataset
	/*@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCellUsingMaxMinDistance(
			final List< TileInfo > tiles,
			final Interval targetInterval,
			final InterpolatorFactory< T, RandomAccessible< T > > interpolatorFactory,
			final int channel ) throws Exception
	{
		final ImageType imageType = Utils.getImageType( tiles );
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

			final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
			Utils.workaroundImagePlusNSlices( imp );

			final FinalRealInterval intersection = intersectReal(
					new FinalRealInterval( tile.getPosition(), tile.getMax() ),
					targetInterval );

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

			// TODO: handle other data types
			final VirtualStackImageLoader< T, ?, ? > loader = ( VirtualStackImageLoader ) VirtualStackImageLoader.createUnsignedShortInstance( imp );
			final RandomAccessibleInterval< T > inputRai = loader.getSetupImgLoader( channel ).getImage( 0 );

			final RealRandomAccessible< T > interpolatedTile = Views.interpolate( Views.extendBorder( inputRai ), interpolatorFactory );
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
	}*/


	/*@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCellUsingMaxMinDistance(
			final List< TileInfo > tiles,
			final TileInfo outputCell,
			final InterpolatorFactory< T, RandomAccessible< T > > interpolatorFactory,
			final String exportedPath ) throws Exception
	{
		final Boundaries targetInterval = outputCell.getBoundaries();

		final ImageType imageType = Utils.getImageType( tiles );
		if ( imageType == null )
			throw new Exception( "Can't fuse images of different or unknown types" );

		final ArrayImg< T, ? > out = new ArrayImgFactory< T >().create(
				Intervals.dimensionsAsLongArray( targetInterval ),
				( T )imageType.getType().createVariable() );
		final ArrayImg< DoubleType, DoubleArray > maxMinDistances = ArrayImgs.doubles(
				Intervals.dimensionsAsLongArray( targetInterval ) );

		for ( final TileInfo tile : tiles )
		{
			final String exportedTileCellPath = exportedPath + "/" + String.format( "tile%d-cell%d", tile.getIndex(), outputCell.getIndex() ) + ".tif";
			System.out.println( "Loading exported overlap: " + String.format( "tile%d-cell%d", tile.getIndex(), outputCell.getIndex() ) );

			final ImagePlus imp = IJ.openImage( exportedTileCellPath );
			Utils.workaroundImagePlusNSlices( imp );

			final FinalRealInterval intersection = intersect(
					new FinalRealInterval( tile.getPosition(), tile.getMax() ),
					targetInterval );

		final VirtualStackImageLoader< T, ?, ? > loader = ( VirtualStackImageLoader ) VirtualStackImageLoader.createUnsignedShortInstance( imp );
		final RandomAccessibleInterval< T > inputRai = loader.getSetupImgLoader( channel ).getImage( 0 );

		final RandomAccessible< T > extendedTile = Views.extendBorder( inputRai );
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
}*/


	/**
	 * Performs the fusion of a collection of {@link TileInfo} objects within specified cell.
	 * It uses simple pixel copying strategy then downsamples the resulting image.
	 */
	public static < T extends RealType< T > & NativeType< T > > ImagePlusImg< T, ? > fuseTilesWithinCellSimpleWithDownsampling(
			final List< TileInfo > tiles,
			final TileInfo cell,
			final int[] downsampleFactors ) throws Exception
	{
//		final ImageType imageType = Utils.getImageType( tiles );
//		if ( imageType == null )
//			throw new Exception( "Can't fuse images of different or unknown types" );
		// TODO: can't derive from the tiles anymore since we convert the image to FloatType with illumination correction
		final ImageType imageType = ImageType.GRAY32;

		cell.setType( imageType );
		final T type = ( T ) imageType.getType();
		final ImagePlusImg< T, ? > fusedImg = fuseSimple( tiles, cell, type );

		final long[] outDimensions = new long[ cell.numDimensions() ];
		for ( int d = 0; d < outDimensions.length; d++ )
			outDimensions[ d ] = cell.getSize( d ) / downsampleFactors[ d ];

		final ImagePlusImg< T, ? > downsampledImg = new ImagePlusImgFactory< T >().create( outDimensions, type.createVariable() );
		Downsample.downsample( fusedImg, downsampledImg, downsampleFactors );

		return downsampledImg;
	}

	/*@SuppressWarnings( "unchecked" )
	public static < T extends RealType< T > & NativeType< T > > Img< T > fuseTilesWithinCell( final List< TileInfo > tiles, final TileInfo cell, final PixelFusion pixelStrategy ) throws Exception
	{
		final ImageType imageType = Utils.getImageType( tiles );
		if ( imageType == null )
			throw new Exception( "Can't fuse images of different or unknown types" );

		cell.setType( imageType );
		final T type = ( T ) imageType.getType();
		if ( pixelStrategy == null )
			return fuseSimple( tiles, cell, type );
		else
			return fuseWithPixelStrategy( tiles, cell, type, pixelStrategy );
	}


	@SuppressWarnings( { "unchecked", "rawtypes" } )
	public static < T extends RealType< T > & NativeType< T > > Map< Integer, Img< T > > copyMultipleCells(
			final TileInfo tile,
			final List< TileInfo > cells,
			final int channel ) throws Exception
	{
		System.out.println( "Copying contents of tile " + tile.getIndex() + " into " + cells.size() + " cells.." );

		final T type = ( T ) tile.getType().getType().createVariable();

		// Create output images
		final Map< Integer, Img< T > > outs = new HashMap<>();
		final Map< Integer, RandomAccessibleInterval< T > > cellImgs = new HashMap<>();
		final Map< Long, List< TileInfo > > zSliceToCells = new TreeMap<>();
		for ( final TileInfo cell : cells )
		{
			final Boundaries cellBoundaries = cell.getBoundaries();
			final Img< T > out = new ImagePlusImgFactory< T >().create( cellBoundaries.getDimensions(), type.createVariable() );
			final RandomAccessibleInterval< T > cellImg = Views.translate( out, cellBoundaries.getMin() );

			outs.put( cell.getIndex(), out );
			cellImgs.put( cell.getIndex(), cellImg );

			final long zKey = cell.numDimensions() > 2 ? cellBoundaries.min( 2 ) : 0;
			if ( !zSliceToCells.containsKey( zKey ) )
				zSliceToCells.put( zKey, new ArrayList<>() );
			zSliceToCells.get( zKey ).add( cell );
		}

		System.out.println( "Loading tile image " + tile.getFilePath() );

		final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
		Utils.workaroundImagePlusNSlices( imp );

		final VirtualStackImageLoader< T, ?, ? > loader = ( VirtualStackImageLoader ) VirtualStackImageLoader.createUnsignedShortInstance( imp );
		final RandomAccessibleInterval< T > inputRai = loader.getSetupImgLoader( channel ).getImage( 0 );
		final Boundaries tileBoundaries = tile.getBoundaries();

		System.out.println( "There are " + zSliceToCells.size() + " slice groups of output cells" );
		for ( final Entry< Long, List< TileInfo > > entry : zSliceToCells.entrySet() )
		{
			System.out.println("  processing slice group " + entry.getKey() );
			for ( final TileInfo sliceCell : entry.getValue() )
			{
				final FinalInterval intersection = Intervals.intersect( tileBoundaries, cellImgs.get( sliceCell.getIndex() ) );

				final RealRandomAccessible< T > interpolatedTile = Views.interpolate( Views.extendBorder( inputRai ), new NLinearInterpolatorFactory<>() );

				final AbstractTranslation translation = ( tile.numDimensions() == 3 ? new Translation3D( tile.getPosition() ) : new Translation2D( tile.getPosition() ) );
				final RandomAccessible< T > translatedInterpolatedTile = RealViews.affine( interpolatedTile, translation );

				final IterableInterval< T > tileSource = Views.flatIterable( Views.interval( translatedInterpolatedTile, intersection ) );
				final IterableInterval< T > cellBox = Views.flatIterable( Views.interval( cellImgs.get( sliceCell.getIndex() ), intersection ) );

				final Cursor< T > source = tileSource.cursor();
				final Cursor< T > target = cellBox.cursor();

				while ( source.hasNext() )
					target.next().set( source.next() );
			}
		}

		return outs;
	}*/


	@SuppressWarnings( "unchecked" )
	private static < T extends RealType< T > & NativeType< T > > ImagePlusImg< T, ? > fuseSimple( final List< TileInfo > tiles, final TileInfo cell, final T type ) throws Exception
	{
		System.out.println( "Fusing tiles within cell #" + cell.getIndex() + " of size " + Arrays.toString( cell.getSize() )+"..." );

		// Create output image
		final Boundaries cellBoundaries = cell.getBoundaries();
		final ImagePlusImg< T, ? > out = new ImagePlusImgFactory< T >().create( cellBoundaries.getDimensions(), type.createVariable() );
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

			final RandomAccessibleInterval< T > rawTile = ImagePlusImgs.from( imp );
			final RandomAccessibleInterval< T > correctedDimTile = rawTile.numDimensions() < cell.numDimensions() ? Views.stack( rawTile ) : rawTile;
			final RealRandomAccessible< T > interpolatedTile = Views.interpolate( Views.extendBorder( correctedDimTile ), new NLinearInterpolatorFactory<>() );

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


	/*@SuppressWarnings( "unchecked" )
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
	}*/
}
