package org.janelia.fusion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.janelia.dataaccess.DataProvider;
import org.janelia.stitching.ImageType;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.TransformedTileImageLoader;
import org.janelia.stitching.Utils;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealPoint;
import net.imglib2.img.list.ListImg;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsNullable;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

public class FusionPerformer
{
	public static < T extends RealType< T > & NativeType< T > > FusionResult< T > fuseTilesWithinCell(
			final DataProvider dataProvider,
			final FusionMode mode,
			final List< TileInfo > tilesWithinCell,
			final Interval targetInterval ) throws Exception
	{
		return fuseTilesWithinCell( dataProvider, mode, tilesWithinCell, targetInterval, null );
	}

	public static <
		T extends RealType< T > & NativeType< T >,
		U extends RealType< U > & NativeType< U > >
	FusionResult< T > fuseTilesWithinCell(
			final DataProvider dataProvider,
			final FusionMode mode,
			final List< TileInfo > tilesWithinCell,
			final Interval targetInterval,
			final RandomAccessiblePairNullable< U, U > flatfield ) throws Exception
	{
		return fuseTilesWithinCell( dataProvider, mode, tilesWithinCell, targetInterval, flatfield, null );
	}

	/**
	 * Fuses a collection of {@link TileInfo} objects within the given target interval using specified strategy in overlaps.
	 *
	 * @param dataProvider
	 * 			A backend storage client
	 * @param mode
	 * 			Required strategy to be used for rendering overlaps
	 * @param tilesWithinCell
	 * 			A list of tiles that have non-empty intersection with the target interval in the transformed space
	 * @param targetInterval
	 * 			An output fusion cell in the transformed space
	 * @param flatfieldCorrection
	 * 			Optional flatfield correction coefficients (can be null)
	 * @param pairwiseConnectionsMap
	 * 			Optional connectivity map specifying the overlaps that have been used to obtain the tile transforms (can be null)
	 */
	public static <
		T extends RealType< T > & NativeType< T >,
		U extends RealType< U > & NativeType< U >,
		R extends RealType< R > & NativeType< R > >
	FusionResult< T > fuseTilesWithinCell(
			final DataProvider dataProvider,
			final FusionMode mode,
			final List< TileInfo > tilesWithinCell,
			final Interval targetInterval,
			final RandomAccessiblePairNullable< U, U > flatfield,
			final Map< Integer, Set< Integer > > pairwiseConnectionsMap ) throws Exception
	{
		final ImageType imageType = Utils.getImageType( tilesWithinCell );
		if ( imageType == null )
			throw new Exception( "Can't fuse images of different or unknown types" );

		@SuppressWarnings( "unchecked" )
		final T type = ( T ) imageType.getType();

		final FusionStrategy< T > fusionStrategy = FusionStrategyFactory.createFusionStrategy( mode, targetInterval, type );

		// initialize helper image for tile connections when exporting only overlaps
		final RandomAccessibleInterval< Set< Integer > > tileIndexes;
		if ( pairwiseConnectionsMap != null )
		{
			final int numElements = ( int ) Intervals.numElements( targetInterval );
			final List< Set< Integer > > tileIndexesList = new ArrayList<>( numElements );
			for ( int i = 0; i < numElements; ++i )
				tileIndexesList.add( new HashSet<>() );
			tileIndexes = new ListImg<>( tileIndexesList, Intervals.dimensionsAsLongArray( targetInterval ) );
		}
		else
		{
			tileIndexes = null;
		}

		for ( final TileInfo tile : tilesWithinCell )
		{
			final InvertibleRealTransform tileTransform = TileOperations.getTileTransform( tile );
			final RandomAccessibleInterval< T > transformedTileImg = TransformedTileImageLoader.loadTile(
					tile,
					dataProvider,
					Optional.ofNullable( flatfield )
				);

			final FinalRealInterval intersection = IntervalsNullable.intersectReal( transformedTileImg, targetInterval );
			if ( intersection == null )
				throw new IllegalArgumentException( "tilesWithinCell contains a tile that doesn't intersect with the target interval: " + "Transformed tile " + tile.getIndex() +  "; " + "Output cell " + " at " + Arrays.toString( Intervals.minAsIntArray( targetInterval ) ) + " of size " + Arrays.toString( Intervals.dimensionsAsIntArray( targetInterval ) ) );

			final long[] offset = new long[ targetInterval.numDimensions() ];
			final long[] minIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			final long[] maxIntersectionInTargetInterval = new long[ targetInterval.numDimensions() ];
			for ( int d = 0; d < minIntersectionInTargetInterval.length; ++d )
			{
				offset[ d ] = transformedTileImg.min( d ) - targetInterval.min( d );
				minIntersectionInTargetInterval[ d ] = ( long ) Math.floor( intersection.realMin( d ) ) - targetInterval.min( d );
				maxIntersectionInTargetInterval[ d ] = ( long ) Math.ceil ( intersection.realMax( d ) ) - targetInterval.min( d );
			}
			final Interval intersectionIntervalInTargetInterval = new FinalInterval( minIntersectionInTargetInterval, maxIntersectionInTargetInterval );

			fusionStrategy.setCursors( intersectionIntervalInTargetInterval );

			final RandomAccessibleInterval< T > transformedTileInTargetSpace = Views.offset( transformedTileImg, Intervals.minAsLongArray( targetInterval ) );
			final RandomAccessibleInterval< T > sourceInterval = Views.interval( transformedTileInTargetSpace, intersectionIntervalInTargetInterval );
			final Cursor< T > sourceCursor = Views.flatIterable( sourceInterval ).localizingCursor();

			final RandomAccessibleInterval< Set< Integer > > tileIndexesInterval = tileIndexes != null ? Views.interval( tileIndexes, intersectionIntervalInTargetInterval ) : null;
			final Cursor< Set< Integer > > tileIndexesCursor = tileIndexesInterval != null ? Views.flatIterable( tileIndexesInterval ).cursor() : null;

			final T zero = type.createVariable();
			zero.setZero();

			final double[] globalPosition = new double[ targetInterval.numDimensions() ];
			final double[] localTilePosition = new double[ targetInterval.numDimensions() ];
			final RealPoint pointInsideTile = new RealPoint( targetInterval.numDimensions() );

			while ( sourceCursor.hasNext() )
			{
				// move all cursors forward
				fusionStrategy.moveCursorsForward();
				sourceCursor.fwd();
				if ( tileIndexesCursor != null )
					tileIndexesCursor.fwd();

				if ( sourceCursor.get().valueEquals( zero ) )
					continue;

				// get global position
				sourceCursor.localize( globalPosition );
				for ( int d = 0; d < globalPosition.length; ++d )
					globalPosition[ d ] += targetInterval.min( d );

				// get local tile position
				tileTransform.applyInverse( localTilePosition, globalPosition );

				boolean isInsideTile = true;
				for ( int d = 0; d < localTilePosition.length; ++d )
					isInsideTile &= localTilePosition[ d ] >= 0 && localTilePosition[ d ] <= tile.getSize( d ) - 1;
				if ( !isInsideTile )
					continue;

				// update the value if the point is inside tile
				pointInsideTile.setPosition( localTilePosition );
				fusionStrategy.updateValue( tile, pointInsideTile, sourceCursor.get() );

				if ( tileIndexesCursor != null )
					tileIndexesCursor.get().add( tile.getIndex() );
			}
		}

		final FusionResult< T > fusionResult = fusionStrategy.getFusionResult();

		// retain only requested content within overlaps that corresponds to pairwise connections map, if required
		if ( tileIndexes != null )
		{
			final Cursor< T > outCursor = Views.flatIterable( fusionResult.getOutImage() ).cursor();
			final Cursor< Set< Integer > > tileIndexesCursor = Views.flatIterable( tileIndexes ).cursor();
			while ( outCursor.hasNext() || tileIndexesCursor.hasNext() )
			{
				outCursor.fwd();
				tileIndexesCursor.fwd();

				boolean retainPixel = false;
				final Set< Integer > tilesAtPoint = tileIndexesCursor.get();
				for ( final Integer testTileIndex : tilesAtPoint )
				{
					final Set< Integer > connectedTileIndexes = pairwiseConnectionsMap.get( testTileIndex );
					if ( connectedTileIndexes != null && !Collections.disjoint( tilesAtPoint, connectedTileIndexes ) )
					{
						retainPixel = true;
						break;
					}
				}

				if ( !retainPixel )
					outCursor.get().setZero();
			}
		}

		return fusionResult;
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
}
