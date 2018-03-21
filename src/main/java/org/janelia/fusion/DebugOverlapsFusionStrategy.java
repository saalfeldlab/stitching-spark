package org.janelia.fusion;

import org.janelia.stitching.TileInfo;

import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealLocalizable;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

/**
 * Same as min-max-distance, but paints transitions between the tiles black.
 */
public class DebugOverlapsFusionStrategy< T extends RealType< T > & NativeType< T > > extends FusionStrategy< T >
{
	public static class DebugOverlapsFusionResult< T extends RealType< T > & NativeType< T > > extends FusionResult< T >
	{
		private final RandomAccessibleInterval< IntType > tileIndexes;

		public DebugOverlapsFusionResult( final RandomAccessibleInterval< T > out, final RandomAccessibleInterval< IntType > tileIndexes )
		{
			super( out );
			this.tileIndexes = tileIndexes;
		}

		public RandomAccessibleInterval< IntType > getTileIndexesImage()
		{
			return tileIndexes;
		}
	}

	private final RandomAccessibleInterval< FloatType > maxMinDistances;
	private final RandomAccessibleInterval< IntType > tileIndexes;

	private Cursor< FloatType > maxMinDistancesCursor;
	private Cursor< IntType > tileIndexesCursor;
	private Cursor< T > outCursor;

	public DebugOverlapsFusionStrategy( final Interval targetInterval, final T type )
	{
		super( targetInterval, type );
		this.maxMinDistances = ArrayImgs.floats( Intervals.dimensionsAsLongArray( targetInterval ) );
		this.tileIndexes = ArrayImgs.ints( Intervals.dimensionsAsLongArray( targetInterval ) );
	}

	@Override
	public void setCursors( final Interval intersectionIntervalInTargetInterval )
	{
		maxMinDistancesCursor = Views.flatIterable( Views.interval( maxMinDistances, intersectionIntervalInTargetInterval ) ).cursor();
		tileIndexesCursor = Views.flatIterable( Views.interval( tileIndexes, intersectionIntervalInTargetInterval ) ).cursor();
		outCursor = Views.flatIterable( Views.interval( out, intersectionIntervalInTargetInterval ) ).cursor();
	}

	@Override
	public void moveCursorsForward()
	{
		maxMinDistancesCursor.fwd();
		tileIndexesCursor.fwd();
		outCursor.fwd();
	}

	@Override
	public void updateValue( final TileInfo tile, final RealLocalizable pointInsideTile, final T value )
	{
		double minDistance = Double.MAX_VALUE;
		for ( int d = 0; d < pointInsideTile.numDimensions(); ++d )
		{
			final double dist = Math.min(
					pointInsideTile.getDoublePosition( d ),
					tile.getSize( d ) - 1 - pointInsideTile.getDoublePosition( d )
				);
			minDistance = Math.min( dist, minDistance );
		}
		if ( minDistance >= maxMinDistancesCursor.get().getRealDouble() )
		{
			maxMinDistancesCursor.get().setReal( minDistance );
			tileIndexesCursor.get().set( tile.getIndex().intValue() + 1 ); // ensure that zeroes denote empty space
			outCursor.get().set( value );
		}
	}

	@Override
	public DebugOverlapsFusionResult< T > getFusionResult()
	{
		return new DebugOverlapsFusionResult<>( out, tileIndexes );
	}



	private void paintTransitionsBlack()
	{
		final Cursor< T > outCursor = Views.iterable( out ).localizingCursor();
		final RandomAccess< IntType > tileIndexesRandomAccess = Views.extendZero( tileIndexes ).randomAccess();
		final int[] position = new int[ out.numDimensions() ];
		while ( outCursor.hasNext() )
		{
			outCursor.fwd();
			tileIndexesRandomAccess.setPosition( outCursor );
			final int currentTileIndex = tileIndexesRandomAccess.get().get();
			if ( currentTileIndex != 0 )
			{
				// if it is a painted output pixel, check neighboring pixels (6 for 3D, 4 for 2D) to see if it is a transitioning pixel
				boolean isTransitioningPixel = false;

				for ( int d = 0; d < out.numDimensions() && !isTransitioningPixel; ++d )
				{
					for ( final int shift : new int[] { -1, 1 } )
					{
						outCursor.localize( position );
						position[ d ] += shift;
						tileIndexesRandomAccess.setPosition( position );
						final int neighboringTile = tileIndexesRandomAccess.get().get();
						if ( neighboringTile != 0 && neighboringTile != currentTileIndex )
						{
							isTransitioningPixel = true;
							break;
						}
					}
				}

				if ( isTransitioningPixel )
					outCursor.get().setZero();
			}
		}
	}
}