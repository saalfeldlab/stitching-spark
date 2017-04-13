package org.janelia.flatfield;

import net.imglib2.Interval;
import net.imglib2.Localizable;
import net.imglib2.RandomAccessible;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.RandomAccessiblePair;

public class FlatfieldCorrectedRandomAccessible< T extends RealType< T >, U extends RealType< U > > implements RandomAccessible< U >
{
	final protected RandomAccessible< T > source;
	final protected RandomAccessiblePair< U, U > flatfield;

	public class RandomAccess implements net.imglib2.RandomAccess< U >
	{
		final protected net.imglib2.RandomAccess< T > sourceRandomAccess;
		final protected RandomAccessiblePair< U, U >.RandomAccess flatfieldRandomAccess;
		final protected U value;

		public RandomAccess()
		{
			sourceRandomAccess = source.randomAccess();
			flatfieldRandomAccess = flatfield.randomAccess();
			value = flatfieldRandomAccess.getA().createVariable();
		}

		@Override
		public void localize( final int[] position )
		{
			sourceRandomAccess.localize( position );
		}

		@Override
		public void localize( final long[] position )
		{
			sourceRandomAccess.localize( position );
		}

		@Override
		public int getIntPosition( final int d )
		{
			return sourceRandomAccess.getIntPosition( d );
		}

		@Override
		public long getLongPosition( final int d )
		{
			return sourceRandomAccess.getLongPosition( d );
		}

		@Override
		public void localize( final float[] position )
		{
			sourceRandomAccess.localize( position );
		}

		@Override
		public void localize( final double[] position )
		{
			sourceRandomAccess.localize( position );
		}

		@Override
		public float getFloatPosition( final int d )
		{
			return sourceRandomAccess.getFloatPosition( d );
		}

		@Override
		public double getDoublePosition( final int d )
		{
			return sourceRandomAccess.getDoublePosition( d );
		}

		@Override
		public int numDimensions()
		{
			return FlatfieldCorrectedRandomAccessible.this.numDimensions();
		}

		@Override
		public void fwd( final int d )
		{
			sourceRandomAccess.fwd( d );
			flatfieldRandomAccess.fwd( d );
		}

		@Override
		public void bck( final int d )
		{
			sourceRandomAccess.bck( d );
			flatfieldRandomAccess.bck( d );
		}

		@Override
		public void move( final int distance, final int d )
		{
			sourceRandomAccess.move( distance, d );
			flatfieldRandomAccess.move( distance, d );
		}

		@Override
		public void move( final long distance, final int d )
		{
			sourceRandomAccess.move( distance, d );
			flatfieldRandomAccess.move( distance, d );
		}

		@Override
		public void move( final Localizable localizable )
		{
			sourceRandomAccess.move( localizable );
			flatfieldRandomAccess.move( localizable );
		}

		@Override
		public void move( final int[] distance )
		{
			sourceRandomAccess.move( distance );
			flatfieldRandomAccess.move( distance );
		}

		@Override
		public void move( final long[] distance )
		{
			sourceRandomAccess.move( distance );
			flatfieldRandomAccess.move( distance );
		}

		@Override
		public void setPosition( final Localizable localizable )
		{
			sourceRandomAccess.setPosition( localizable );
			flatfieldRandomAccess.setPosition( localizable );
		}

		@Override
		public void setPosition( final int[] position )
		{
			sourceRandomAccess.setPosition( position );
			flatfieldRandomAccess.setPosition( position );
		}

		@Override
		public void setPosition( final long[] position )
		{
			sourceRandomAccess.setPosition( position );
			flatfieldRandomAccess.setPosition( position );
		}

		@Override
		public void setPosition( final int position, final int d )
		{
			sourceRandomAccess.setPosition( position, d );
			flatfieldRandomAccess.setPosition( position, d );
		}

		@Override
		public void setPosition( final long position, final int d )
		{
			sourceRandomAccess.setPosition( position, d );
			flatfieldRandomAccess.setPosition( position, d );
		}

		@Override
		public U get()
		{
			value.setReal( sourceRandomAccess.get().getRealDouble() * flatfieldRandomAccess.getA().getRealDouble() + flatfieldRandomAccess.getB().getRealDouble() );
			return value;
		}

		@Override
		public RandomAccess copy()
		{
			final RandomAccess copy = new RandomAccess();
			copy.setPosition( this );
			return copy;
		}

		@Override
		public RandomAccess copyRandomAccess()
		{
			return copy();
		}
	}

	public FlatfieldCorrectedRandomAccessible(
			final RandomAccessible< T > source,
			final RandomAccessiblePair< U, U > flatfield )
	{
		this.source = source;
		this.flatfield = flatfield;
	}

	@Override
	public int numDimensions()
	{
		return source.numDimensions();
	}

	@Override
	public RandomAccess randomAccess()
	{
		return new RandomAccess();
	}

	@Override
	public RandomAccess randomAccess( final Interval interval )
	{
		return new RandomAccess();
	}
}
