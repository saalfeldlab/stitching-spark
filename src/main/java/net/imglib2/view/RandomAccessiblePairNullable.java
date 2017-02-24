/*
 * #%L
 * ImgLib2: a general-purpose, multidimensional image processing library.
 * %%
 * Copyright (C) 2009 - 2016 Tobias Pietzsch, Stephan Preibisch, Stephan Saalfeld,
 * John Bogovic, Albert Cardona, Barry DeZonia, Christian Dietz, Jan Funke,
 * Aivar Grislis, Jonathan Hale, Grant Harris, Stefan Helfrich, Mark Hiner,
 * Martin Horn, Steffen Jaensch, Lee Kamentsky, Larry Lindsey, Melissa Linkert,
 * Mark Longair, Brian Northan, Nick Perry, Curtis Rueden, Johannes Schindelin,
 * Jean-Yves Tinevez and Michael Zinsmaier.
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package net.imglib2.view;

import net.imglib2.Interval;
import net.imglib2.Localizable;
import net.imglib2.RandomAccessible;
import net.imglib2.util.Pair;

/**
 * A {@link RandomAccessible} over two independent
 * {@link RandomAccessible RandomAccessibles} whose type is the {@link Pair} of
 * corresponding values at the same coordinates in either of the two sources.
 *
 * @author Stephan Saalfeld
 * @author Tobias Pietzsch
 */
public class RandomAccessiblePairNullable< A, B > implements RandomAccessible< Pair< A, B > >
{
	final protected RandomAccessible< A > sourceA;
	final protected RandomAccessible< B > sourceB;

	public class RandomAccess implements Pair< A, B >, net.imglib2.RandomAccess< Pair< A, B > >
	{
		final protected net.imglib2.RandomAccess< A > a;
		final protected net.imglib2.RandomAccess< B > b;

		public RandomAccess()
		{
			a = sourceA != null ? sourceA.randomAccess() : null;
			b = sourceB != null ? sourceB.randomAccess() : null;
		}

		@Override
		public A getA()
		{
			return a != null ? a.get() : null;
		}

		@Override
		public B getB()
		{
			return b != null ? b.get() : null;
		}

		@Override
		public void localize( final int[] position )
		{
			( a != null ? a : b ).localize( position );
		}

		@Override
		public void localize( final long[] position )
		{
			( a != null ? a : b ).localize( position );
		}

		@Override
		public int getIntPosition( final int d )
		{
			return ( a != null ? a : b ).getIntPosition( d );
		}

		@Override
		public long getLongPosition( final int d )
		{
			return ( a != null ? a : b ).getLongPosition( d );
		}

		@Override
		public void localize( final float[] position )
		{
			( a != null ? a : b ).localize( position );
		}

		@Override
		public void localize( final double[] position )
		{
			( a != null ? a : b ).localize( position );
		}

		@Override
		public float getFloatPosition( final int d )
		{
			return ( a != null ? a : b ).getFloatPosition( d );
		}

		@Override
		public double getDoublePosition( final int d )
		{
			return ( a != null ? a : b ).getDoublePosition( d );
		}

		@Override
		public int numDimensions()
		{
			return RandomAccessiblePairNullable.this.numDimensions();
		}

		@Override
		public void fwd( final int d )
		{
			if ( a != null ) a.fwd( d );
			if ( b != null ) b.fwd( d );
		}

		@Override
		public void bck( final int d )
		{
			if ( a != null ) a.bck( d );
			if ( b != null ) b.bck( d );
		}

		@Override
		public void move( final int distance, final int d )
		{
			if ( a != null ) a.move( distance, d );
			if ( b != null ) b.move( distance, d );
		}

		@Override
		public void move( final long distance, final int d )
		{
			if ( a != null ) a.move( distance, d );
			if ( b != null ) b.move( distance, d );
		}

		@Override
		public void move( final Localizable localizable )
		{
			if ( a != null ) a.move( localizable );
			if ( b != null ) b.move( localizable );
		}

		@Override
		public void move( final int[] distance )
		{
			if ( a != null ) a.move( distance );
			if ( b != null ) b.move( distance );
		}

		@Override
		public void move( final long[] distance )
		{
			if ( a != null ) a.move( distance );
			if ( b != null ) b.move( distance );
		}

		@Override
		public void setPosition( final Localizable localizable )
		{
			if ( a != null ) a.setPosition( localizable );
			if ( b != null ) b.setPosition( localizable );
		}

		@Override
		public void setPosition( final int[] position )
		{
			if ( a != null ) a.setPosition( position );
			if ( b != null ) b.setPosition( position );
		}

		@Override
		public void setPosition( final long[] position )
		{
			if ( a != null ) a.setPosition( position );
			if ( b != null ) b.setPosition( position );
		}

		@Override
		public void setPosition( final int position, final int d )
		{
			if ( a != null ) a.setPosition( position, d );
			if ( b != null ) b.setPosition( position, d );
		}

		@Override
		public void setPosition( final long position, final int d )
		{
			if ( a != null ) a.setPosition( position, d );
			if ( b != null ) b.setPosition( position, d );
		}

		@Override
		public RandomAccess get()
		{
			return this;
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

	public RandomAccessiblePairNullable(
			final RandomAccessible< A > sourceA,
			final RandomAccessible< B > sourceB )
	{
		if ( sourceA == null && sourceB == null )
			throw new IllegalArgumentException( "Both sources can't be null at the same time" );

		this.sourceA = sourceA;
		this.sourceB = sourceB;
	}

	@Override
	public int numDimensions()
	{
		return ( sourceA != null ? sourceA : sourceB ).numDimensions();
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
