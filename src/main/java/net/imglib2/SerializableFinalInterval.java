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

package net.imglib2;

import java.io.Serializable;

/**
 * Implementation of the {@link Interval} interface.
 *
 *
 * @author Tobias Pietzsch
 * @author Stephan Preibisch
 */
public final class SerializableFinalInterval implements Interval, Serializable
{
	private static final long serialVersionUID = 7521588992355710684L;

	/**
	 * the number of dimensions.
	 */
	final protected int n;

	final protected long[] min;

	final protected long[] max;

	/**
	 * Creates an <em>n</em>-dimensional {@link AbstractInterval} with min and
	 * max = 0<sup>n</sup>.
	 *
	 * @param n
	 *            number of dimensions
	 */
	public SerializableFinalInterval( final int n )
	{
		this.n = n;
		this.min = new long[ n ];
		this.max = new long[ n ];
	}

	/**
	 * Creates an Interval from another {@link Interval}
	 *
	 * @param interval
	 *            another {@link Interval}
	 */
	public SerializableFinalInterval( final Interval interval )
	{
		this( interval.numDimensions() );

		interval.min( min );
		interval.max( max );
	}

	/**
	 * Creates an Interval with the boundaries [0, dimensions-1]
	 *
	 * @param dimensions
	 *            the size of the interval
	 */
	public SerializableFinalInterval( final Dimensions dimensions )
	{
		this( dimensions.numDimensions() );
		for ( int d = 0; d < n; ++d )
			this.max[ d ] = dimensions.dimension( d ) - 1;
	}

	/**
	 * Creates an Interval with the boundaries [min, max] (both including)
	 *
	 * @param min
	 *            the position of the first elements in each dimension
	 * @param max
	 *            the position of the last elements in each dimension
	 */
	public SerializableFinalInterval( final long[] min, final long[] max )
	{
		this( min.length );
		assert min.length == max.length;

		for ( int d = 0; d < n; ++d )
		{
			this.min[ d ] = min[ d ];
			this.max[ d ] = max[ d ];
		}
	}

	/**
	 * Creates an Interval with the boundaries [0, dimensions-1]
	 *
	 * @param dimensions
	 *            the size of the interval
	 */
	public SerializableFinalInterval( final long... dimensions )
	{
		this( dimensions.length );
		for ( int d = 0; d < n; ++d )
			this.max[ d ] = dimensions[ d ] - 1;
	}

	/**
	 * Create a {@link SerializableFinalInterval} from a parameter list comprising minimum
	 * coordinates and size. For example, to create a 2D interval from (10, 10)
	 * to (20, 40) use createMinSize( 10, 10, 11, 31 ).
	 *
	 * @param minsize
	 *            a list of <em>2*n</em> parameters to create a <em>n</em>
	 *            -dimensional interval. The first <em>n</em> parameters specify
	 *            the minimum of the interval, the next <em>n</em> parameters
	 *            specify the dimensions of the interval.
	 * @return interval with the specified boundaries
	 */
	public static SerializableFinalInterval createMinSize( final long... minsize )
	{
		final int n = minsize.length / 2;
		final long[] min = new long[ n ];
		final long[] max = new long[ n ];
		for ( int d = 0; d < n; ++d )
		{
			min[ d ] = minsize[ d ];
			max[ d ] = min[ d ] + minsize[ d + n ] - 1;
		}
		return new SerializableFinalInterval( min, max );
	}

	/**
	 * Create a {@link SerializableFinalInterval} from a parameter list comprising minimum
	 * and maximum coordinates. For example, to create a 2D interval from (10,
	 * 10) to (20, 40) use createMinMax( 10, 10, 20, 40 ).
	 *
	 * @param minmax
	 *            a list of <em>2*n</em> parameters to create a <em>n</em>
	 *            -dimensional interval. The first <em>n</em> parameters specify
	 *            the minimum of the interval, the next <em>n</em> parameters
	 *            specify the maximum of the interval.
	 * @return interval with the specified boundaries
	 */
	public static SerializableFinalInterval createMinMax( final long... minmax )
	{
		final int n = minmax.length / 2;
		final long[] min = new long[ n ];
		final long[] max = new long[ n ];
		for ( int d = 0; d < n; ++d )
		{
			min[ d ] = minmax[ d ];
			max[ d ] = minmax[ d + n ];
		}
		return new SerializableFinalInterval( min, max );
	}

	@Override
	public double realMin( final int d )
	{
		assert d >= 0;
		assert d < n;

		return min[ d ];
	}

	@Override
	public void realMin( final double[] minimum )
	{
		assert minimum.length == n;

		for ( int d = 0; d < n; ++d )
			minimum[ d ] = this.min[ d ];
	}

	@Override
	public void realMin( final RealPositionable minimum )
	{
		assert minimum.numDimensions() == n;

		minimum.setPosition( this.min );
	}

	@Override
	public double realMax( final int d )
	{
		assert d >= 0;
		assert d < n;

		return max[ d ];
	}

	@Override
	public void realMax( final double[] maximum )
	{
		assert maximum.length == n;

		for ( int d = 0; d < n; ++d )
			maximum[ d ] = this.max[ d ];
	}

	@Override
	public void realMax( final RealPositionable m )
	{
		assert m.numDimensions() == n;

		m.setPosition( this.max );
	}

	@Override
	public long min( final int d )
	{
		assert d >= 0;
		assert d < n;

		return min[ d ];
	}

	@Override
	public void min( final long[] minimum )
	{
		assert minimum.length == n;

		for ( int d = 0; d < n; ++d )
			minimum[ d ] = this.min[ d ];
	}

	@Override
	public void min( final Positionable m )
	{
		assert m.numDimensions() == n;

		m.setPosition( this.min );
	}

	@Override
	public long max( final int d )
	{
		assert d >= 0;
		assert d < n;

		return max[ d ];
	}

	@Override
	public void max( final long[] maximum )
	{
		assert maximum.length == n;

		for ( int d = 0; d < n; ++d )
			maximum[ d ] = this.max[ d ];
	}

	@Override
	public void max( final Positionable m )
	{
		assert m.numDimensions() == n;

		m.setPosition( this.max );
	}

	@Override
	public void dimensions( final long[] dimensions )
	{
		assert dimensions.length == n;

		for ( int d = 0; d < n; ++d )
			dimensions[ d ] = max[ d ] - min[ d ] + 1;
	}

	@Override
	public long dimension( final int d )
	{
		assert d >= 0;
		assert d < n;

		return max[ d ] - min[ d ] + 1;
	}

	@Override
	public int numDimensions()
	{
		return n;
	}
}
