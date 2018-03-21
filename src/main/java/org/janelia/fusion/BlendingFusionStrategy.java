package org.janelia.fusion;

import org.janelia.stitching.TileInfo;

import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealLocalizable;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class BlendingFusionStrategy< T extends RealType< T > & NativeType< T > > extends FusionStrategy< T >
{
	private final static double FRACTION_BLENDED = 0.2;

	private final RandomAccessibleInterval< FloatType > weights;
	private final RandomAccessibleInterval< FloatType > values;
	private final double fractionBlended;

	private Cursor< FloatType > weightsCursor;
	private Cursor< FloatType > valuesCursor;
	private FusionResult< T > result;

	public BlendingFusionStrategy( final Interval targetInterval, final T type )
	{
		this( targetInterval, type, FRACTION_BLENDED );
	}

	public BlendingFusionStrategy( final Interval targetInterval, final T type, final double fractionBlended )
	{
		super( targetInterval, type );
		this.weights = ArrayImgs.floats( Intervals.dimensionsAsLongArray( targetInterval ) );
		this.values = ArrayImgs.floats( Intervals.dimensionsAsLongArray( targetInterval ) );
		this.fractionBlended = fractionBlended;
	}

	@Override
	public void setCursors( final Interval intersectionIntervalInTargetInterval )
	{
		weightsCursor = Views.flatIterable( Views.interval( weights, intersectionIntervalInTargetInterval ) ).cursor();
		valuesCursor = Views.flatIterable( Views.interval( values, intersectionIntervalInTargetInterval ) ).cursor();
	}

	@Override
	public void moveCursorsForward()
	{
		weightsCursor.fwd();
		valuesCursor.fwd();
	}

	@Override
	public void updateValue( final TileInfo tile, final RealLocalizable pointInsideTile, final T value )
	{
		if ( result != null )
			throw new IllegalStateException( "Populating out image after it has been filled" );

		final double weight = getBlendingWeight( pointInsideTile, tile.getSize() );
		weightsCursor.get().setReal( weightsCursor.get().getRealDouble() + weight );
		valuesCursor.get().setReal( valuesCursor.get().getRealDouble() + value.getRealDouble() * weight );
	}

	@Override
	public FusionResult< T > getFusionResult()
	{
		if ( result != null )
			return result;

		setCursors( out );
		final Cursor< T > outCursor = Views.flatIterable( out ).cursor();
		while ( outCursor.hasNext() || weightsCursor.hasNext() || valuesCursor.hasNext() )
		{
			final double weight = weightsCursor.next().getRealDouble();
			final double value = valuesCursor.next().getRealDouble();
			outCursor.next().setReal( weight == 0 ? 0 : value / weight );
		}
		result = new FusionResult<>( out );
		return result;
	}

	private double getBlendingWeight( final RealLocalizable pointInsideTile, final long[] tileDimensions )
	{
		// compute multiplicative distance to the respective borders [0...1]
		double minDistance = 1;

		for ( int d = 0; d < pointInsideTile.numDimensions(); ++d )
		{
			// the distance to the border that is closer
			double value = Math.max( 1, Math.min( pointInsideTile.getDoublePosition( d ), tileDimensions[ d ] - 1 - pointInsideTile.getDoublePosition( d ) ) );

			final float imgAreaBlend = Math.round( fractionBlended * 0.5f * ( tileDimensions[ d ] - 1 ) );

			if ( value < imgAreaBlend )
				value = value / imgAreaBlend;
			else
				value = 1;

			minDistance *= value;
		}

		if ( minDistance == 1 )
			return 1;
		else if ( minDistance <= 0 )
			return 0.0000001;
		else
			return ( Math.cos( (1 - minDistance) * Math.PI ) + 1 ) / 2;
	}
}