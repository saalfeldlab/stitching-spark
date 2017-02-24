package org.janelia.flatfield;

import java.io.Serializable;

public class HistogramSettings implements Serializable
{
	private static final long serialVersionUID = -3256954929451071880L;

	public final int minValue, maxValue, bins;

	public HistogramSettings( final int minValue, final int maxValue, final int bins )
	{
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.bins = bins;
	}

	public double getBinWidth()
	{
		return getBinWidth( minValue, maxValue, bins );
	}
	public static double getBinWidth( final int minValue, final int maxValue, final int bins )
	{
		return ( maxValue - minValue + 1 ) / ( double ) bins;
	}

	public int getBinIndex( final int value )
	{
		return getBinIndex( value, minValue, maxValue, bins );
	}
	public static int getBinIndex( final int value, final int minValue, final int maxValue, final int bins )
	{
		if ( value < minValue )
			return 0;
		else if ( value > maxValue )
			return bins - 1;

		return ( int ) ( ( value - minValue ) / getBinWidth( minValue, maxValue, bins ) );
	}

	public int getBinValue( final int index )
	{
		return getBinValue( index, minValue, maxValue, bins );
	}
	public static int getBinValue( final int index, final int minValue, final int maxValue, final int bins )
	{
		return ( int ) ( minValue + ( index + 0.5 ) * getBinWidth( minValue, maxValue, bins ) );
	}
}
