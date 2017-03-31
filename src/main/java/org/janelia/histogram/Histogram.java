package org.janelia.histogram;

import java.io.Serializable;

public class Histogram implements Serializable
{
	private static final long serialVersionUID = -3130834243396947444L;

	private final double[] histogram;
	private final double minValue, maxValue, binWidth;
	private double quantityLessThanMin, quantityGreaterThanMax, quantityTotal;

	public Histogram( final double minValue, final double maxValue, final int bins )
	{
		assert minValue < maxValue;
		this.histogram = new double[ bins ];
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.binWidth = ( maxValue - minValue ) / bins;
	}

	public double[] getHistogram() { return histogram; }
	public int getNumBins() { return histogram.length; }
	public double getMinValue() { return minValue; }
	public double getMaxValue() { return maxValue; }
	public double getBinWidth() { return binWidth; }
	public double getQuantityLessThanMin() { return quantityLessThanMin; }
	public double getQuantityGreaterThanMax() { return quantityGreaterThanMax; }
	public double getQuantityTotal() { return quantityTotal; }

	public double get( final int bin )
	{
		return histogram[ bin ];
	}

	public void set( final int bin, final double quantity )
	{
		quantityTotal += quantity - histogram[ bin ];
		histogram[ bin ] = quantity;

		if ( bin <= 0 )
			quantityLessThanMin = 0;

		if ( bin >= histogram.length - 1 )
			quantityGreaterThanMax = 0;
	}

	public void put( final double value )
	{
		put( value, 1 );
	}
	public void put( final double value, final double quantity )
	{
		final int bin;
		if ( value < minValue )
		{
			bin = 0;
			quantityLessThanMin += quantity;
		}
		else if ( value >= maxValue )
		{
			bin = histogram.length - 1;
			quantityGreaterThanMax += quantity;
		}
		else
		{
			bin = ( int ) Math.floor( ( value - minValue ) / binWidth );
		}
		histogram[ bin ] += quantity;
		quantityTotal += quantity;
	}

	public double getBinValue( final int bin )
	{
		return minValue + bin * binWidth;
	}
}
