package org.janelia.histogram;

import java.util.ArrayList;
import java.util.List;

import mpicbg.models.Point;
import mpicbg.models.PointMatch;

public class HistogramsMatching
{
	private static final double EPSILON = 1e-10;

	public static List< PointMatch > generateHistogramMatches( final Histogram hist1, final Histogram hist2 )
	{
		if ( Math.abs( hist1.getQuantityTotal() - hist2.getQuantityTotal() ) >= EPSILON )
			throw new IllegalArgumentException( "Different total quantity" );

		final double quantityTotal = ( hist1.getQuantityTotal() + hist2.getQuantityTotal() ) / 2;
		final double quantitySkipLeft = Math.max( hist1.getQuantityLessThanMin(), hist2.getQuantityLessThanMin() );
		final double quantitySkipRight = Math.max( hist1.getQuantityGreaterThanMax(), hist2.getQuantityGreaterThanMax() );
		double quantityProcessed = 0;

		final List< PointMatch > matches = new ArrayList<>();
		final Histogram[] histograms = new Histogram[] { hist1, hist2 };
		final double[] quantity = new double[] { 0, 0 };
		final int[] index = new int[] { -1, -1 };

		while ( true )
		{
			for ( int i = 0; i < 2; ++i )
			{
				while ( quantity[ i ] < EPSILON && index[ i ] < histograms[ i ].getNumBins() - 1 )
					quantity[ i ] = histograms[ i ].get( ++index[ i ] );

				// boundary condition
				if ( quantity[ i ] < EPSILON && index[ i ] == histograms[ i ].getNumBins() - 1 )
					return matches;
			}

			final double quantityMin = Math.min( quantity[ 0 ], quantity[ 1 ] );

			// ignore the values that are less than minValue or greater than maxValue (i.e. undersaturated/oversaturated values)
			if ( quantityProcessed + quantityMin > quantitySkipLeft && quantityTotal - quantityProcessed > quantitySkipRight )
			{
				final double weightLeft = quantityMin - Math.max( quantitySkipLeft - quantityProcessed, 0 );
				final double weightRight = Math.min( quantityTotal - quantitySkipRight - quantityProcessed, quantityMin );
				final double weightInner = quantityTotal - quantitySkipRight - quantitySkipLeft;
				final double weight = Math.min( Math.min( weightLeft, weightRight ), weightInner );
				if ( weight >= EPSILON )
					matches.add(
							new PointMatch(
									new Point( new double[] { histograms[ 0 ].getBinValue( index[ 0 ] ) } ),
									new Point( new double[] { histograms[ 1 ].getBinValue( index[ 1 ] ) } ),
									weight )
							);
			}

			quantityProcessed += quantityMin;
			for ( int i = 0; i < 2; ++i )
				quantity[ i ] -= quantityMin;
		}
	}
}
