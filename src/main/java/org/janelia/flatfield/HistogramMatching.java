package org.janelia.flatfield;

import java.util.ArrayList;
import java.util.List;

import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import net.imglib2.histogram.Real1dBinMapper;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.Util;
import net.imglib2.view.composite.RealComposite;

public class HistogramMatching
{
	public static double[] getBinValues( final double histMinValue, final double histMaxValue, final int bins )
	{
		final Real1dBinMapper< DoubleType > binMapper = new Real1dBinMapper<>( histMinValue, histMaxValue, bins, true );
		final double[] binValues = new double[ bins ];
		final DoubleType binCenterValue = new DoubleType();
		for ( int bin = 0; bin < bins; ++bin )
		{
			binMapper.getCenterValue( bin, binCenterValue );
			binValues[ bin ] = binCenterValue.get();
		}
		return binValues;
	}

	@SuppressWarnings( "unchecked" )
	public static < T extends RealType< T > > List< PointMatch > generateHistogramMatches(
			final RealComposite< T > hist1,
			final RealComposite< T > hist2,
			final double[] binValues )
	{
		final int bins = binValues.length;
		final RealComposite< T >[] histograms = new RealComposite[] { hist1, hist2 };

		final double[] histQuantityTotal = new double[ 2 ];
		for ( int i = 0; i < 2; ++i )
			for ( int bin = 0; bin < bins; ++bin )
				histQuantityTotal[ i ] += histograms[ i ].get( bin ).getRealDouble();

		assert Util.isApproxEqual( histQuantityTotal[ 0 ], histQuantityTotal[ 1 ], 1e-10 );

		final double quantityTotal = ( histQuantityTotal[ 0 ] + histQuantityTotal[ 1 ] ) / 2;
		final double quantitySkipLeft = Math.max( hist1.get( 0 ).getRealDouble(), hist2.get( 0 ).getRealDouble() );
		final double quantitySkipRight = Math.max( hist1.get( bins - 1 ).getRealDouble(), hist2.get( bins - 1 ).getRealDouble() );
		double quantityProcessed = 0;

		final List< PointMatch > matches = new ArrayList<>();

		final double[] quantity = new double[] { 0, 0 };
		final int[] index = new int[] { -1, -1 };

		while ( true )
		{
			for ( int i = 0; i < 2; ++i )
			{
				while ( quantity[ i ] <= 0 && index[ i ] < bins - 1 )
					quantity[ i ] = histograms[ i ].get( ++index[ i ] ).getRealDouble();

				// boundary condition
				if ( quantity[ i ] <= 0 && index[ i ] == bins - 1 )
					return matches;
			}

			final double quantityMin = Math.min( quantity[ 0 ], quantity[ 1 ] );

			// ignore the values that are less than minValue or greater than maxValue (i.e. undersaturated/oversaturated values)
			if ( quantityProcessed + quantityMin > quantitySkipLeft && quantityTotal - quantityProcessed > quantitySkipRight )
			{
				assert index[ 0 ] > 0 && index[ 0 ] < bins - 1 && index[ 1 ] > 0 && index[ 1 ] < bins - 1;
				if ( index[ 0 ] > 0 && index[ 0 ] < bins - 1 && index[ 1 ] > 0 && index[ 1 ] < bins - 1 )
				{
					final double weightLeft = quantityMin - Math.max( quantitySkipLeft - quantityProcessed, 0 );
					final double weightRight = Math.min( quantityTotal - quantitySkipRight - quantityProcessed, quantityMin );
					final double weightInner = quantityTotal - quantitySkipRight - quantitySkipLeft;
					final double weight = Math.min( Math.min( weightLeft, weightRight ), weightInner );
					if ( weight > 0 )
						matches.add(
								new PointMatch(
										new Point( new double[] { binValues[ index[ 0 ] ] } ),
										new Point( new double[] { binValues[ index[ 1 ] ] } ),
										weight )
								);
				}
			}

			quantityProcessed += quantityMin;
			for ( int i = 0; i < 2; ++i )
				quantity[ i ] -= quantityMin;
		}
	}
}
