package org.janelia.stitching;

import java.util.Arrays;
import java.util.List;

import net.imglib2.RealPoint;

public class SampleWeightCalculator
{
	private final boolean weighted;

	public SampleWeightCalculator( final boolean weighted )
	{
		this.weighted = weighted;
	}

	public double[] calculateSampleWeights( final RealPoint point, final List< RealPoint > samples )
	{
		final double[] sampleWeights = new double[ samples.size() ];

		// calculate weights
		if ( weighted )
		{
			for ( int i = 0; i < sampleWeights.length; ++i )
			{
				double distanceSqr = 0;
				for ( int d = 0; d < Math.max( point.numDimensions(), samples.get( i ).numDimensions() ); ++d )
					distanceSqr += Math.pow( point.getDoublePosition( d ) - samples.get( i ).getDoublePosition( d ), 2 );

				if ( distanceSqr < 1 )
					throw new IllegalArgumentException( "The distance between the points is too small" );

				sampleWeights[ i ] = 1. / distanceSqr;
			}
		}
		else
		{
			Arrays.fill( sampleWeights, 1 );
		}

		// normalize weights
		double sumWeights = 0;
		for ( final double weight : sampleWeights )
			sumWeights += weight;
		for ( int i = 0; i < sampleWeights.length; ++i )
			sampleWeights[ i ] /= sumWeights;

		return sampleWeights;
	}
}
