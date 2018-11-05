package org.janelia.stitching.math;

import java.util.List;

import net.imglib2.util.RealSum;

public class WeightedCovariance
{
	private final double[] weightedMean;
	private final double[][] weightedCovarianceMatrix;

	public WeightedCovariance( final List< double[] > samples, final double[] weights )
	{
		assert samples.size() == weights.length;
		final int dim = samples.iterator().next().length;

		// make sure that weights are normalized
		final double[] normalizedWeights = new double[ weights.length ];
		final RealSum weightsAccumulatedSum = new RealSum();
		for ( final double weight : weights )
			weightsAccumulatedSum.add( weight );
		final double weightSum = weightsAccumulatedSum.getSum();
		for ( int i = 0; i < weights.length; ++i )
			normalizedWeights[ i ] = weights[ i ] / weightSum;

		// compute weighted mean
		weightedMean = new double[ dim ];
		for ( int i = 0; i < samples.size(); ++i )
		{
			final double[] sample = samples.get( i );
			for ( int d = 0; d < weightedMean.length; ++d )
				weightedMean[ d ] += sample[ d ] * normalizedWeights[ i ];
		}

		// precalculate the denominator for computing covariances
		final RealSum weightSquaresAccumulatedSum = new RealSum();
		for ( final double weight : normalizedWeights )
			weightSquaresAccumulatedSum.add( weight * weight );
		final double covarianceDenomCoeff = 1 - weightSquaresAccumulatedSum.getSum();

		// compute weighted covariance matrix
		weightedCovarianceMatrix = new double[ dim ][ dim ];
		for ( int row = 0; row < dim; ++row )
		{
			for ( int col = row; col < dim; ++col )
			{
				final RealSum sumProduct = new RealSum();
				for ( int i = 0; i < samples.size(); ++i )
				{
					final double[] sample = samples.get( i );
					sumProduct.add( ( sample[ row ] - weightedMean[ row ] ) * ( sample[ col ] - weightedMean[ col ] ) * normalizedWeights[ i ] );
				}
				final double covariance = sumProduct.getSum() / covarianceDenomCoeff;
				weightedCovarianceMatrix[ row ][ col ] = weightedCovarianceMatrix[ col ][ row ] = covariance;
			}
		}
	}

	public double[] getWeightedMean()
	{
		return weightedMean;
	}

	public double[][] getWeightedCovarianceMatrix()
	{
		return weightedCovarianceMatrix;
	}
}
