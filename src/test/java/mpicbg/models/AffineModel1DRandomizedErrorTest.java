package mpicbg.models;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

public class AffineModel1DRandomizedErrorTest
{
	private static final double EPSILON = 1e-4;

	private final Random rnd = new Random();

	@Test
	public void fixedScalingModelTest() throws NotEnoughDataPointsException
	{
		final double fixedScaling = ( rnd.nextDouble() - 0.5 ) * 100;
		final FixedScalingAffineModel1D model = new FixedScalingAffineModel1D( fixedScaling );
		final List< PointMatch > matches = generateMatches();
		model.fit( matches );

		double error = 0;
		for ( final PointMatch match : matches )
		{
			match.apply( model );
			error += Point.squareDistance( match.getP1(), match.getP2() ) * match.getWeight();
		}

		for ( int iters = 0; iters < 500000; ++iters )
		{
			final double testTranslation = ( rnd.nextDouble() - 0.5 ) * 200;
			final AffineModel1D testModel = new AffineModel1D();
			testModel.set( fixedScaling, testTranslation );

			double testError = 0;
			for ( final PointMatch match : matches )
			{
				match.apply( testModel );
				testError += Point.squareDistance( match.getP1(), match.getP2() ) * match.getWeight();
			}

			if ( testError < error - EPSILON )
				fail( String.format( "[FixedScalingModelTest] Assertion error!\nEstimated model = %s,  error = %f\nTest model = %s,  test error = %f\n",
						Arrays.toString( model.getMatrix( null ) ), error,
						Arrays.toString( testModel.getMatrix( null ) ), testError ) );
		}
	}

	@Test
	public void fixedTranslationModelTest() throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		final double fixedTranslation = ( rnd.nextDouble() - 0.5 ) * 200;
		final FixedTranslationAffineModel1D model = new FixedTranslationAffineModel1D( fixedTranslation );
		final List< PointMatch > matches = generateMatches();
		model.fit( matches );

		double error = 0;
		for ( final PointMatch match : matches )
		{
			match.apply( model );
			error += Point.squareDistance( match.getP1(), match.getP2() ) * match.getWeight();
		}

		for ( int iters = 0; iters < 500000; ++iters )
		{
			final double testScaling = ( rnd.nextDouble() - 0.5 ) * 10;
			final AffineModel1D testModel = new AffineModel1D();
			testModel.set( testScaling, fixedTranslation );

			double testError = 0;
			for ( final PointMatch match : matches )
			{
				match.apply( testModel );
				testError += Point.squareDistance( match.getP1(), match.getP2() ) * match.getWeight();
			}

			if ( testError < error - EPSILON )
				fail( String.format( "[FixedTranslationModelTest] Assertion error!\nEstimated model = %s,  error = %f\nTest model = %s,  test error = %f\n",
						Arrays.toString( model.getMatrix( null ) ), error,
						Arrays.toString( testModel.getMatrix( null ) ), testError ) );
		}
	}

	private List< PointMatch > generateMatches()
	{
		final List< PointMatch > matches = new ArrayList<>();
		final int numMatches = rnd.nextInt( 100 ) + 2;
		for ( int i = 0; i < numMatches; ++i )
			matches.add(
					new PointMatch(
							new Point( new double[] { ( rnd.nextDouble() - 0.5 ) * 100 } ),
							new Point( new double[] { ( rnd.nextDouble() - 0.5 ) * 100 } ),
							rnd.nextDouble() * 1000
						)
				);
		return matches;
	}
}
