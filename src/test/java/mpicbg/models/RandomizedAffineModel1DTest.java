package mpicbg.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.janelia.util.Conversions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RandomizedAffineModel1DTest
{
	private static final double DELTA = 1e-4;

	private double[] p, q, w;
	private double[] expectedModel;


	@Before
	public void setUp() throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		final Random rnd = new Random();
		final int n = rnd.nextInt( 998 ) + 2;

		p = new double[ n ];
		q = new double[ n ];
		w = new double[ n ];

		for ( int i = 0; i < n; i++ )
		{
			p[ i ] = rnd.nextDouble() * 1000;
			q[ i ] = rnd.nextDouble() * 1000;
			w[ i ] = rnd.nextDouble() * 1000;
		}

		final AffineModel1D model = new AffineModel1D();
		model.fit( new double[][] { p, new double[ p.length ] }, new double[][] { q, new double[ q.length ] }, w );

		expectedModel = new double[ 2 ];
		model.toArray( expectedModel );
	}


	@Test
	public void testFixedScalingAffineModel1D() throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		testFitDouble ( new FixedScalingAffineModel1D( expectedModel[ 0 ] ), "FixedScalingAffineModel1D" );
		testFitFloat  ( new FixedScalingAffineModel1D( expectedModel[ 0 ] ), "FixedScalingAffineModel1D" );
		testFitMatches( new FixedScalingAffineModel1D( expectedModel[ 0 ] ), "FixedScalingAffineModel1D" );
	}

	@Test
	public void testFixedTranslationAffineModel1D() throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		testFitDouble ( new FixedTranslationAffineModel1D( expectedModel[ 1 ] ), "FixedTranslationAffineModel1D" );
		testFitFloat  ( new FixedTranslationAffineModel1D( expectedModel[ 1 ] ), "FixedTranslationAffineModel1D" );
		testFitMatches( new FixedTranslationAffineModel1D( expectedModel[ 1 ] ), "FixedTranslationAffineModel1D" );
	}


	private < A extends AbstractAffineModel1D< A > > void testFitDouble( final A model, final String modelType ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		model.fit( new double[][] { p, new double[ p.length ] }, new double[][] { q, new double[ q.length ] }, w );
		validateModel( model, modelType, "double" );
	}

	private < A extends AbstractAffineModel1D< A > > void testFitFloat( final A model, final String modelType ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		model.fit( new float[][] { Conversions.toFloatArray( p ), new float[ p.length ] }, new float[][] { Conversions.toFloatArray( q ), new float[ q.length ] }, Conversions.toFloatArray( w ) );
		validateModel( model, modelType, "float" );
	}

	private < A extends AbstractAffineModel1D< A > > void testFitMatches( final A model, final String modelType ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		final Collection< PointMatch > matches = new ArrayList<>();
		for ( int i = 0; i < p.length; i++ )
			matches.add( new PointMatch( new Point( new double[] { p[ i ] } ), new Point( new double[] { q[ i ] } ), w[ i ] ) );
		model.fit( matches );
		validateModel( model, modelType, "matches" );
	}


	private < A extends AbstractAffineModel1D< A > > void validateModel( final A model, final String modelType, final String fitType )
	{
		final double[] m = new double[ 2 ];
		model.toArray( m );
		try
		{
			Assert.assertArrayEquals( expectedModel, m, DELTA );
		}
		catch ( final AssertionError e )
		{
			System.out.println( String.format( "[%s, fit=%s] Expected model = %s, actual model = %s", modelType, fitType, Arrays.toString( expectedModel ), Arrays.toString( m ) ) );
			throw e;
		}
	}
}
