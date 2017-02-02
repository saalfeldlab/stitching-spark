package mpicbg.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.janelia.util.Conversions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AffineModel1DTest
{
	private static final double DELTA = 1e-15;

	private double[] p, q, w;
	private double[] expectedModel;


	@Before
	public void setUp()
	{
		p = new double[] { 1, 10, 7, 50 };
		q = new double[] { 5, 32, 23, 152 };

		w = new double[ p.length ];
		Arrays.fill( w, 1 );

		expectedModel = new double[] { 3, 2 };
	}


	@Test
	public void testAffineModel1D() throws Exception
	{
		testFitDouble ( new AffineModel1D(), "AffineModel1D" );
		testFitFloat  ( new AffineModel1D(), "AffineModel1D" );
		testFitMatches( new AffineModel1D(), "AffineModel1D" );
	}

	@Test
	public void testFixedScalingAffineModel1D() throws Exception
	{
		testFitDouble ( new FixedScalingAffineModel1D( expectedModel[ 0 ] ), "FixedScalingAffineModel1D" );
		testFitFloat  ( new FixedScalingAffineModel1D( expectedModel[ 0 ] ), "FixedScalingAffineModel1D" );
		testFitMatches( new FixedScalingAffineModel1D( expectedModel[ 0 ] ), "FixedScalingAffineModel1D" );
	}

	@Test
	public void testFixedTranslationAffineModel1D() throws Exception
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
