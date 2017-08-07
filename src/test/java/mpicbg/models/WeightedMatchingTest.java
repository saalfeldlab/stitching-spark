package mpicbg.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.janelia.util.Conversions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WeightedMatchingTest
{
	private static final double DELTA = 1e-9;

	private double[] p, q, w;
	private double[] expectedModel;


	@Before
	public void setUp() throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		final Random rnd = new Random();
		final int n = rnd.nextInt( 200 ) + 3;
		p = new double[ n ];
		q = new double[ n ];
		w = new double[ n ];
		for ( int i = 0; i < n; i++ )
		{
			p[ i ] = rnd.nextInt( 10 ) + 2;
			q[ i ] = rnd.nextInt( 10 ) + 2;
			w[ i ] = rnd.nextInt( 10 ) + 2;
		}

		// unroll to estimate expected model
		int total = 0;
		for ( int i = 0; i < n; i++ )
			total += w[ i ];
		final double[] pu = new double[ total ], qu = new double[ total ], wu = new double[ total ];
		Arrays.fill( wu, 1 );
		for ( int i = 0, j = 0; i < n; i++ )
		{
			for ( int k = 0; k < w[ i ]; k++, j++ )
			{
				pu[ j ] = p[ i ];
				qu[ j ] = q[ i ];
			}
		}
		final AffineModel1D model = new AffineModel1D();
		model.fit( new double[][] { pu, new double[ pu.length ] }, new double[][] { qu, new double[ qu.length ] }, wu );
		expectedModel = new double[ 2 ];
		model.toArray( expectedModel );
	}


	@Test
	public void testAffineModel1D() throws Exception
	{
		testFitDouble ( new AffineModel1D(), "AffineModel1D" );
		testFitFloat  ( new AffineModel1D(), "AffineModel1D" );
		testFitMatches( new AffineModel1D(), "AffineModel1D" );
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
