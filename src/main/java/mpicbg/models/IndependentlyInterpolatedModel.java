package mpicbg.models;

import java.util.Collection;

/**
*
* @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
*/
public class IndependentlyInterpolatedModel< A extends Model< A >, B extends Model< B >, M extends IndependentlyInterpolatedModel< A, B, M > > extends AbstractModel< M >
{
	private static final long serialVersionUID = -3975559034238200788L;

	final protected A a;
	final protected B b;
	protected double[] lambdas;

	public IndependentlyInterpolatedModel( final A a, final B b, final double... lambdas )
	{
		this.a = a;
		this.b = b;
		this.lambdas = lambdas;
	}

	public A getA()
	{
		return a;
	}

	public B getB()
	{
		return b;
	}

	public double[] getLambdas()
	{
		return lambdas;
	}

	public void setLambdas( final double... lambdas )
	{
		this.lambdas = lambdas;
	}

	@Override
	public int getMinNumMatches()
	{
		return Math.max( a.getMinNumMatches(), b.getMinNumMatches() );
	}

	@Override
	public < P extends PointMatch > void fit( final Collection< P > matches ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		a.fit( matches );
		b.fit( matches );
	}

	@Override
	public void set( final M m )
	{
		a.set( m.a );
		b.set( m.b );
		lambdas = m.lambdas.clone();
		cost = m.cost;
	}

	@Override
	public M copy()
	{
		@SuppressWarnings( "unchecked" )
		final M copy = ( M )new IndependentlyInterpolatedModel< A, B, M >( a.copy(), b.copy(), lambdas.clone() );
		copy.cost = cost;
		return copy;
	}

	@Override
	public double[] apply( final double[] location )
	{
		final double[] copy = location.clone();
		applyInPlace( copy );
		return copy;
	}

	@Override
	public void applyInPlace( final double[] location )
	{
		final double[] copy = b.apply( location );
		a.applyInPlace( location );

		for ( int d = 0; d < location.length; ++d )
		{
			final double dd = copy[ d ] - location[ d ];
			location[ d ] += lambdas[ d ] * dd;
		}
	}
}
