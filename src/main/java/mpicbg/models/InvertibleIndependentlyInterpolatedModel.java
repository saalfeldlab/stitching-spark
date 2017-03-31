package mpicbg.models;

/**
*
* @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
*/
public class InvertibleIndependentlyInterpolatedModel<
		A extends Model< A > & InvertibleCoordinateTransform,
		B extends Model< B > & InvertibleCoordinateTransform,
		M extends InvertibleIndependentlyInterpolatedModel< A, B, M > > extends IndependentlyInterpolatedModel< A, B, M > implements InvertibleCoordinateTransform
{
	private static final long serialVersionUID = 4472796413007281164L;

	public InvertibleIndependentlyInterpolatedModel( final A a, final B b, final double... lambdas )
	{
		super( a, b, lambdas );
	}

	@Override
	public M copy()
	{
		@SuppressWarnings( "unchecked" )
		final M copy = ( M )new InvertibleIndependentlyInterpolatedModel< A, B, M >( a.copy(), b.copy(), lambdas.clone() );
		copy.cost = cost;
		return copy;
	}

	@Override
	public double[] applyInverse( final double[] point ) throws NoninvertibleModelException
	{
		final double[] copy = point.clone();
		applyInverseInPlace( copy );
		return copy;
	}

	@Override
	public void applyInverseInPlace( final double[] point ) throws NoninvertibleModelException
	{
		final double[] copy = b.applyInverse( point );
		a.applyInverseInPlace( point );

		for ( int d = 0; d < point.length; ++d )
		{
			final double dd = copy[ d ] - point[ d ];
			point[ d ] += lambdas[ d ] * dd;
		}
	}

	@Override
	public InvertibleCoordinateTransform createInverse()
	{
		@SuppressWarnings( "unchecked" )
		final InvertibleIndependentlyInterpolatedModel< A, B, M > inverse = new InvertibleIndependentlyInterpolatedModel< >( ( A )a.createInverse(), ( B )b.createInverse(), lambdas.clone() );
		inverse.cost = cost;
		return inverse;
	}
}
