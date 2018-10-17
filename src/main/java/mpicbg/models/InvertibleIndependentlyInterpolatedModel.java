package mpicbg.models;

/**
*
* @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
*/
public abstract class InvertibleIndependentlyInterpolatedModel<
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
}
