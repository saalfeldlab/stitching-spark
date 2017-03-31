package mpicbg.models;

import java.util.Collection;

/**
*
* @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
*/
final public class IndependentlyInterpolatedAffineModel1D<
		A extends Model< A > & Affine1D< A >,
		B extends Model< B > & Affine1D< B > >
	extends InvertibleIndependentlyInterpolatedModel< A, B, IndependentlyInterpolatedAffineModel1D< A, B > >
	implements Affine1D< IndependentlyInterpolatedAffineModel1D< A, B > >, InvertibleBoundable
{
	private static final long serialVersionUID = -215895155784384320L;

	final protected AffineModel1D affine = new AffineModel1D();
	final protected double[] afs = new double[ 2 ];
	final protected double[] bfs = new double[ 2 ];

	public IndependentlyInterpolatedAffineModel1D( final A model, final B regularizer, final double... lambdas )
	{
		super( model, regularizer, lambdas );
		interpolate();
	}

	protected void interpolate()
	{
		a.toArray( afs );
		b.toArray( bfs );

		affine.set(
				afs[ 0 ] * ( 1.0 - lambdas[ 0 ] ) + bfs[ 0 ] * lambdas[ 0 ],
				afs[ 1 ] * ( 1.0 - lambdas[ 1 ] ) + bfs[ 1 ] * lambdas[ 1 ] );
	}

	@Override
	public < P extends PointMatch > void fit( final Collection< P > matches ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		super.fit( matches );
		interpolate();
	}

	@Override
	public void set( final IndependentlyInterpolatedAffineModel1D< A, B > m )
	{
		super.set( m );
		if ( InterpolatedAffineModel1D.class.isInstance( m ) )
			affine.set( m.affine );
	}

	@Override
	public IndependentlyInterpolatedAffineModel1D< A, B > copy()
	{
		final IndependentlyInterpolatedAffineModel1D< A, B > copy = new IndependentlyInterpolatedAffineModel1D< >( a.copy(), b.copy(), lambdas.clone() );
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
		affine.applyInPlace( location );
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
		affine.applyInverseInPlace( point );
	}

	@Override
	public IndependentlyInterpolatedAffineModel1D< A, B > createInverse()
	{
		final IndependentlyInterpolatedAffineModel1D< A, B > inverse = new IndependentlyInterpolatedAffineModel1D< >( a.createInverse(), b.createInverse(), lambdas.clone() );
		inverse.cost = cost;
		return inverse;
	}

	public AffineModel1D createAffineModel1D()
	{
		return affine.copy();
	}

	@Override
	public void preConcatenate( final IndependentlyInterpolatedAffineModel1D< A, B > affine1d )
	{
		affine.preConcatenate( affine1d.affine );
	}

	public void concatenate( final AffineModel1D affine1d )
	{
		affine.concatenate( affine1d );
	}

	public void preConcatenate( final AffineModel1D affine1d )
	{
		affine.preConcatenate( affine1d );
	}

	@Override
	public void concatenate( final IndependentlyInterpolatedAffineModel1D< A, B > affine1d )
	{
		affine.concatenate( affine1d.affine );
	}

	@Override
	public void toArray( final double[] data )
	{
		affine.toArray( data );
	}

	@Override
	public void toMatrix( final double[][] data )
	{
		affine.toMatrix( data );
	}

	/**
	 * Initialize the model such that the respective affine transform is:
	 *
	 * <pre>
	 * m0 m1
	 * 0   1
	 * </pre>
	 *
	 * @param m0
	 * @param m1
	 */
	final public void set( final float m0, final float m1 )
	{
		affine.set( m0, m1 );
	}

	@Override
	public void estimateBounds( final double[] min, final double[] max )
	{
		affine.estimateBounds( min, max );
	}

	@Override
	public void estimateInverseBounds( final double[] min, final double[] max ) throws NoninvertibleModelException
	{
		affine.estimateInverseBounds( min, max );
	}
}
