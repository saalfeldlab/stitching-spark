package org.janelia.util;

import java.util.Collection;

import mpicbg.models.Affine1D;
import mpicbg.models.AffineModel1D;
import mpicbg.models.IllDefinedDataPointsException;
import mpicbg.models.InvertibleBoundable;
import mpicbg.models.InvertibleInterpolatedModel;
import mpicbg.models.Model;
import mpicbg.models.NoninvertibleModelException;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.PointMatch;

final public class MismatchedInterpolatedAffineModel1D<
		A extends Model< A > & Affine1D< A >,
		B extends Model< B > & Affine1D< B > >
	extends InvertibleInterpolatedModel< A, B, MismatchedInterpolatedAffineModel1D< A, B > >
	implements Affine1D< MismatchedInterpolatedAffineModel1D< A, B > >, InvertibleBoundable
{
	private static final long serialVersionUID = 2662227348414849267L;

	final protected AffineModel1D affine = new AffineModel1D();
	final protected double[] afs = new double[ 2 ];
	final protected double[] bfs = new double[ 2 ];

	public MismatchedInterpolatedAffineModel1D( final A model, final B regularizer, final double lambda )
	{
		super( model, regularizer, lambda );
		interpolate();
	}

	protected void interpolate()
	{
		a.toArray( afs );
		b.toArray( bfs );

		affine.set(
				afs[ 0 ] * lambda + bfs[ 0 ] * l1,
				afs[ 1 ] * l1     + bfs[ 1 ] * lambda );
	}

	@Override
	public < P extends PointMatch > void fit( final Collection< P > matches ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		super.fit( matches );
		interpolate();
	}

	@Override
	public void set( final MismatchedInterpolatedAffineModel1D< A, B > m )
	{
		super.set( m );
		if ( MismatchedInterpolatedAffineModel1D.class.isInstance( m ) )
			affine.set( ( ( MismatchedInterpolatedAffineModel1D< A, B > ) m ).affine );
	}

	@Override
	public MismatchedInterpolatedAffineModel1D< A, B > copy()
	{
		final MismatchedInterpolatedAffineModel1D< A, B > copy = new MismatchedInterpolatedAffineModel1D< A, B >( a.copy(), b.copy(), lambda );
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
	public MismatchedInterpolatedAffineModel1D< A, B > createInverse()
	{
		final MismatchedInterpolatedAffineModel1D< A, B > inverse = new MismatchedInterpolatedAffineModel1D< A, B >( a.createInverse(), b.createInverse(), lambda );
		inverse.cost = cost;
		return inverse;
	}

	public AffineModel1D createAffineModel1D()
	{
		return affine.copy();
	}

	@Override
	public void preConcatenate( final MismatchedInterpolatedAffineModel1D< A, B > affine1d )
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
	public void concatenate( final MismatchedInterpolatedAffineModel1D< A, B > affine1d )
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
