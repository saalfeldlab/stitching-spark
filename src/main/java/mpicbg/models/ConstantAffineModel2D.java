/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package mpicbg.models;

import java.awt.geom.AffineTransform;

final public class ConstantAffineModel2D< A extends Model< A > & Affine2D< A > & InvertibleBoundable >
	extends InvertibleConstantModel< A, ConstantAffineModel2D< A > >
	implements Affine2D< ConstantAffineModel2D< A > >, InvertibleBoundable
{
	private static final long serialVersionUID = -3540327692126579857L;

	public ConstantAffineModel2D( final A model )
	{
		super( model );
	}

	@Override
	public ConstantAffineModel2D< A > copy()
	{
		final ConstantAffineModel2D< A > copy = new ConstantAffineModel2D< >( model.copy() );
		copy.cost = cost;
		return copy;
	}

	@Override
	public ConstantAffineModel2D< A > createInverse()
	{
		final ConstantAffineModel2D< A > inverse = new ConstantAffineModel2D< >( model.createInverse() );
		inverse.cost = cost;
		return inverse;
	}

	@Override
	public void preConcatenate( final ConstantAffineModel2D< A > affine2d )
	{
		model.preConcatenate( affine2d.model );
	}

	@Override
	public void concatenate( final ConstantAffineModel2D< A > affine2d )
	{
		model.concatenate( affine2d.model );
	}

	@Override
	public void toArray( final double[] data )
	{
		model.toArray( data );
	}

	@Override
	public void toMatrix( final double[][] data )
	{
		model.toMatrix( data );
	}

	@Override
	public AffineTransform createAffine()
	{
		return model.createAffine();
	}

	@Override
	public AffineTransform createInverseAffine()
	{
		return model.createInverseAffine();
	}

	@Override
	public void estimateBounds( final double[] min, final double[] max )
	{
		model.estimateBounds( min, max );
	}

	@Override
	public void estimateInverseBounds( final double[] min, final double[] max ) throws NoninvertibleModelException
	{
		model.estimateInverseBounds( min, max );
	}
}
