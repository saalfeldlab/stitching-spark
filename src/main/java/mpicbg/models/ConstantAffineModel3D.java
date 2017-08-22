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

final public class ConstantAffineModel3D< A extends Model< A > & Affine3D< A > & InvertibleBoundable >
	extends InvertibleConstantModel< A, ConstantAffineModel3D< A > >
	implements Affine3D< ConstantAffineModel3D< A > >, InvertibleBoundable
{
	private static final long serialVersionUID = -3540327692126579857L;

	public ConstantAffineModel3D( final A model )
	{
		super( model );
	}

	@Override
	public ConstantAffineModel3D< A > copy()
	{
		final ConstantAffineModel3D< A > copy = new ConstantAffineModel3D< >( model.copy() );
		copy.cost = cost;
		return copy;
	}

	@Override
	public ConstantAffineModel3D< A > createInverse()
	{
		final ConstantAffineModel3D< A > inverse = new ConstantAffineModel3D< >( model.createInverse() );
		inverse.cost = cost;
		return inverse;
	}

	@Override
	public void preConcatenate( final ConstantAffineModel3D< A > affine3d )
	{
		model.preConcatenate( affine3d.model );
	}

	@Override
	public void concatenate( final ConstantAffineModel3D< A > affine3d )
	{
		model.concatenate( affine3d.model );
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
