package org.janelia.stitching;

import mpicbg.models.Affine2D;
import mpicbg.models.Affine3D;
import mpicbg.models.Model;
import net.imglib2.concatenate.Concatenable;
import net.imglib2.concatenate.PreConcatenable;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.AffineTransform;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.AffineTransform3D;

public class TransformUtils
{
	public static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > A createTransform( final double[][] affine )
	{
		final A transform = createTransform( affine.length );
		transform.set( affine );
		return transform;
	}

	@SuppressWarnings( "unchecked" )
	public static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > A createTransform( final int dim )
	{
		switch ( dim )
		{
		case 2:
			return ( A ) new AffineTransform2D();
		case 3:
			return ( A ) new AffineTransform3D();
		default:
			return ( A ) new AffineTransform( dim );
		}
	}

	/**
	 * Returns a new transformation as a representation of a given affine model.
	 */
	public static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > A getModelTransform( final Model< ? > model )
	{
		final double[][] matrix;
		if ( model instanceof Affine2D )
		{
			final Affine2D< ? > affineModel = ( Affine2D< ? > ) model;
			matrix = new double[ 2 ][ 3 ];
			affineModel.toMatrix( matrix );
		}
		else if ( model instanceof Affine3D )
		{
			final Affine3D< ? > affineModel = ( Affine3D< ? > ) model;
			matrix = new double[ 3 ][ 4 ];
			affineModel.toMatrix( matrix );
		}
		else
		{
			throw new IllegalArgumentException( "only 2d/3d are supported" );
		}
		return createTransform( matrix );
	}

	/**
	 * Returns an affine matrix for a given transformation.
	 */
	public static double[][] getMatrix( final AffineGet transform )
	{
		final double[][] matrix;
		if ( transform.numDimensions() == 2 )
		{
			matrix = new double[ 2 ][ 3 ];
			matrix[ 0 ][ 0 ] = transform.get( 0, 0 );
			matrix[ 0 ][ 1 ] = transform.get( 0, 1 );
			matrix[ 0 ][ 2 ] = transform.get( 0, 2 );
			matrix[ 1 ][ 0 ] = transform.get( 1, 0 );
			matrix[ 1 ][ 1 ] = transform.get( 1, 1 );
			matrix[ 1 ][ 2 ] = transform.get( 1, 2 );
		}
		else if ( transform.numDimensions() == 3 )
		{
			matrix = new double[ 3 ][ 4 ];
			matrix[ 0 ][ 0 ] = transform.get( 0, 0 );
			matrix[ 0 ][ 1 ] = transform.get( 0, 1 );
			matrix[ 0 ][ 2 ] = transform.get( 0, 2 );
			matrix[ 0 ][ 3 ] = transform.get( 0, 3 );
			matrix[ 1 ][ 0 ] = transform.get( 1, 0 );
			matrix[ 1 ][ 1 ] = transform.get( 1, 1 );
			matrix[ 1 ][ 2 ] = transform.get( 1, 2 );
			matrix[ 1 ][ 3 ] = transform.get( 1, 3 );
			matrix[ 2 ][ 0 ] = transform.get( 2, 0 );
			matrix[ 2 ][ 1 ] = transform.get( 2, 1 );
			matrix[ 2 ][ 2 ] = transform.get( 2, 2 );
			matrix[ 2 ][ 3 ] = transform.get( 2, 3 );
		}
		else
		{
			throw new IllegalArgumentException( "only 2d/3d are supported" );
		}
		return matrix;
	}
}
