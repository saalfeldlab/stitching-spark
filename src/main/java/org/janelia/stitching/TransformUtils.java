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
	 * Returns the linear component of the given transform.
	 */
	public static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > A getLinearComponent( final AffineGet transform )
	{
		final double[][] linearComponentAffineMatrix = new double[ transform.numDimensions() ][ transform.numDimensions() + 1 ];
		for ( int dRow = 0; dRow < transform.numDimensions(); ++dRow )
			for ( int dCol = 0; dCol < transform.numDimensions(); ++dCol )
				linearComponentAffineMatrix[ dRow ][ dCol ] = transform.get( dRow, dCol );
		return createTransform( linearComponentAffineMatrix );
	}

	/**
	 * Returns the translational component of the given transform.
	 */
	public static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > A getTranslationalComponent( final AffineGet transform )
	{
		final double[][] translationalComponentAffineMatrix = new double[ transform.numDimensions() ][ transform.numDimensions() + 1 ];
		for ( int d = 0; d < transform.numDimensions(); ++d )
		{
			translationalComponentAffineMatrix[ d ][ transform.numDimensions() ] = transform.get( d, transform.numDimensions() );
			translationalComponentAffineMatrix[ d ][ d ] = 1;
		}
		return createTransform( translationalComponentAffineMatrix );
	}

	/**
	 * Returns a new transformation where the linear component of the given transformation has been undone.
	 */
	public static < A extends AffineGet & AffineSet & Concatenable< AffineGet > & PreConcatenable< AffineGet > > A undoLinearComponent( final AffineGet transform )
	{
		final A undoLinearComponentTransform = createTransform( transform.numDimensions() );
		undoLinearComponentTransform.preConcatenate( transform );
		undoLinearComponentTransform.preConcatenate( getLinearComponent( transform ).inverse() );
		return undoLinearComponentTransform;
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
}
