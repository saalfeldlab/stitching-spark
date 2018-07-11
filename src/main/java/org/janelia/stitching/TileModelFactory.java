package org.janelia.stitching;

import mpicbg.models.Affine2D;
import mpicbg.models.Affine3D;
import mpicbg.models.AffineModel2D;
import mpicbg.models.AffineModel3D;
import mpicbg.models.InterpolatedAffineModel2D;
import mpicbg.models.InterpolatedAffineModel3D;
import mpicbg.models.Model;
import mpicbg.models.RigidModel2D;
import mpicbg.models.RigidModel3D;
import mpicbg.models.TranslationModel2D;
import mpicbg.models.TranslationModel3D;

/**
 * Convenience class for creating tile {@link Model}s used by the global optimizer.
 *
 * @author Igor Pisarev
 */

public class TileModelFactory
{
	@SuppressWarnings( "unchecked" )
	public static < M extends Model< M >, M2D extends Model< M2D > & Affine2D< M2D >, M3D extends Model< M3D > & Affine3D< M3D > > M createInterpolatedModel(
			final int numDimensions,
			final M model,
			final M regularizer,
			final double lambda ) throws RuntimeException
	{
		switch ( numDimensions )
		{
		case 2:
			return ( M ) new InterpolatedAffineModel2D<>(
					( M2D ) model,
					( M2D ) regularizer,
					lambda
				);
		case 3:
			return ( M ) new InterpolatedAffineModel3D<>(
					( M3D ) model,
					( M3D ) regularizer,
					lambda
				);
		default:
			throw new IllegalArgumentException( "only 2d and 3d are supported" );
		}
	}

	@SuppressWarnings( "unchecked" )
	public static < M extends Model< M > > M createAffineModel( final int numDimensions )
	{
		switch ( numDimensions )
		{
		case 2:
			return ( M ) new AffineModel2D();
		case 3:
			return ( M ) new AffineModel3D();
		default:
			throw new IllegalArgumentException( "only 2d and 3d are supported" );
		}
	}

	@SuppressWarnings( "unchecked" )
	public static < M extends Model< M > > M createTranslationModel( final int numDimensions )
	{
		switch ( numDimensions )
		{
		case 2:
			return ( M ) new TranslationModel2D();
		case 3:
			return ( M ) new TranslationModel3D();
		default:
			throw new IllegalArgumentException( "only 2d and 3d are supported" );
		}
	}

	@SuppressWarnings( "unchecked" )
	public static < M extends Model< M > > M createRigidModel( final int numDimensions )
	{
		switch ( numDimensions )
		{
		case 2:
			return ( M ) new RigidModel2D();
		case 3:
			return ( M ) new RigidModel3D();
		default:
			throw new IllegalArgumentException( "only 2d and 3d are supported" );
		}
	}
}
