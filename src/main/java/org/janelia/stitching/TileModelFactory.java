package org.janelia.stitching;

import mpicbg.models.AffineModel2D;
import mpicbg.models.AffineModel3D;
import mpicbg.models.InterpolatedAffineModel2D;
import mpicbg.models.InterpolatedAffineModel3D;
import mpicbg.models.Model;
import mpicbg.models.TranslationModel2D;
import mpicbg.models.TranslationModel3D;

/**
 * Convenience class for creating default {@link Model} which is required by stitching procedure.
 * Possible options are:
 * - translation model (2d/3d)
 * - affine model regularized by translation model (2d/3d)
 *
 * @author Igor Pisarev
 */

public class TileModelFactory {

	private static final double REGULARIZER_TRANSLATION = 0.1;

	@SuppressWarnings( "unchecked" )
	public static < M extends Model< M > > M createAffineModel( final int numDimensions ) throws RuntimeException
	{
		switch ( numDimensions )
		{
		case 2:
			return ( M ) new InterpolatedAffineModel2D<>(
					new AffineModel2D(),
					new TranslationModel2D(),
					REGULARIZER_TRANSLATION
				);
		case 3:
			return ( M ) new InterpolatedAffineModel3D<>(
					new AffineModel3D(),
					new TranslationModel3D(),
					REGULARIZER_TRANSLATION
				);
		default:
			throw new RuntimeException( "only 2d and 3d are supported" );
		}
	}

	@SuppressWarnings( "unchecked" )
	public static < M extends Model< M > > M createTranslationModel( final int numDimensions ) throws RuntimeException
	{
		switch ( numDimensions )
		{
		case 2:
			return ( M ) new TranslationModel2D();
		case 3:
			return ( M ) new TranslationModel3D();
		default:
			throw new RuntimeException( "only 2d and 3d are supported" );
		}
	}
}
