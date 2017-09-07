package org.janelia.stitching;

import mpicbg.models.InterpolatedAffineModel2D;
import mpicbg.models.InterpolatedAffineModel3D;
import mpicbg.models.Model;
import mpicbg.models.SimilarityModel2D;
import mpicbg.models.SimilarityModel3D;
import mpicbg.models.TranslationModel2D;
import mpicbg.models.TranslationModel3D;

/**
 * Convenience class for creating default {@link Model} which is required by stitching procedure.
 * This class uses {@link TranslationModel2D} or {@link TranslationModel3D}.
 *
 * @author Igor Pisarev
 */

public class TileModelFactory {

	private static final double REGULARIZER_TRANSLATION = 0.1;

	/**
	 * @return default translational model initialized to origin
	 */
	public static < M extends Model< M > > M createDefaultModel( final int dim ) throws Exception {
		return createModel( dim, null );
	}

	/**
	 * @return offset translational model set to position of the tile
	 */
	public static < M extends Model< M > > M createOffsetModel( final TileInfo tile ) throws Exception {
		return createModel( tile.numDimensions(), tile );
	}

	/**
	 * @return offset translational model
	 */
	public static < M extends Model< M > > M createOffsetModel( final double[] offset ) throws Exception {
		final int dim = offset.length;
		final TileInfo tile = new TileInfo( dim );
		tile.setPosition( offset );
		return createModel( dim, tile );
	}

	public static < M extends Model< M > > M createSimilarityModel( final TileInfo tile ) throws Exception
	{
		switch ( tile.numDimensions() ) {
		case 2:
//			final SimilarityModel2D m2d = new SimilarityModel2D();
//			return (M)m2d;
			return ( M ) new InterpolatedAffineModel2D<>(
					new SimilarityModel2D(),
					new TranslationModel2D(),
					REGULARIZER_TRANSLATION
				);

		case 3:
//			final SimilarityModel3D m3d = new SimilarityModel3D();
//			return (M)m3d;
			return ( M ) new InterpolatedAffineModel3D<>(
					new SimilarityModel3D(),
					new TranslationModel3D(),
					REGULARIZER_TRANSLATION
				);

		default:
			throw new Exception( "Not supported" );
		}
	}

	private static < M extends Model< M > > M createModel( final int dim, final TileInfo tile ) throws Exception {
		switch ( dim ) {
		case 2:
			final TranslationModel2D m2d = new TranslationModel2D();
			if ( tile != null)
				m2d.set( tile.getPosition(0), tile.getPosition(1) );
			return (M)m2d;

		case 3:
			final TranslationModel3D m3d = new TranslationModel3D();
			if ( tile != null)
				m3d.set( tile.getPosition(0), tile.getPosition(1), tile.getPosition(2) );
			return (M)m3d;

		default:
			throw new Exception( "Not supported" );
		}
	}
}
