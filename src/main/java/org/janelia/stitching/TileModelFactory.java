package org.janelia.stitching;

import mpicbg.models.Model;
import mpicbg.models.TranslationModel2D;
import mpicbg.models.TranslationModel3D;

/**
 * @author pisarevi
 *
 */

public class TileModelFactory {

	public static Model< ? > createDefaultModel( final int dim ) throws Exception {
		return createModel( dim, null );
	}
	
	public static Model< ? > createOffsetModel( final TileInfo tile ) throws Exception {
		return createModel( tile.getDimensionality(), tile );
	}
	
	public static Model< ? > createOffsetModel( final double[] offset ) throws Exception {
		final int dim = offset.length;
		final TileInfo tile = new TileInfo( dim );
		tile.setPosition( offset );
		return createModel( dim, tile );
	}
	
	private static Model< ? > createModel( final int dim, final TileInfo tile ) throws Exception {
		switch ( dim ) {
		case 2:
			TranslationModel2D m2d = new TranslationModel2D();
			if ( tile != null)
				m2d.set( tile.getPosition(0), tile.getPosition(1) );
			return m2d;
			
		case 3:
			TranslationModel3D m3d = new TranslationModel3D();
			if ( tile != null)
				m3d.set( tile.getPosition(0), tile.getPosition(1), tile.getPosition(2) );
			return m3d;
			
		default:	
			throw new Exception( "Not supported" );
		}
	}
}
