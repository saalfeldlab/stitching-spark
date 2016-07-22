package org.janelia.stitching;

import java.awt.Rectangle;
import java.io.File;
import java.nio.file.Paths;

import ij.gui.Roi;
import mpicbg.stitching.ImageCollectionElement;

/**
 * @author pisarevi
 *
 */

public class Utils {
	
	public static String addFilenameSuffix( final String filename, final String suffix ) {
		final StringBuilder ret = new StringBuilder( filename );
		int lastDotIndex = ret.lastIndexOf( "." );
		if ( lastDotIndex == -1 )
			lastDotIndex = ret.length();
		ret.insert( lastDotIndex, suffix );
		return ret.toString();
	}
	
	public static String getAbsoluteImagePath( final StitchingJob job, final TileInfo tile ) {
		String filePath = tile.getFile();
		if ( !Paths.get( filePath ).isAbsolute() )
			filePath = Paths.get( job.getBaseFolder(), filePath ).toString();
		return filePath; 
	}
	
	public static ImageCollectionElement createElement( final StitchingJob job, final TileInfo tile ) throws Exception {
		final File file = new File( getAbsoluteImagePath( job, tile) );
		final ImageCollectionElement e = new ImageCollectionElement( file, tile.getIndex() );
		e.setOffset( Conversions.toFloatArray( tile.getPosition() ) );
		e.setDimensionality( tile.getDimensionality() );
		e.setModel( TileModelFactory.createDefaultModel( e.getDimensionality() ) );
		return e;
	}
}
