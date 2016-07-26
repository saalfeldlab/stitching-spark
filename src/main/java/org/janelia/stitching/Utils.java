package org.janelia.stitching;

import java.io.File;
import java.nio.file.Paths;

import mpicbg.stitching.ImageCollectionElement;

/**
 * @author pisarevi
 *
 */

public class Utils {
	
	public static String addFilenameSuffix( final String filename, final String suffix )
	{
		int lastDotIndex = filename.lastIndexOf( "." );
		if ( lastDotIndex == -1 )
			lastDotIndex = filename.length();
		
		final StringBuilder ret = new StringBuilder( filename );
		ret.insert( lastDotIndex, suffix );
		return ret.toString();
	}
	
	public static String removeFilenameSuffix( final String filename, final String suffix ) 
	{
		final int suffixIndex = filename.lastIndexOf( suffix );
		if ( suffixIndex == -1 )
			return filename;
		
		int lastDotIndex = filename.lastIndexOf( "." );
		if ( lastDotIndex == -1 )
			lastDotIndex = filename.length();
		
		if ( suffixIndex + suffix.length() != lastDotIndex )
			return filename; // not a suffix
		
		final StringBuilder ret = new StringBuilder( filename );
		ret.delete( suffixIndex, lastDotIndex );
		return ret.toString();
	}
	
	public static String getAbsoluteImagePath( final StitchingJob job, final TileInfo tile ) 
	{
		String filePath = tile.getFile();
		if ( !Paths.get( filePath ).isAbsolute() )
			filePath = Paths.get( job.getBaseFolder(), filePath ).toString();
		return filePath; 
	}
	
	public static ImageCollectionElement createElement( final StitchingJob job, final TileInfo tile ) throws Exception 
	{
		final File file = new File( getAbsoluteImagePath( job, tile) );
		final ImageCollectionElement e = new ImageCollectionElement( file, tile.getIndex() );
		e.setOffset( Conversions.toFloatArray( tile.getPosition() ) );
		e.setDimensionality( tile.getDimensionality() );
		e.setModel( TileModelFactory.createOffsetModel( tile ) );
		return e;
	}
}
