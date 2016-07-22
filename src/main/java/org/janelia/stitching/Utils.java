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
	
	
	
	// taken from CollectionStitchingImgLib (it is made protected there)
	// TODO: pull request making it public, then remove it from here
	public static Roi getROI( final ImageCollectionElement e1, final ImageCollectionElement e2 )
	{
		final int start[] = new int[ 2 ], end[] = new int[ 2 ];
		for ( int d = 0; d < 2; d++ )
		{		
			final float p1 = e1.getOffset( d ), p2 = e2.getOffset( d );
			final int s1 = e1.getDimension( d ), s2 = e2.getDimension( d );
			
			// begin of 2 lies inside 1
			if ( p2 >= p1 && p2 <= p1 + s1 )
			{
				start[ d ] = Math.round( p2 - p1 );
				
				// end of 2 lies inside 1
				if ( p2 + s2 <= p1 + s1 )
					end[ d ] = Math.round( p2 + s2 - p1 );
				else
					end[ d ] = Math.round( s1 );
			}
			else if ( p2 + s2 <= p1 + s1 ) // end of 2 lies inside 1
			{
				start[ d ] = 0;
				end[ d ] = Math.round( p2 + s2 - p1 );
			}
			else // if both outside then the whole image 
			{
				start[ d ] = -1;
				end[ d ] = -1;
			}
		}
		
		return new Roi( new Rectangle( start[ 0 ], start[ 1 ], end[ 0 ] - start[ 0 ], end[ 1 ] - start[ 1 ] ) );
	}
}
