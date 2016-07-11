package org.janelia.stitching;

import java.awt.Rectangle;
import java.io.File;
import java.util.ArrayList;

import ij.gui.Roi;
import mpicbg.models.TranslationModel2D;
import mpicbg.models.TranslationModel3D;
import mpicbg.stitching.ImageCollectionElement;
import scala.Tuple2;

/**
 * @author pisarevi
 *
 */

public class IJUtil {

	public static ImageCollectionElement createElement( final StitchingJob job, final TileInfo tile ) throws Exception {
		
		final ImageCollectionElement e = new ImageCollectionElement( new File( job.getBaseImagesFolder(), tile.getFile() ), tile.getIndex() );
		e.setOffset( tile.getPosition() );
		e.setDimensionality( tile.getDimensionality() );
		switch ( e.getDimensionality() ) {
		case 3:
			e.setModel( new TranslationModel3D() );
			break;
			
		case 2:
			e.setModel( new TranslationModel2D() );
			break;

		default:
			throw new Exception( "Not supported" );
		}
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
