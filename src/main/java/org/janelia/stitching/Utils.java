package org.janelia.stitching;

import java.awt.Rectangle;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;

import ij.gui.Roi;
import mpicbg.stitching.ImageCollectionElement;
import scala.Tuple2;

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
	
	public static ImageCollectionElement createElement( final StitchingJob job, final TileInfo tile ) throws Exception {
		
		File file;
		final String filePath = tile.getFile();
		if ( Paths.get( filePath ).isAbsolute() )
			file = new File( filePath );
		else
			file = new File( job.getBaseFolder(), filePath );
		
		final ImageCollectionElement e = new ImageCollectionElement( file, tile.getIndex() );
		e.setOffset( Conversions.toFloatArray( tile.getPosition() ) );
		e.setDimensionality( tile.getDimensionality() );
		e.setModel( TileModelFactory.createDefaultModel( e.getDimensionality() ) );
		return e;
	}
	
	
	public static ArrayList< Tuple2< TileInfo, TileInfo > > findOverlappingTiles( final TileInfo[] tiles ) {
		
		final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles = new ArrayList<>();
		for ( int i = 0; i < tiles.length; i++ )
			for ( int j = i + 1; j < tiles.length; j++ )
				if ( overlap( tiles[ i ], tiles[ j ] ) )
					overlappingTiles.add( new Tuple2< TileInfo, TileInfo >( tiles[ i ], tiles[ j ] ) );
		return overlappingTiles;
	}
	
	public static boolean overlap( final TileInfo t1, final TileInfo t2 ) {
		assert t1.getDimensionality() == t2.getDimensionality();
		
		for ( int d = 0; d < t1.getDimensionality(); d++ ) {
			
			final double p1 = t1.getPosition()[ d ], p2 = t2.getPosition()[ d ];
			final long s1 = t1.getSize()[ d ], s2 = t2.getSize()[ d ];
			
			if ( !( ( p2 >= p1 && p2 <= p1 + s1 ) || 
					( p1 >= p2 && p1 <= p2 + s2 ) ) )
				return false;
		}
		return true;
	}
	
	
	public static Boundaries findBoundaries( final TileInfo[] tiles ) {
		
		if ( tiles.length == 0 )
			return null;
		
		final int dim = tiles[ 0 ].getDimensionality();
		
		final Boundaries boundaries = new Boundaries( dim );
		for ( int d = 0; d < dim; d++ ) {
			boundaries.setMin( d, Integer.MAX_VALUE );
			boundaries.setMax( d, Integer.MIN_VALUE );
		}
			
		for ( final TileInfo tile : tiles ) {
			assert dim == tile.getDimensionality();
			
			for ( int d = 0; d < dim; d++ ) {
				boundaries.setMin( d, Math.min( boundaries.getMin(d), (int)Math.floor( tile.getPosition(d) ) ) );
				boundaries.setMax( d, Math.max( boundaries.getMax(d), (int)Math.ceil( tile.getPosition(d) ) + tile.getSize(d) ) );
			}
		}
		
		return boundaries;
	}
	
	public static ArrayList< TileInfo > divideSpace( final Boundaries space, final int subregionSize ) {
		assert space.validate() && subregionSize > 0;
		if ( !space.validate() || subregionSize <= 0 )
			return null;

		final ArrayList< TileInfo > subregions = new ArrayList<>();
		divideSpaceRecursive( space, subregions, subregionSize, new TileInfo( space.getDimensionality() ), 0 );
		
		for ( int i = 0; i < subregions.size(); i++ )
			subregions.get( i ).setIndex( i );
		return subregions;
	}
	private static void divideSpaceRecursive( final Boundaries space, final ArrayList< TileInfo > subregions, final int subregionSize, final TileInfo currSubregion, final int currDim ) {
		
		if ( currDim == space.getDimensionality() ) {
			subregions.add( currSubregion );
			return;
		}
		
		for ( long coord = space.getMin( currDim ); coord < space.getMax( currDim ); coord += subregionSize ) {
			
			final TileInfo newSubregion = currSubregion.clone();
			newSubregion.setPosition( currDim, coord );
			newSubregion.setSize( currDim, Math.min( subregionSize, space.getMax(currDim) - coord ) );
			
			divideSpaceRecursive( space, subregions, subregionSize, newSubregion, currDim + 1 );
		}
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
