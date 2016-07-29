package org.janelia.stitching;

import java.util.ArrayList;

import scala.Tuple2;

/**
 * @author pisarevi
 *
 */

public class TileHelper
{
	public static ArrayList< Tuple2< TileInfo, TileInfo > > findOverlappingTiles( final TileInfo[] tiles )
	{
		final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles = new ArrayList<>();
		for ( int i = 0; i < tiles.length; i++ )
			for ( int j = i + 1; j < tiles.length; j++ )
				if ( getOverlappingRegion( tiles[ i ], tiles[ j ] ) != null )
					overlappingTiles.add( new Tuple2< TileInfo, TileInfo >( tiles[ i ], tiles[ j ] ) );
		return overlappingTiles;
	}

	public static Boundaries getOverlappingRegion( final TileInfo t1, final TileInfo t2 )
	{
		final Boundaries r = new Boundaries( t1.numDimensions() );
		for ( int d = 0; d < r.numDimensions(); d++ )
		{
			final long p1 = Math.round( t1.getPosition( d ) ), p2 = Math.round( t2.getPosition( d ) );
			final long s1 = t1.getSize( d ), s2 = t2.getSize( d );

			if ( !( ( p2 >= p1 && p2 < p1 + s1 ) || ( p1 >= p2 && p1 < p2 + s2 ) ) )
				return null;

			r.setMin( d, Math.max( 0, p2 - p1 ) );
			r.setMax( d, Math.min( p1 + s1 - 1, p2 + s2 - 1 ) );
		}
		assert r.validate();
		return r;
	}

	public static Boundaries getCollectionBoundaries( final TileInfo[] tiles )
	{
		if ( tiles.length == 0 )
			return null;

		final int dim = tiles[ 0 ].numDimensions();

		final Boundaries boundaries = new Boundaries( dim );
		for ( int d = 0; d < dim; d++ )
		{
			boundaries.setMin( d, Integer.MAX_VALUE );
			boundaries.setMax( d, Integer.MIN_VALUE );
		}

		for ( final TileInfo tile : tiles )
		{
			assert dim == tile.numDimensions();

			for ( int d = 0; d < dim; d++ )
			{
				boundaries.setMin( d, Math.min( boundaries.min( d ), ( int ) Math.round( tile.getPosition( d ) ) ) );
				boundaries.setMax( d, Math.max( boundaries.max( d ), ( int ) Math.round( tile.getPosition( d ) ) + tile.getSize( d ) - 1 ) );
			}
		}

		return boundaries;
	}

	public static ArrayList< TileInfo > divideSpace( final Boundaries space, final int subregionSize )
	{
		assert space.validate() && subregionSize > 0;
		if ( !space.validate() || subregionSize <= 0 )
			return null;

		final ArrayList< TileInfo > subregions = new ArrayList<>();
		divideSpaceRecursive( space, subregions, subregionSize, new TileInfo( space.numDimensions() ), 0 );

		for ( int i = 0; i < subregions.size(); i++ )
			subregions.get( i ).setIndex( i );
		return subregions;
	}

	private static void divideSpaceRecursive( final Boundaries space, final ArrayList< TileInfo > subregions, final int subregionSize, final TileInfo currSubregion, final int currDim )
	{
		if ( currDim == space.numDimensions() )
		{
			subregions.add( currSubregion );
			return;
		}

		for ( long coord = space.min( currDim ); coord <= space.max( currDim ); coord += subregionSize )
		{

			final TileInfo newSubregion = currSubregion.clone();
			newSubregion.setPosition( currDim, coord );
			newSubregion.setSize( currDim, Math.min( subregionSize, space.max( currDim ) - coord + 1 ) );

			divideSpaceRecursive( space, subregions, subregionSize, newSubregion, currDim + 1 );
		}
	}
}