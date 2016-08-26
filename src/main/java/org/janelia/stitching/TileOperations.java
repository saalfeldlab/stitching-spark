package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;

import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;

/**
 * @author pisarevi
 *
 */

public class TileOperations
{
	public static ArrayList< TilePair > findOverlappingTiles( final TileInfo[] tiles )
	{
		final ArrayList< TilePair > overlappingTiles = new ArrayList<>();
		for ( int i = 0; i < tiles.length; i++ )
			for ( int j = i + 1; j < tiles.length; j++ )
				if ( getOverlappingRegion( tiles[ i ], tiles[ j ] ) != null )
					overlappingTiles.add( new TilePair( tiles[ i ], tiles[ j ] ) );
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
			r.setMax( d, Math.min( s1 - 1, p2 + s2 - p1 - 1 ) );
		}
		assert r.validate();
		return r;
	}

	public static Boundaries getOverlappingRegionGlobal( final TileInfo t1, final TileInfo t2 )
	{
		final Boundaries r = getOverlappingRegion( t1, t2 );
		final Boundaries offset = t1.getBoundaries();
		if (r != null )
		{
			for ( int d = 0; d < r.numDimensions(); d++ )
			{
				r.setMin( d, r.min( d ) + offset.min( d ) );
				r.setMax( d, r.max( d ) + offset.min( d ) );
			}
		}
		return r;
	}

	public static ArrayList< TileInfo > findTilesWithinSubregion( final TileInfo[] tiles, final long[] min, final int[] dimensions )
	{
		assert min.length == dimensions.length;
		final TileInfo subregion = new TileInfo( min.length );
		for ( int d = 0; d < min.length; d++ ) {
			subregion.setPosition( d, min[ d ] );
			subregion.setSize( d, dimensions[ d ] );
		}
		return findTilesWithinSubregion( tiles, subregion );
	}

	public static ArrayList< TileInfo > findTilesWithinSubregion( final TileInfo[] tiles, final TileInfo subregion )
	{
		final ArrayList< TileInfo > tilesWithinSubregion = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( TileOperations.getOverlappingRegion( tile, subregion ) != null )
				tilesWithinSubregion.add( tile );
		return tilesWithinSubregion;
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
			final Boundaries tileBoundaries = tile.getBoundaries();
			for ( int d = 0; d < dim; d++ )
			{
				boundaries.setMin( d, Math.min( boundaries.min( d ), tileBoundaries.min( d ) ) );
				boundaries.setMax( d, Math.max( boundaries.max( d ), tileBoundaries.max( d ) ) );
			}
		}

		return boundaries;
	}

	public static void translateTilesToOrigin( final TileInfo[] tiles )
	{
		final Boundaries space = TileOperations.getCollectionBoundaries( tiles );
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setPosition( d, Math.round( tile.getPosition( d ) ) - space.min( d ) );
	}

	public static void translateTiles( final TileInfo[] tiles, final double[] offset )
	{
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setPosition( d, tile.getPosition( d ) + offset[ d ] );
	}

	public static ArrayList< TileInfo > divideSpaceBySize( final Boundaries space, final int subregionSize )
	{
		final long[] subregionDimsArr = new long[ space.numDimensions() ];
		Arrays.fill( subregionDimsArr, subregionSize );
		return divideSpace( space, new FinalDimensions( subregionDimsArr ) );
	}

	public static ArrayList< TileInfo > divideSpaceByCount( final Boundaries space, final int subregionsCountPerDim )
	{
		final long[] subregionDimsArr = new long[ space.numDimensions() ];
		for ( int d = 0; d < subregionDimsArr.length; d++ )
			subregionDimsArr[ d ] = ( long ) Math.ceil( ( double ) space.dimension( d ) / subregionsCountPerDim );
		return divideSpace( space, new FinalDimensions( subregionDimsArr ) );
	}

	public static ArrayList< TileInfo > divideSpace( final Boundaries space, final Dimensions subregionDims )
	{
		final ArrayList< TileInfo > subregions = new ArrayList<>();
		divideSpaceRecursive( space, subregionDims, subregions, new TileInfo( space.numDimensions() ), 0 );
		for ( int i = 0; i < subregions.size(); i++ )
			subregions.get( i ).setIndex( i );
		return subregions;
	}

	/*private static void divideSpaceRecursive( final Boundaries space, final ArrayList< TileInfo > subregions, final int subregionSize, final TileInfo currSubregion, final int currDim )
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
	}*/

	private static void divideSpaceRecursive( final Boundaries space, final Dimensions subregionDims, final ArrayList< TileInfo > subregions, final TileInfo currSubregion, final int currDim )
	{
		if ( currDim == space.numDimensions() )
		{
			subregions.add( currSubregion );
			return;
		}

		for ( long coord = space.min( currDim ); coord <= space.max( currDim ); coord += subregionDims.dimension( currDim ) )
		{

			final TileInfo newSubregion = currSubregion.clone();
			newSubregion.setPosition( currDim, coord );
			newSubregion.setSize( currDim, Math.min( subregionDims.dimension( currDim ), space.max( currDim ) - coord + 1 ) );

			divideSpaceRecursive( space, subregionDims, subregions, newSubregion, currDim + 1 );
		}
	}
}