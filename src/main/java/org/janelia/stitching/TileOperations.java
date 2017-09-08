package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.LongConsumer;

import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RealInterval;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;

/**
 * Contains a number of useful operations on a set of tiles.
 *
 * @author Igor Pisarev
 */

public class TileOperations
{
	/**
	 * Pads the {@code interval} defined within the outer space of size {@code outerDimensions} with {@code padding}.
	 * Tries to equally pad the {@code interval} from both sides (which means to expand it by {@code padding}/2 on each side).
	 * If the {@code padding} is odd, it adds the remaining 1 towards the min side.
	 * The padded interval cannot exceed the outer space boundaries (e.g., 0 and {@code outerDimensions}-1), so in this case the remainder is applied towards the other side.
	 * If the resulting interval has reached the outer space size (0, {@code outerDimensions}-1), the rest of the remaining padding is ignored.
	 */
	public static Interval padInterval( final Interval interval, final Dimensions outerDimensions, final long[] padding )
	{
		final long[] paddedMin = new long[ interval.numDimensions() ], paddedMax = new long[ interval.numDimensions() ];
		for ( int d = 0; d < interval.numDimensions(); d++ )
		{
			paddedMin[ d ] = Math.max( interval.min( d ) - padding[ d ] / 2, 0 );
			paddedMax[ d ] = Math.min( interval.max( d ) + padding[ d ] / 2, outerDimensions.dimension( d ) - 1 );

			final long remainder = padding[ d ] - ( interval.min( d ) - paddedMin[ d ] ) - ( paddedMax[ d ] - interval.max( d ) );
			if ( remainder > 0 )
			{
				if ( paddedMin[ d ] == 0 )
					paddedMax[ d ] = Math.min( paddedMax[ d ] + remainder, outerDimensions.dimension( d ) - 1 );
				else
					paddedMin[ d ] = Math.max( paddedMin[ d ] - remainder, 0 );
			}
		}
		return new FinalInterval( paddedMin, paddedMax );
	}

	// TODO: remove as it duplicates Intervals.smallestContainingInterval() functionality
	public static Interval floorCeilRealInterval( final RealInterval realInterval )
	{
		final long[] min = new long[ realInterval.numDimensions() ], max = new long[ realInterval.numDimensions() ];
		for ( int d = 0; d < realInterval.numDimensions(); ++d )
		{
			min[ d ] = ( long ) Math.floor( realInterval.realMin( d ) );
			max[ d ] = ( long ) Math.ceil( realInterval.realMax( d ) );
		}
		return new FinalInterval( min, max );
	}

	public static Interval roundRealInterval( final RealInterval realInterval )
	{
		final long[] min = new long[ realInterval.numDimensions() ], max = new long[ realInterval.numDimensions() ];
		for ( int d = 0; d < realInterval.numDimensions(); ++d )
		{
			min[ d ] = Math.round( realInterval.realMin( d ) );
			max[ d ] = Math.round( realInterval.realMax( d ) );
		}
		return new FinalInterval( min, max );
	}

	/**
	 * @return a list of overlapping pairs
	 */
	public static ArrayList< TilePair > findOverlappingTiles( final TileInfo[] tiles )
	{
		final ArrayList< TilePair > overlappingTiles = new ArrayList<>();
		for ( int i = 0; i < tiles.length; i++ )
			for ( int j = i + 1; j < tiles.length; j++ )
				if ( overlap( tiles[ i ], tiles[ j ] ) )
					overlappingTiles.add( new TilePair( tiles[ i ], tiles[ j ] ) );
		return overlappingTiles;
	}

	/**
	 * @return an overlap with relative coordinates of the first tile
	 */
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

	/**
	 * @return an overlap as a RealInterval with relative coordinates of the first tile
	 */
	public static RealInterval getOverlappingRegionReal( final TileInfo t1, final TileInfo t2 )
	{
		final double[] min = new double[ t1.numDimensions() ], max = new double[ t1.numDimensions() ];
		for ( int d = 0; d < t1.numDimensions(); d++ )
		{
			final double p1 = t1.getPosition( d ), p2 = t2.getPosition( d );
			final long s1 = t1.getSize( d ), s2 = t2.getSize( d );

			if ( !( ( p2 >= p1 && p2 < p1 + s1 ) || ( p1 >= p2 && p1 < p2 + s2 ) ) )
				return null;

			min[ d ] = Math.max( 0, p2 - p1 );
			max[ d ] = Math.min( s1 - 1, p2 + s2 - p1 - 1 );
		}
		return new FinalRealInterval( min, max );
	}

	/**
	 * @return true if two tiles overlap, false otherwise
	 */
	public static boolean overlap( final TileInfo t1, final TileInfo t2 )
	{
		for ( int d = 0; d < t1.numDimensions(); d++ )
		{
			final double min1 = t1.getPosition( d ), min2 = t2.getPosition( d ), max1 = t1.getMax( d ), max2 = t2.getMax( d );
			if ( !( ( min2 >= min1 && min2 <= max1 ) || ( min1 >= min2 && min1 <= max2 ) ) )
				return false;
		}
		return true;
	}

	public static boolean overlap( final RealInterval t1, final RealInterval t2 )
	{
		for ( int d = 0; d < t1.numDimensions(); d++ )
		{
			final double min1 = t1.realMin( d ), min2 = t2.realMin( d ), max1 = t1.realMax( d ), max2 = t2.realMax( d );
			if ( !( ( min2 >= min1 && min2 <= max1 ) || ( min1 >= min2 && min1 <= max2 ) ) )
				return false;
		}
		return true;
	}

	/**
	 * @return an overlap with global coordinates
	 */
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

	/**
	 * @return a list of tiles lying within specified subregion (overlapping with it)
	 */
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

	/**
	 * @return a list of tiles lying within specified subregion (overlapping with it)
	 */
	public static ArrayList< TileInfo > findTilesWithinSubregion( final TileInfo[] tiles, final TileInfo subregion )
	{
		final ArrayList< TileInfo > tilesWithinSubregion = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( TileOperations.overlap( tile, subregion ) )
				tilesWithinSubregion.add( tile );
		return tilesWithinSubregion;
	}

	public static Set< Integer > findTilesWithinSubregion( final Map< Integer, ? extends Interval > tiles, final Interval subregion )
	{
		final Set< Integer > tilesWithinSubregion = new HashSet<>();
		for ( final Entry< Integer, ? extends Interval > entry : tiles.entrySet() )
			if ( TileOperations.overlap( entry.getValue(), subregion ) )
				tilesWithinSubregion.add( entry.getKey() );
		return tilesWithinSubregion;
	}

	public static ArrayList< TileInfo > findTilesWithinSubregion( final TileInfo[] tiles, final RealInterval subregion )
	{
		final ArrayList< TileInfo > tilesWithinSubregion = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( TileOperations.overlap( tile, subregion ) )
				tilesWithinSubregion.add( tile );
		return tilesWithinSubregion;
	}

	/**
	 * @return an integer bounding box of a collection of tiles
	 */
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
	/**
	 * @return an integer bounding box of a collection of tiles
	 */
	public static Interval getCollectionBoundaries( final Collection< ? extends Interval > boxes )
	{
		if ( boxes.isEmpty() )
			return null;

		final int dim = boxes.iterator().next().numDimensions();

		final long[] min = new long[ dim ], max = new long[ dim ];
		Arrays.fill( min, Long.MAX_VALUE );
		Arrays.fill( max, Long.MIN_VALUE );

		for ( final Interval box : boxes )
		{
			for ( int d = 0; d < dim; d++ )
			{
				min[ d ] = Math.min( box.min( d ), min[ d ] );
				max[ d ] = Math.max( box.max( d ), max[ d ] );
			}
		}

		return new FinalInterval( min, max );
	}
	/**
	 * @return a real bounding box of a collection of tiles
	 */
	public static RealInterval getRealCollectionBoundaries( final TileInfo[] tiles )
	{
		if ( tiles.length == 0 )
			return null;

		final int dim = tiles[ 0 ].numDimensions();

		final double[] min = new double[ dim ], max = new double[ dim ];
		Arrays.fill( min, Double.MAX_VALUE );
		Arrays.fill( max, -Double.MAX_VALUE );

		for ( final TileInfo tile : tiles )
		{
			for ( int d = 0; d < dim; d++ )
			{
				min[ d ] = Math.min( min[ d ], tile.getPosition( d ) );
				max[ d ] = Math.max( max[ d ], tile.getPosition( d ) + tile.getSize( d ) - 1 );
			}
		}

		return new FinalRealInterval( min, max );
	}

	/**
	 * Translates a set of tiles in a way such that the lowest coordinate of every dimension is at the origin.
	 */
	public static void translateTilesToOrigin( final TileInfo[] tiles )
	{
		final Boundaries space = TileOperations.getCollectionBoundaries( tiles );
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setPosition( d, Math.round( tile.getPosition( d ) ) - space.min( d ) );
	}
	public static void translateTilesToOriginReal( final TileInfo[] tiles )
	{
		final RealInterval space = TileOperations.getRealCollectionBoundaries( tiles );
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setPosition( d, tile.getPosition( d ) - space.realMin( d ) );
	}

	/**
	 * Applies translation to a set of tiles with a specified {@code offset}.
	 */
	public static void translateTiles( final TileInfo[] tiles, final double[] offset )
	{
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setPosition( d, tile.getPosition( d ) + offset[ d ] );
	}

	/**
	 * Estimates the bounding box of a transformed (or shifted) tile.
	 *
	 * FIXME: to be consistent with an affine transformation or a real shift vector, {@link TileInfo#getBoundaries()} should be changed from rounding to extending
	 * to the smallest containing interval
	 */
	public static Interval estimateBoundingBox( final TileInfo tile )
	{
		if ( tile.getTransform() == null )
			return tile.getBoundaries();

		final double[] tileMax = new double[ tile.numDimensions() ];
		for ( int d = 0; d < tileMax.length; ++d )
			tileMax[ d ] = tile.getSize( d ) - 1;
		final RealInterval tileRealBoundsAtZero = new FinalRealInterval( new double[ tile.numDimensions() ], tileMax );
		final RealInterval estimatedRealBounds = tile.getTransform().estimateBounds( tileRealBoundsAtZero );
		return Intervals.smallestContainingInterval( estimatedRealBounds );
	}

	/**
	 * Applies a user-defined {@code function} for each pixel of an {@code interval} defined within a larger {@code space}.
	 */
	public static void forEachPixel( final Interval interval, final Dimensions space, final LongConsumer function )
	{
		assert interval.numDimensions() == space.numDimensions();
		for ( int d = 0; d < space.numDimensions(); d++ )
			assert interval.max( d ) < space.dimension( d );

		final int n = interval.numDimensions();
		final long[] min = Intervals.minAsLongArray( interval );
		final long[] max = Intervals.maxAsLongArray( interval );
		final long[] dimensions = Intervals.dimensionsAsLongArray( space );
		final long[] position = min.clone();
		for ( int d = 0; d < n; )
		{
			function.accept( IntervalIndexer.positionToIndex( position, dimensions ) );
			for ( d = 0; d < n; ++d )
			{
				position[ d ]++;
				if ( position[ d ] <= max[ d ] )
					break;
				else
					position[ d ] = min[ d ];
			}
		}
	}

	/**
	 * Applies a user-defined {@code function} for each pixel of an {@code interval}.
	 */
	public static void forEachPixel( final Dimensions dimensions, final LongConsumer function )
	{
		forEachPixel( new FinalInterval( dimensions ), dimensions, function );
	}

	/**
	 * Cuts given region into a set of non-overlapping cubes with a side length of {@code subregionSize}.
	 * @return a list of non-overlapping tiles that form a given region of space.
	 */
	public static ArrayList< TileInfo > divideSpaceBySize( final Interval space, final int subregionSize )
	{
		final long[] subregionDimsArr = new long[ space.numDimensions() ];
		Arrays.fill( subregionDimsArr, subregionSize );
		return divideSpace( space, new FinalDimensions( subregionDimsArr ) );
	}

	/**
	 * Cuts given region into a set of non-overlapping tiles with exactly {@code subregionsCountPerDim} tiles for each dimension.
	 * @return a list of non-overlapping tiles that form a given region of space.
	 */
	public static ArrayList< TileInfo > divideSpaceByCount( final Interval space, final int subregionsCountPerDim )
	{
		final int[] subregionsCountPerDimArr = new int[ space.numDimensions() ];
		Arrays.fill( subregionsCountPerDimArr, subregionsCountPerDim );
		return divideSpaceByCount( space, subregionsCountPerDimArr );
	}

	/**
	 * Cuts given region into a set of non-overlapping tiles with exactly {@code subregionsCountPerDim} tiles specified for each dimension separately.
	 * @return a list of non-overlapping tiles that form a given region of space.
	 */
	public static ArrayList< TileInfo > divideSpaceByCount( final Interval space, final int[] subregionsCountPerDim )
	{
		final long[] subregionDimsArr = new long[ space.numDimensions() ];
		final int[] sizePlusOneCount = new int[ space.numDimensions() ];
		for ( int d = 0; d < subregionDimsArr.length; d++ )
		{
			subregionDimsArr[ d ] = space.dimension( d ) / subregionsCountPerDim[ d ];
			sizePlusOneCount[ d ] = ( int ) ( space.dimension( d ) - subregionsCountPerDim[ d ] * subregionDimsArr[ d ] );
		}
		return divideSpace( space, new FinalDimensions( subregionDimsArr ), sizePlusOneCount );
	}

	/**
	 * Cuts given region into a set of non-overlapping tiles of a specified size.
	 * @return a list of non-overlapping tiles that form a given region of space.
	 */
	public static ArrayList< TileInfo > divideSpace( final Interval space, final Dimensions subregionDims )
	{
		return divideSpace( space, subregionDims, new int[ space.numDimensions() ] );
	}

	private static ArrayList< TileInfo > divideSpace( final Interval space, final Dimensions subregionDims, final int[] sizePlusOneCount )
	{
		final ArrayList< TileInfo > subregions = new ArrayList<>();
		divideSpaceRecursive( space, subregionDims, sizePlusOneCount, subregions, new TileInfo( space.numDimensions() ), 0 );
		for ( int i = 0; i < subregions.size(); i++ )
			subregions.get( i ).setIndex( i );
		return subregions;
	}
	private static void divideSpaceRecursive( final Interval space, final Dimensions subregionDims, final int[] sizePlusOneCount, final List< TileInfo > subregions, final TileInfo currSubregion, final int currDim )
	{
		if ( currDim == space.numDimensions() )
		{
			subregions.add( currSubregion );
			return;
		}

		int i = 0;
		for ( long coord = space.min( currDim ); coord <= space.max( currDim ); coord += subregionDims.dimension( currDim ), i++ )
		{
			final TileInfo newSubregion = currSubregion.clone();
			newSubregion.setPosition( currDim, coord );
			newSubregion.setSize( currDim, Math.min( subregionDims.dimension( currDim ), space.max( currDim ) - coord + 1 ) );

			if ( i < sizePlusOneCount[ currDim ] )
			{
				coord++;
				newSubregion.setSize( currDim, newSubregion.getSize( currDim ) + 1 );
			}

			divideSpaceRecursive( space, subregionDims, sizePlusOneCount, subregions, newSubregion, currDim + 1 );
		}
	}
}