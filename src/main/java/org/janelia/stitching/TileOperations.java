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
import net.imglib2.util.IntervalsHelper;

public class TileOperations
{
	/**
	 * Pads the {@code interval} with {@code padding}.
	 * Tries to equally pad the {@code interval} from both sides (which means to expand it by {@code padding}/2 on each side).
	 * If the {@code padding} is odd, it adds the remaining 1 towards the min side.
	 */
	public static Interval padInterval( final Interval interval, final long[] padding )
	{
		final long[] spaceMin = new long[ interval.numDimensions() ], spaceMax = new long[ interval.numDimensions() ];
		Arrays.fill( spaceMin, Long.MIN_VALUE );
		Arrays.fill( spaceMax, Long.MAX_VALUE );
		return padInterval( interval, new FinalInterval( spaceMin, spaceMax ), padding );
	}

	/**
	 * Pads the {@code interval} defined within the outer space of size {@code outerDimensions} with {@code padding}.
	 * Tries to equally pad the {@code interval} from both sides (which means to expand it by {@code padding}/2 on each side).
	 * If the {@code padding} is odd, it adds the remaining 1 towards the min side.
	 * The padded interval cannot exceed the outer space boundaries (e.g., 0 and {@code outerDimensions}-1), so in this case the remainder is applied towards the other side.
	 * If the resulting interval has reached the outer space size (0, {@code outerDimensions}-1), the rest of the remaining padding is ignored.
	 */
	public static Interval padInterval( final Interval interval, final Interval outerSpace, final long[] padding )
	{
		final long[] paddedMin = new long[ interval.numDimensions() ], paddedMax = new long[ interval.numDimensions() ];
		for ( int d = 0; d < interval.numDimensions(); d++ )
		{
			paddedMin[ d ] = Math.max( interval.min( d ) - padding[ d ] / 2, outerSpace.min( d ) );
			paddedMax[ d ] = Math.min( interval.max( d ) + padding[ d ] / 2, outerSpace.max( d ) );

			final long remainder = padding[ d ] - ( interval.min( d ) - paddedMin[ d ] ) - ( paddedMax[ d ] - interval.max( d ) );
			if ( remainder > 0 )
			{
				if ( paddedMin[ d ] == outerSpace.min( d ) )
					paddedMax[ d ] = Math.min( paddedMax[ d ] + remainder, outerSpace.max( d ) );
				else
					paddedMin[ d ] = Math.max( paddedMin[ d ] - remainder, outerSpace.min( d ) );
			}
		}
		return new FinalInterval( paddedMin, paddedMax );
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
	public static Interval getOverlappingRegion( final TileInfo t1, final TileInfo t2 )
	{
		final long[] min = new long[ t1.numDimensions() ], max = new long[ t1.numDimensions() ];
		for ( int d = 0; d < t1.numDimensions(); d++ )
		{
			final long p1 = Math.round( t1.getStagePosition( d ) ), p2 = Math.round( t2.getStagePosition( d ) );
			final long s1 = t1.getSize( d ), s2 = t2.getSize( d );

			if ( !( ( p2 >= p1 && p2 < p1 + s1 ) || ( p1 >= p2 && p1 < p2 + s2 ) ) )
				return null;

			min[ d ] = Math.max( 0, p2 - p1 );
			max[ d ] = Math.min( s1 - 1, p2 + s2 - p1 - 1 );
		}
		return new FinalInterval( min, max );
	}

	/**
	 * @return an overlap as a RealInterval with relative coordinates of the first tile
	 */
	public static RealInterval getOverlappingRegionReal( final TileInfo t1, final TileInfo t2 )
	{
		final double[] min = new double[ t1.numDimensions() ], max = new double[ t1.numDimensions() ];
		for ( int d = 0; d < t1.numDimensions(); d++ )
		{
			final double p1 = t1.getStagePosition( d ), p2 = t2.getStagePosition( d );
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
		return overlap( t1.getStageInterval(), t2.getStageInterval() );
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
	public static Interval getOverlappingRegionGlobal( final TileInfo t1, final TileInfo t2 )
	{
		final Interval r = getOverlappingRegion( t1, t2 );
		if ( r == null )
			return null;

		final long[] globalMin = new long[ r.numDimensions() ], globalMax = new long[ r.numDimensions() ];
		final long[] offset = Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( t1.getStageInterval() ) );
		for ( int d = 0; d < r.numDimensions(); d++ )
		{
			globalMin[ d ] = r.min( d ) + offset[ d ];
			globalMax[ d ] = r.max( d ) + offset[ d ];
		}
		return new FinalInterval( globalMin, globalMax );
	}

	/**
	 * @return a list of tiles lying within specified subregion (overlapping with it)
	 */
	public static ArrayList< TileInfo > findTilesWithinSubregion( final TileInfo[] tiles, final TileInfo subregion )
	{
		final ArrayList< TileInfo > tilesWithinSubregion = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( overlap( tile, subregion ) )
				tilesWithinSubregion.add( tile );
		return tilesWithinSubregion;
	}

	public static Set< Integer > findTilesWithinSubregion( final Map< Integer, ? extends Interval > tiles, final Interval subregion )
	{
		final Set< Integer > tilesWithinSubregion = new HashSet<>();
		for ( final Entry< Integer, ? extends Interval > entry : tiles.entrySet() )
			if ( overlap( entry.getValue(), subregion ) )
				tilesWithinSubregion.add( entry.getKey() );
		return tilesWithinSubregion;
	}

	public static ArrayList< TileInfo > findTilesWithinSubregion( final TileInfo[] tiles, final RealInterval subregion )
	{
		final ArrayList< TileInfo > tilesWithinSubregion = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( overlap( tile.getStageInterval(), subregion ) )
				tilesWithinSubregion.add( tile );
		return tilesWithinSubregion;
	}

	/**
	 * @return an integer bounding box of a collection of tiles
	 */
	public static Interval getCollectionBoundaries( final TileInfo[] tiles )
	{
		if ( tiles.length == 0 )
			return null;

		final int dim = tiles[ 0 ].numDimensions();

		final long[] min = new long[ dim ], max = new long[ dim ];
		Arrays.fill( min, Long.MAX_VALUE );
		Arrays.fill( max, Long.MIN_VALUE );

		for ( final TileInfo tile : tiles )
		{
			final Interval tileBoundaries = IntervalsHelper.roundRealInterval( tile.getStageInterval() );
			for ( int d = 0; d < dim; d++ )
			{
				min[ d ] = Math.min( min[ d ], tileBoundaries.min( d ) );
				max[ d ] = Math.max( max[ d ], tileBoundaries.max( d ) );
			}
		}

		return new FinalInterval( min, max );
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
				min[ d ] = Math.min( min[ d ], tile.getStagePosition( d ) );
				max[ d ] = Math.max( max[ d ], tile.getStagePosition( d ) + tile.getSize( d ) - 1 );
			}
		}

		return new FinalRealInterval( min, max );
	}

	/**
	 * Translates a set of tiles in a way such that the lowest coordinate of every dimension is at the origin.
	 */
	public static void translateTilesToOrigin( final TileInfo[] tiles )
	{
		final Interval space = getCollectionBoundaries( tiles );
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setStagePosition( d, Math.round( tile.getStagePosition( d ) ) - space.min( d ) );
	}
	public static void translateTilesToOriginReal( final TileInfo[] tiles )
	{
		final RealInterval space = getRealCollectionBoundaries( tiles );
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setStagePosition( d, tile.getStagePosition( d ) - space.realMin( d ) );
	}

	/**
	 * Applies translation to a set of tiles with specified {@code offset}.
	 */
	public static void translateTiles( final TileInfo[] tiles, final double[] offset )
	{
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setStagePosition( d, tile.getStagePosition( d ) + offset[ d ] );
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
	 * Cuts the given space interval into a set of non-overlapping subintervals
	 * with a side length of {@code subregionSize}, where edge subintervals may be smaller.
	 */
	public static ArrayList< Interval > divideSpaceBySize( final Interval space, final int subregionSize )
	{
		final long[] subregionDimsArr = new long[ space.numDimensions() ];
		Arrays.fill( subregionDimsArr, subregionSize );
		return divideSpace( space, new FinalDimensions( subregionDimsArr ) );
	}

	/**
	 * Cuts the given space interval into a set of non-overlapping subintervals
	 * with exactly {@code subregionsCountPerDim} subintervals for each dimension.
	 * Some of the resulting subintervals may be extended by 1px to cover the entire space.
	 */
	public static ArrayList< Interval > divideSpaceByCount( final Interval space, final int subregionsCountPerDim )
	{
		final int[] subregionsCountPerDimArr = new int[ space.numDimensions() ];
		Arrays.fill( subregionsCountPerDimArr, subregionsCountPerDim );
		return divideSpaceByCount( space, subregionsCountPerDimArr );
	}

	/**
	 * Cuts given region into a set of non-overlapping tiles with exactly {@code subregionsCountPerDim} tiles specified for each dimension separately.
	 * @return a list of non-overlapping tiles that form a given region of space.
	 */
	public static ArrayList< Interval > divideSpaceByCount( final Interval space, final int[] subregionsCountPerDim )
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
	 * Cuts given region into a set of non-overlapping tiles of specified size.
	 * @return a list of non-overlapping tiles that form a given region of space.
	 */
	public static ArrayList< Interval > divideSpace( final Interval space, final Dimensions subregionDims )
	{
		return divideSpace( space, subregionDims, new int[ space.numDimensions() ] );
	}

	/**
	 * Cuts given region into a set of non-overlapping tiles of specified size. Ignores tiles that are smaller than specified size.
	 * @return
	 */
	public static ArrayList< Interval > divideSpaceIgnoreSmaller( final Interval space, final Dimensions subregionDims )
	{
		return divideSpace( space, subregionDims, new int[ space.numDimensions() ], true );
	}

	private static ArrayList< Interval > divideSpace(
			final Interval space,
			final Dimensions subregionDims,
			final int[] sizePlusOneCount )
	{
		return divideSpace( space, subregionDims, sizePlusOneCount, false );
	}

	private static ArrayList< Interval > divideSpace(
			final Interval space,
			final Dimensions subregionDims,
			final int[] sizePlusOneCount,
			final boolean ignoreSmaller )
	{
		final ArrayList< Interval > subregions = new ArrayList<>();
		divideSpaceRecursive(
				space,
				subregionDims,
				sizePlusOneCount,
				ignoreSmaller,
				subregions,
				new long[ space.numDimensions() ],
				new long[ space.numDimensions() ],
				0
			);
		return subregions;
	}

	private static void divideSpaceRecursive(
			final Interval space,
			final Dimensions subregionDims,
			final int[] sizePlusOneCount,
			final boolean ignoreSmaller,
			final List< Interval > subregions,
			final long[] currMin,
			final long[] currMax,
			final int currDim )
	{
		if ( currDim == space.numDimensions() )
		{
			subregions.add( new FinalInterval( currMin, currMax ) );
			return;
		}

		int i = 0;
		for ( long minCoord = space.min( currDim ); minCoord <= space.max( currDim ); minCoord += subregionDims.dimension( currDim ), i++ )
		{
			long maxCoord = Math.min( minCoord + subregionDims.dimension( currDim ) - 1, space.max( currDim ) );
			if ( i < sizePlusOneCount[ currDim ] )
			{
				++minCoord;
				++maxCoord;
			}

			if ( !ignoreSmaller || maxCoord - minCoord + 1 >= subregionDims.dimension( currDim ) )
			{
				final long[] newMin = currMin.clone(), newMax = currMax.clone();
				newMin[ currDim ] = minCoord;
				newMax[ currDim ] = maxCoord;
				divideSpaceRecursive(
						space,
						subregionDims,
						sizePlusOneCount,
						ignoreSmaller,
						subregions,
						newMin,
						newMax,
						currDim + 1
					);
			}
		}
	}
}
