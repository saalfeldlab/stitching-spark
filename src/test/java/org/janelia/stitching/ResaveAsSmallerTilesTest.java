package org.janelia.stitching;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.Interval;
import net.imglib2.util.Intervals;

public class ResaveAsSmallerTilesTest
{
	@Test
	public void testFitsExactly()
	{
		final long[] originalTileSize = new long[] { 6, 5, 13 }, newTileSize = new long[] { 6, 5, 4 };
		final List< Interval > newTilesIntervals = ResaveAsSmallerTilesSpark.getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize, 2, 0.25 );

		for ( final Interval newTileInterval : newTilesIntervals )
			Assert.assertArrayEquals( new long[] { 0, 0, 5, 4 }, new long[] { newTileInterval.min( 0 ), newTileInterval.min( 1 ), newTileInterval.max( 0 ), newTileInterval.max( 1 ) } );

		final long[] newTilesRetileDimensionMins = new long[ newTilesIntervals.size() ], newTilesRetileDimensionMaxs = new long[ newTilesIntervals.size() ];
		for ( int i = 0; i < newTilesIntervals.size(); ++i )
		{
			newTilesRetileDimensionMins[ i ] = newTilesIntervals.get( i ).min( 2 );
			newTilesRetileDimensionMaxs[ i ] = newTilesIntervals.get( i ).max( 2 );
		}

		Assert.assertArrayEquals( new long[] { 0, 3, 6, 9  }, newTilesRetileDimensionMins );
		Assert.assertArrayEquals( new long[] { 3, 6, 9, 12 }, newTilesRetileDimensionMaxs );
	}

	@Test
	public void testOneCorrection()
	{
		final long[] originalTileSize = new long[] { 6, 5, 12 }, newTileSize = new long[] { 6, 5, 4 };
		final List< Interval > newTilesIntervals = ResaveAsSmallerTilesSpark.getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize, 2, 0.25 );

		for ( final Interval newTileInterval : newTilesIntervals )
			Assert.assertArrayEquals( new long[] { 0, 0, 5, 4 }, new long[] { newTileInterval.min( 0 ), newTileInterval.min( 1 ), newTileInterval.max( 0 ), newTileInterval.max( 1 ) } );

		final long[] newTilesRetileDimensionMins = new long[ newTilesIntervals.size() ], newTilesRetileDimensionMaxs = new long[ newTilesIntervals.size() ];
		for ( int i = 0; i < newTilesIntervals.size(); ++i )
		{
			newTilesRetileDimensionMins[ i ] = newTilesIntervals.get( i ).min( 2 );
			newTilesRetileDimensionMaxs[ i ] = newTilesIntervals.get( i ).max( 2 );
		}

		Assert.assertArrayEquals( new long[] { 0, 2, 5, 8  }, newTilesRetileDimensionMins );
		Assert.assertArrayEquals( new long[] { 3, 5, 8, 11 }, newTilesRetileDimensionMaxs );
	}

	@Test
	public void testTwoCorrections()
	{
		final long[] originalTileSize = new long[] { 6, 5, 11 }, newTileSize = new long[] { 6, 5, 4 };
		final List< Interval > newTilesIntervals = ResaveAsSmallerTilesSpark.getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize, 2, 0.25 );

		for ( final Interval newTileInterval : newTilesIntervals )
			Assert.assertArrayEquals( new long[] { 0, 0, 5, 4 }, new long[] { newTileInterval.min( 0 ), newTileInterval.min( 1 ), newTileInterval.max( 0 ), newTileInterval.max( 1 ) } );

		final long[] newTilesRetileDimensionMins = new long[ newTilesIntervals.size() ], newTilesRetileDimensionMaxs = new long[ newTilesIntervals.size() ];
		for ( int i = 0; i < newTilesIntervals.size(); ++i )
		{
			newTilesRetileDimensionMins[ i ] = newTilesIntervals.get( i ).min( 2 );
			newTilesRetileDimensionMaxs[ i ] = newTilesIntervals.get( i ).max( 2 );
		}

		Assert.assertArrayEquals( new long[] { 0, 2, 4, 7  }, newTilesRetileDimensionMins );
		Assert.assertArrayEquals( new long[] { 3, 5, 7, 10 }, newTilesRetileDimensionMaxs );
	}

	@Test
	public void testLargeValues()
	{
		final long[] originalTileSize = new long[] { 100, 150, 1128 }, newTileSize = ResaveAsSmallerTilesSpark.determineNewTileSize( originalTileSize, 2 );
		Assert.assertArrayEquals( new long[] { 100, 150, 125 }, newTileSize );

		final List< Interval > newTilesIntervals = ResaveAsSmallerTilesSpark.getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize, 2, 0.3 );

		final long[] newTilesRetileDimensionMins = new long[ newTilesIntervals.size() ];
		for ( int i = 0; i < newTilesIntervals.size(); ++i )
			newTilesRetileDimensionMins[ i ] = newTilesIntervals.get( i ).min( 2 );

		Interval union = newTilesIntervals.get( 0 );
		for ( int i = 1; i < newTilesIntervals.size(); ++i )
			union = Intervals.union( newTilesIntervals.get( i ), union );
		Assert.assertArrayEquals( new long[] { 0, 0, 0 }, Intervals.minAsLongArray( union ) );
		Assert.assertArrayEquals( new long[] { 99, 149, 1127 }, Intervals.maxAsLongArray( union ) );
	}
}
