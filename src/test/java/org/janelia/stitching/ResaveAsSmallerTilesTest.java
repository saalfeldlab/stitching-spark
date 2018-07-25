package org.janelia.stitching;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.Interval;

public class ResaveAsSmallerTilesTest
{
	@Test
	public void testFitsExactly()
	{
		final long[] originalTileSize = new long[] { 6, 5, 13 }, newTileSize = new long[] { 6, 5, 4 };
		final List< Interval > newTilesIntervals = ResaveAsSmallerTilesSpark.getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize, 2, 0.25 );

		for ( final Interval newTileInterval : newTilesIntervals )
			Assert.assertArrayEquals( new long[] { 0, 0, 5, 4 }, new long[] { newTileInterval.min( 0 ), newTileInterval.min( 1 ), newTileInterval.max( 0 ), newTileInterval.max( 1 ) } );

		final long[] newTilesSplitDimensionMins = new long[ newTilesIntervals.size() ], newTilesSplitDimensionMaxs = new long[ newTilesIntervals.size() ];
		for ( int i = 0; i < newTilesIntervals.size(); ++i )
		{
			newTilesSplitDimensionMins[ i ] = newTilesIntervals.get( i ).min( 2 );
			newTilesSplitDimensionMaxs[ i ] = newTilesIntervals.get( i ).max( 2 );
		}

		Assert.assertArrayEquals( new long[] { 0, 3, 6, 9  }, newTilesSplitDimensionMins );
		Assert.assertArrayEquals( new long[] { 3, 6, 9, 12 }, newTilesSplitDimensionMaxs );
	}

	@Test
	public void testOneCorrection()
	{
		final long[] originalTileSize = new long[] { 6, 5, 12 }, newTileSize = new long[] { 6, 5, 4 };
		final List< Interval > newTilesIntervals = ResaveAsSmallerTilesSpark.getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize, 2, 0.25 );

		for ( final Interval newTileInterval : newTilesIntervals )
			Assert.assertArrayEquals( new long[] { 0, 0, 5, 4 }, new long[] { newTileInterval.min( 0 ), newTileInterval.min( 1 ), newTileInterval.max( 0 ), newTileInterval.max( 1 ) } );

		final long[] newTilesSplitDimensionMins = new long[ newTilesIntervals.size() ], newTilesSplitDimensionMaxs = new long[ newTilesIntervals.size() ];
		for ( int i = 0; i < newTilesIntervals.size(); ++i )
		{
			newTilesSplitDimensionMins[ i ] = newTilesIntervals.get( i ).min( 2 );
			newTilesSplitDimensionMaxs[ i ] = newTilesIntervals.get( i ).max( 2 );
		}

		Assert.assertArrayEquals( new long[] { 0, 2, 5, 8  }, newTilesSplitDimensionMins );
		Assert.assertArrayEquals( new long[] { 3, 5, 8, 11 }, newTilesSplitDimensionMaxs );
	}

	@Test
	public void testTwoCorrections()
	{
		final long[] originalTileSize = new long[] { 6, 5, 11 }, newTileSize = new long[] { 6, 5, 4 };
		final List< Interval > newTilesIntervals = ResaveAsSmallerTilesSpark.getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize, 2, 0.25 );

		for ( final Interval newTileInterval : newTilesIntervals )
			Assert.assertArrayEquals( new long[] { 0, 0, 5, 4 }, new long[] { newTileInterval.min( 0 ), newTileInterval.min( 1 ), newTileInterval.max( 0 ), newTileInterval.max( 1 ) } );

		final long[] newTilesSplitDimensionMins = new long[ newTilesIntervals.size() ], newTilesSplitDimensionMaxs = new long[ newTilesIntervals.size() ];
		for ( int i = 0; i < newTilesIntervals.size(); ++i )
		{
			newTilesSplitDimensionMins[ i ] = newTilesIntervals.get( i ).min( 2 );
			newTilesSplitDimensionMaxs[ i ] = newTilesIntervals.get( i ).max( 2 );
		}

		Assert.assertArrayEquals( new long[] { 0, 2, 4, 7  }, newTilesSplitDimensionMins );
		Assert.assertArrayEquals( new long[] { 3, 5, 7, 10 }, newTilesSplitDimensionMaxs );
	}
}
