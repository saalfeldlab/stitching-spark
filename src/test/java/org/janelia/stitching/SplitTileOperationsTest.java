package org.janelia.stitching;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;

public class SplitTileOperationsTest
{
	@Test
	public void splitTilesIntoBoxesTest()
	{
		final TileInfo tile = new TileInfo( 3 );
		tile.setPosition( new double[] { 100, 200, 300 } );
		tile.setSize( new long[] { 50, 60, 70 } );

		final List< TileInfo > tileBoxes = SplitTileOperations.splitTilesIntoBoxes( new TileInfo[] { tile }, new int[] { 2, 2, 2 } );
		Assert.assertEquals( 8, tileBoxes.size() );

		// test mins
		final Map< Long, long[] > actualMins = new TreeMap<>();
		for ( int i = 0; i < 8; ++i )
		{
			final long[] mins = Intervals.minAsLongArray( tileBoxes.get( i ).getBoundaries() );
			final long key = IntervalIndexer.positionToIndex( mins, tile.getSize() );
			Assert.assertNull( actualMins.put( key, mins ) );
		}
		final long[][] expectedMinsArray = new long[][] {
			new long[] { 0, 0, 0 },
			new long[] { 25, 0, 0 },
			new long[] { 0, 30, 0 },
			new long[] { 25, 30, 0 },
			new long[] { 0, 0, 35 },
			new long[] { 25, 0, 35 },
			new long[] { 0, 30, 35 },
			new long[] { 25, 30, 35 },
		};
		for ( final long[] expectedMin : expectedMinsArray )
		{
			final long key = IntervalIndexer.positionToIndex( expectedMin, tile.getSize() );
			Assert.assertArrayEquals( expectedMin, actualMins.get( key ) );
		}

		// test dimensions
		for ( int i = 0; i < 8; ++i )
			Assert.assertArrayEquals( new long[] { 25, 30, 35 }, Intervals.dimensionsAsLongArray( tileBoxes.get( i ).getBoundaries() ) );

		// check that reference to the original tile is preserved
		for ( final TileInfo tileBox : tileBoxes )
			Assert.assertEquals( tile, tileBox.getOriginalTile() );
	}
}
