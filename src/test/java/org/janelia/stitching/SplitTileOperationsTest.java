package org.janelia.stitching;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.Interval;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;

public class SplitTileOperationsTest
{
	private static final double EPSILON = 1e-9;

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
		for ( final TileInfo tileBox : tileBoxes )
		{
			final long[] mins = Intervals.minAsLongArray( tileBox.getBoundaries() );
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
		for ( final TileInfo tileBox : tileBoxes )
			Assert.assertArrayEquals( new long[] { 25, 30, 35 }, Intervals.dimensionsAsLongArray( tileBox.getBoundaries() ) );

		// check that reference to the original tile is preserved
		for ( final TileInfo tileBox : tileBoxes )
			Assert.assertEquals( tile, tileBox.getOriginalTile() );
	}

	@Test
	public void transformedMovingTileBoxMiddlePointTest()
	{
		final TileInfo[] tiles = new TileInfo[ 2 ];
		tiles[ 0 ] = new TileInfo( 3 );
		tiles[ 0 ].setIndex( 0 );
		tiles[ 0 ].setPosition( new double[] { 100, 200, 300 } );
		tiles[ 0 ].setSize( new long[] { 50, 60, 70 } );

		tiles[ 1 ] = new TileInfo( 3 );
		tiles[ 1 ].setIndex( 1 );
		tiles[ 1 ].setPosition( new double[] { 140, 210, 290 } );
		tiles[ 1 ].setSize( new long[] { 90, 80, 70 } );

		final List< TileInfo > tileBoxes = SplitTileOperations.splitTilesIntoBoxes( tiles, new int[] { 2, 2, 2 } );
		Assert.assertEquals( 16, tileBoxes.size() );

		// test left-top-front tile box of the second tile
		{
			final TileInfo movingTileBox = tileBoxes.get( 8 );
			Assert.assertArrayEquals( new double[] { 22.5, 20, 17.5 }, SplitTileOperations.getTileBoxMiddlePoint( movingTileBox ), EPSILON );
			final Interval transformedSecondTileBox = SplitTileOperations.transformMovingTileBox( TileOperations.getTileTransform( tiles[ 0 ] ), movingTileBox );
			Assert.assertArrayEquals( new long[] { 40, 10, -10 }, Intervals.minAsLongArray( transformedSecondTileBox ) );
			Assert.assertArrayEquals( movingTileBox.getSize(), Intervals.dimensionsAsLongArray( transformedSecondTileBox ) );
		}

		// test right-bottom-back tile box of the second tile
		{
			final TileInfo movingTileBox = tileBoxes.get( 15 );
			Assert.assertArrayEquals( new double[] { 67.5, 60, 52.5 }, SplitTileOperations.getTileBoxMiddlePoint( movingTileBox ), EPSILON );
			final Interval transformedSecondTileBox = SplitTileOperations.transformMovingTileBox( TileOperations.getTileTransform( tiles[ 0 ] ), movingTileBox );
			Assert.assertArrayEquals( new long[] { 85, 50, 25 }, Intervals.minAsLongArray( transformedSecondTileBox ) );
			Assert.assertArrayEquals( movingTileBox.getSize(), Intervals.dimensionsAsLongArray( transformedSecondTileBox ) );
		}
	}

	@Test
	public void testOverlappingTileBoxes()
	{
		final TileInfo[] tiles = new TileInfo[ 2 ];
		tiles[ 0 ] = new TileInfo( 3 );
		tiles[ 0 ].setIndex( 0 );
		tiles[ 0 ].setPosition( new double[] { 100, 200, 300 } );
		tiles[ 0 ].setSize( new long[] { 50, 60, 70 } );

		tiles[ 1 ] = new TileInfo( 3 );
		tiles[ 1 ].setIndex( 1 );
		tiles[ 1 ].setPosition( new double[] { 140, 210, 290 } );
		tiles[ 1 ].setSize( new long[] { 90, 80, 70 } );

		final List< TileInfo > tileBoxes = SplitTileOperations.splitTilesIntoBoxes( tiles, new int[] { 2, 2, 2 } );
		Assert.assertEquals( 16, tileBoxes.size() );

		final List< TilePair > overlappingTileBoxes = SplitTileOperations.findOverlappingTileBoxes( tileBoxes );
		Assert.assertEquals( 4, overlappingTileBoxes.size() );

		// test references to the original tiles and their order
		for ( final TilePair tileBoxPair : overlappingTileBoxes )
		{
			Assert.assertEquals( tiles[ 0 ], tileBoxPair.getA().getOriginalTile() );
			Assert.assertEquals( tiles[ 1 ], tileBoxPair.getB().getOriginalTile() );
		}

		// test mins of overlapping pairs
		final Map< Long, TilePair > actualMinsFirstTile = new TreeMap<>();
		for ( final TilePair tileBoxPair : overlappingTileBoxes )
		{
			final long[] minsFirstTile = Intervals.minAsLongArray( tileBoxPair.getA().getBoundaries() );
			final long key = IntervalIndexer.positionToIndex( minsFirstTile, tiles[ 0 ].getSize() );
			Assert.assertNull( actualMinsFirstTile.put( key, tileBoxPair ) );
		}
		final long[][] expectedMinsFirstTileArray = new long[][] {
			new long[] { 25, 0, 0 },
			new long[] { 25, 30, 0 },
			new long[] { 25, 0, 35 },
			new long[] { 25, 30, 35 },
		};
		final long[][] expectedMinsSecondTileArray = new long[][] {
			new long[] { 0, 0, 0 },
			new long[] { 0, 0, 0 },
			new long[] { 0, 0, 35 },
			new long[] { 0, 0, 35 },
		};
		for ( int i = 0; i < overlappingTileBoxes.size(); ++i )
		{
			final long key = IntervalIndexer.positionToIndex( expectedMinsFirstTileArray[ i ], tiles[ 0 ].getSize() );
			final TilePair tileBoxPair = actualMinsFirstTile.get( key );
			Assert.assertArrayEquals( expectedMinsFirstTileArray[ i ], Intervals.minAsLongArray( tileBoxPair.getA().getBoundaries() ) );
			Assert.assertArrayEquals( expectedMinsSecondTileArray[ i ], Intervals.minAsLongArray( tileBoxPair.getB().getBoundaries() ) );
		}
	}
}
