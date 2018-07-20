package org.janelia.stitching;

import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.Interval;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;
import net.imglib2.util.IntervalsNullable;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class SubTileOperationsTest
{
	private static final double EPSILON = 1e-9;

	@Test
	public void splitTilesTest()
	{
		final TileInfo tile = new TileInfo( 3 );
		tile.setStagePosition( new double[] { 100, 200, 300 } );
		tile.setSize( new long[] { 50, 60, 70 } );

		final List< SubTile > subTiles = SubTileOperations.subdivideTiles( new TileInfo[] { tile }, new int[] { 2, 2, 2 } );
		Assert.assertEquals( 8, subTiles.size() );

		// test mins
		final Map< Long, long[] > actualMins = new TreeMap<>();
		for ( final SubTile subTile : subTiles )
		{
			final long[] mins = Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTile ) );
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
		for ( final SubTile subTile : subTiles )
			Assert.assertArrayEquals( new long[] { 25, 30, 35 }, Intervals.dimensionsAsLongArray( IntervalsHelper.roundRealInterval( subTile ) ) );

		// check that reference to the original tile is preserved
		for ( final SubTile subTile : subTiles )
			Assert.assertEquals( tile, subTile.getFullTile() );
	}

	@Test
	public void transformedMovingSubTileMiddlePointTest()
	{
		final TileInfo[] tiles = new TileInfo[ 2 ];
		tiles[ 0 ] = new TileInfo( 3 );
		tiles[ 0 ].setIndex( 0 );
		tiles[ 0 ].setStagePosition( new double[] { 100, 200, 300 } );
		tiles[ 0 ].setSize( new long[] { 50, 60, 70 } );

		tiles[ 1 ] = new TileInfo( 3 );
		tiles[ 1 ].setIndex( 1 );
		tiles[ 1 ].setStagePosition( new double[] { 140, 210, 290 } );
		tiles[ 1 ].setSize( new long[] { 90, 80, 70 } );

		final List< SubTile > subTiles = SubTileOperations.subdivideTiles( tiles, new int[] { 2, 2, 2 } );
		Assert.assertEquals( 16, subTiles.size() );

		// test left-top-front subtile of the second tile
		{
			final SubTile movingSubTile = subTiles.get( 8 );
			Assert.assertArrayEquals( new double[] { 22.5, 20, 17.5 }, SubTileOperations.getSubTileMiddlePoint( movingSubTile ), EPSILON );
			final Pair< Interval, Interval > transformedInGlobalSpace = TransformedTileOperations.transformSubTilePair( new SubTilePair( subTiles.get( 0 ), movingSubTile ), false );
			final Interval transformedSecondSubTile = SubTileOperations.globalToFixedSpace( transformedInGlobalSpace ).getB();
			Assert.assertArrayEquals( new long[] { 40, 10, -10 }, Intervals.minAsLongArray( transformedSecondSubTile ) );
			Assert.assertArrayEquals( Intervals.dimensionsAsLongArray( movingSubTile ), Intervals.dimensionsAsLongArray( transformedSecondSubTile ) );
		}

		// test right-bottom-back subtile of the second tile
		{
			final SubTile movingSubTile = subTiles.get( 15 );
			Assert.assertArrayEquals( new double[] { 67.5, 60, 52.5 }, SubTileOperations.getSubTileMiddlePoint( movingSubTile ), EPSILON );
			final Pair< Interval, Interval > transformedInGlobalSpace = TransformedTileOperations.transformSubTilePair( new SubTilePair( subTiles.get( 0 ), movingSubTile ), false );
			final Interval transformedSecondSubTile = SubTileOperations.globalToFixedSpace( transformedInGlobalSpace ).getB();
			Assert.assertArrayEquals( new long[] { 85, 50, 25 }, Intervals.minAsLongArray( transformedSecondSubTile ) );
			Assert.assertArrayEquals( Intervals.dimensionsAsLongArray( movingSubTile ), Intervals.dimensionsAsLongArray( transformedSecondSubTile ) );
		}

		// test world-only
		{
			final Pair< Interval, Interval > transformedInGlobalSpace = TransformedTileOperations.transformSubTilePair( new SubTilePair( subTiles.get( 0 ), subTiles.get( 1 ) ), true );
			Assert.assertTrue( transformedInGlobalSpace.getA() == null && transformedInGlobalSpace.getB() == null );
		}
	}

	@Test
	public void testOverlappingSubTiles()
	{
		final TileInfo[] tiles = new TileInfo[ 2 ];
		tiles[ 0 ] = new TileInfo( 3 );
		tiles[ 0 ].setIndex( 0 );
		tiles[ 0 ].setStagePosition( new double[] { 100, 200, 300 } );
		tiles[ 0 ].setSize( new long[] { 50, 60, 70 } );

		tiles[ 1 ] = new TileInfo( 3 );
		tiles[ 1 ].setIndex( 1 );
		tiles[ 1 ].setStagePosition( new double[] { 140, 210, 290 } );
		tiles[ 1 ].setSize( new long[] { 90, 80, 70 } );

		final List< SubTile > subTiles = SubTileOperations.subdivideTiles( tiles, new int[] { 2, 2, 2 } );
		Assert.assertEquals( 16, subTiles.size() );

		final List< SubTilePair > overlappingSubTiles = SubTileOperations.findOverlappingSubTiles( subTiles, true, false );
		Assert.assertEquals( 4, overlappingSubTiles.size() );

		// world-only should yield an empty list
		Assert.assertTrue( SubTileOperations.findOverlappingSubTiles( subTiles, true, true ).isEmpty() );

		// test references to the full tiles and their order
		for ( final SubTilePair subTilePair : overlappingSubTiles )
		{
			Assert.assertEquals( tiles[ 0 ], subTilePair.getA().getFullTile() );
			Assert.assertEquals( tiles[ 1 ], subTilePair.getB().getFullTile() );
		}

		// test mins of overlapping pairs
		final Map< Long, SubTilePair > actualMinsFirstTile = new TreeMap<>();
		for ( final SubTilePair subTilePair : overlappingSubTiles )
		{
			final long[] minsFirstTile = Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTilePair.getA() ) );
			final long key = IntervalIndexer.positionToIndex( minsFirstTile, tiles[ 0 ].getSize() );
			Assert.assertNull( actualMinsFirstTile.put( key, subTilePair ) );
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
		for ( int i = 0; i < overlappingSubTiles.size(); ++i )
		{
			final long key = IntervalIndexer.positionToIndex( expectedMinsFirstTileArray[ i ], tiles[ 0 ].getSize() );
			final SubTilePair subTilePair = actualMinsFirstTile.get( key );
			Assert.assertArrayEquals( expectedMinsFirstTileArray[ i ], Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTilePair.getA() ) ) );
			Assert.assertArrayEquals( expectedMinsSecondTileArray[ i ], Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTilePair.getB() ) ) );
		}
	}

	@Test
	public void testOverlaps() throws PipelineExecutionException
	{
		final TileInfo fixedTile = new TileInfo( 2 );
		fixedTile.setIndex( 0 );
		fixedTile.setStagePosition( new double[] { 50, 50 } );
		fixedTile.setSize( new long[] { 20, 20 } );

		final TileInfo movingTile = new TileInfo( 2 );
		movingTile.setIndex( 1 );
		movingTile.setStagePosition( new double[] { 46, 32 } );
		movingTile.setSize( new long[] { 20, 20 } );

		final List< SubTile > subTiles = SubTileOperations.subdivideTiles( new TileInfo[] { fixedTile,  movingTile }, new int[] { 2, 2 } );
		Assert.assertEquals( 8, subTiles.size() );

		final SubTilePair subTilePair;
		{
			SubTile fixedSubTile = null;
			SubTile movingSubTile = null;
			for ( final SubTile subTile : subTiles )
			{
				if ( subTile.getFullTile().getIndex().intValue() == 0 )
				{
					// top-right fixed subtile
					if ( Math.round( subTile.min( 0 ) ) == 10 && Math.round( subTile.min( 1 ) ) == 0 )
					{
						if ( fixedSubTile != null )
							fail();
						fixedSubTile = subTile;
					}
				}
				else if ( subTile.getFullTile().getIndex().intValue() == 1 )
				{
					// bottom-right moving subtile
					if ( Math.round( subTile.min( 0 ) ) == 10 && Math.round( subTile.min( 1 ) ) == 10 )
					{
						if ( movingSubTile != null )
							fail();
						movingSubTile = subTile;
					}
				}
				else
				{
					fail();
				}
			}
			if ( fixedSubTile == null || movingSubTile == null )
				fail();
			subTilePair = new SubTilePair( fixedSubTile, movingSubTile );
		}
		Assert.assertArrayEquals( new long[] { 10, 0 }, Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTilePair.getA() ) ) );
		Assert.assertArrayEquals( new long[] { 10, 10 }, Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTilePair.getB() ) ) );

		final Pair< Interval, Interval > intervalsInGlobalSpace = new ValuePair<>(
				IntervalsHelper.translate(
						IntervalsHelper.roundRealInterval( subTilePair.getA() ),
						Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTilePair.getA().getFullTile().getStageInterval() ) )
					),
				IntervalsHelper.translate(
						IntervalsHelper.roundRealInterval( subTilePair.getB() ),
						Intervals.minAsLongArray( IntervalsHelper.roundRealInterval( subTilePair.getB().getFullTile().getStageInterval() ) )
					)
			);

		final Pair< Interval, Interval > intervalsInFixedSpace = SubTileOperations.globalToFixedSpace( intervalsInGlobalSpace );

		Assert.assertArrayEquals( new long[] { 60, 50 }, Intervals.minAsLongArray( IntervalsNullable.intersect( intervalsInGlobalSpace.getA(), intervalsInGlobalSpace.getB() ) ) );
		Assert.assertArrayEquals( new long[] { 65, 51 }, Intervals.maxAsLongArray( IntervalsNullable.intersect( intervalsInGlobalSpace.getA(), intervalsInGlobalSpace.getB() ) ) );

		Assert.assertArrayEquals( new long[] { 0, 0 }, Intervals.minAsLongArray( IntervalsNullable.intersect( intervalsInFixedSpace.getA(), intervalsInFixedSpace.getB() ) ) );
		Assert.assertArrayEquals( new long[] { 5, 1 }, Intervals.maxAsLongArray( IntervalsNullable.intersect( intervalsInFixedSpace.getA(), intervalsInFixedSpace.getB() ) ) );

		Assert.assertArrayEquals( new long[] { 0, 0 }, Intervals.minAsLongArray( intervalsInFixedSpace.getA() ) );
		Assert.assertArrayEquals( new long[] { -4, -8 }, Intervals.minAsLongArray( intervalsInFixedSpace.getB() ) );

		final double ellipseRadius = 2;
		final double[] offsetsMeanValues = Intervals.minAsDoubleArray( intervalsInFixedSpace.getB() );
		final double[][] offsetsCovarianceMatrix = new double[][] { new double[] { 1, 0, 0 }, new double[] { 0, 1, 0 }, new double[] { 0, 0, 1 } };
		final ErrorEllipse errorEllipse = new ErrorEllipse( ellipseRadius, offsetsMeanValues, offsetsCovarianceMatrix );

		final Pair< Interval, Interval > adjustedOverlaps = SubTileOperations.getAdjustedOverlapIntervals( intervalsInFixedSpace, errorEllipse );

		Assert.assertArrayEquals( new long[] { 0, 0 }, Intervals.minAsLongArray( adjustedOverlaps.getA() ) );
		Assert.assertArrayEquals( new long[] { 7, 3 }, Intervals.maxAsLongArray( adjustedOverlaps.getA() ) );

		Assert.assertArrayEquals( new long[] { 2, 6 }, Intervals.minAsLongArray( adjustedOverlaps.getB() ) );
		Assert.assertArrayEquals( new long[] { 9, 9 }, Intervals.maxAsLongArray( adjustedOverlaps.getB() ) );

		final Pair< Interval, Interval > adjustedOverlapsInFullTile = SubTileOperations.getOverlapsInFullTile( subTilePair, adjustedOverlaps );

		Assert.assertArrayEquals( new long[] { 10, 0 }, Intervals.minAsLongArray( adjustedOverlapsInFullTile.getA() ) );
		Assert.assertArrayEquals( new long[] { 17, 3 }, Intervals.maxAsLongArray( adjustedOverlapsInFullTile.getA() ) );

		Assert.assertArrayEquals( new long[] { 12, 16 }, Intervals.minAsLongArray( adjustedOverlapsInFullTile.getB() ) );
		Assert.assertArrayEquals( new long[] { 19, 19 }, Intervals.maxAsLongArray( adjustedOverlapsInFullTile.getB() ) );
	}
}
