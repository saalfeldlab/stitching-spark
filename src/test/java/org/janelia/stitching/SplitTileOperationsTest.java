package org.janelia.stitching;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.util.Conversions;
import org.junit.Assert;
import org.junit.Test;

import mpicbg.imglib.custom.OffsetConverter;
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

	@Test
	public void testOverlaps() throws PipelineExecutionException
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

		final Map< Integer, Map< Integer, TilePair > > overlappingTileBoxesMap = new TreeMap<>();
		for ( final TilePair tileBoxPair : overlappingTileBoxes )
		{
			if ( !overlappingTileBoxesMap.containsKey( tileBoxPair.getA().getIndex() ) )
				overlappingTileBoxesMap.put( tileBoxPair.getA().getIndex(), new TreeMap<>() );
			overlappingTileBoxesMap.get( tileBoxPair.getA().getIndex() ).put( tileBoxPair.getB().getIndex(), tileBoxPair );
		}

		final TilePair tileBoxPair = overlappingTileBoxesMap.get( 4 ).get( 8 );

		// test overlaps between tile boxes
		final Interval[] overlaps = SplitTileOperations.getOverlapIntervals( tileBoxPair );
		Assert.assertArrayEquals( new long[] { 40, 10, 0 }, Intervals.minAsLongArray( overlaps[ 0 ] ) );
		Assert.assertArrayEquals( new long[] { 0, 0, 10 }, Intervals.minAsLongArray( overlaps[ 1 ] ) );
		final long[] overlapDimensions = new long[] { 10, 20, 25 };
		Assert.assertArrayEquals( overlapDimensions, Intervals.dimensionsAsLongArray( overlaps[ 0 ] ) );
		Assert.assertArrayEquals( overlapDimensions, Intervals.dimensionsAsLongArray( overlaps[ 1 ] ) );

		// test padded overlaps between tile boxes
		final Interval[] paddedOverlaps = SplitTileOperations.getPaddedOverlapIntervals( tileBoxPair, new long[] { 16, 16, 16 } );
		Assert.assertArrayEquals( new long[] { 24, 2, 0 }, Intervals.minAsLongArray( paddedOverlaps[ 0 ] ) );
		Assert.assertArrayEquals( new long[] { 0, 0, 2 }, Intervals.minAsLongArray( paddedOverlaps[ 1 ] ) );
		final long[] paddedOverlapDimensions = new long[] { 26, 36, 41 };
		Assert.assertArrayEquals( paddedOverlapDimensions, Intervals.dimensionsAsLongArray( paddedOverlaps[ 0 ] ) );
		Assert.assertArrayEquals( paddedOverlapDimensions, Intervals.dimensionsAsLongArray( paddedOverlaps[ 1 ] ) );

		// test adjusted overlaps with respect to the search radius
		final double[] offsetsMeanValues = new double[] { 40, 10, -10 };
		final double[][] offsetsCovarianceMatrix = new double[][] { new double[] { 1, 0, 0 }, new double[] { 0, 1, 0 }, new double[] { 0, 0, 1 } };
		final double sphereRadiusPixels = 9;
		final SearchRadius searchRadius = new SearchRadius( sphereRadiusPixels, offsetsMeanValues, offsetsCovarianceMatrix );
		final Interval[] adjustedOverlaps = SplitTileOperations.getAdjustedOverlapIntervals( tileBoxPair, searchRadius );
		Assert.assertArrayEquals( new long[] { 31, 1, 0 }, Intervals.minAsLongArray( adjustedOverlaps[ 0 ] ) );
		Assert.assertArrayEquals( new long[] { 0, 0, 1 }, Intervals.minAsLongArray( adjustedOverlaps[ 1 ] ) );
		final long[] adjustedOverlapDimensions = new long[] { 19, 59, 69 };
		Assert.assertArrayEquals( adjustedOverlapDimensions, Intervals.dimensionsAsLongArray( adjustedOverlaps[ 0 ] ) );
		Assert.assertArrayEquals( adjustedOverlapDimensions, Intervals.dimensionsAsLongArray( adjustedOverlaps[ 1 ] ) );

		// test offset converter
		final OffsetConverter offsetConverter = SplitTileOperations.getOffsetConverter( tileBoxPair, adjustedOverlaps );
		final int[] roiOffset = new int[] { 3, 4, 5 };
		final long[] originalTileOffset = offsetConverter.roiOffsetToTileOffset( roiOffset );
		Assert.assertArrayEquals( new long[] { 34, 5, 4 }, originalTileOffset );
		Assert.assertArrayEquals( new double[] { 34, 5, 4 }, offsetConverter.roiOffsetToTileOffset( Conversions.toDoubleArray( roiOffset ) ), EPSILON ); // test that double implementation does the same
		final double[] movingTileBoxPosition = offsetConverter.tileOffsetToGlobalPosition( originalTileOffset );
		Assert.assertArrayEquals( new double[] { 34, 5, 4 }, movingTileBoxPosition, EPSILON );

		// test point match
		final PointPair pointPair = SplitTileOperations.createPointPair( tileBoxPair, Conversions.toDoubleArray( originalTileOffset ) );
		Assert.assertArrayEquals( new double[] { 37.5, 15, 17.5 }, pointPair.getA().getL(), EPSILON );
		Assert.assertArrayEquals( new double[] { 3.5, 10, 13.5 }, pointPair.getB().getL(), EPSILON );
	}
}
