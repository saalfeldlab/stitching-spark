package org.janelia.stitching;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.janelia.util.ComparableTuple;
import org.junit.Assert;
import org.junit.Test;

public class CheckSubTileMatchesCoplanarityTest
{
	@Test
	public void test()
	{
		final TileInfo tile = new TileInfo( 3 );
		tile.setSize( new long[] { 100, 200, 300 } );

		final List< SubTile > subTiles = SubTileOperations.subdivideTiles( new TileInfo[] { tile }, new int[] { 2, 2, 2 } );
		Assert.assertEquals( 8, subTiles.size() );

		final int numConfigs = 1 << subTiles.size();
		Assert.assertEquals( 256, numConfigs );

		int numCollinearConfigs = 0;
		int numConfigsWithFewerThan3Points = 0, numConfigsWith3Points = 0;
		int numCoplanarConfigsWith3Points = 0, numCoplanarConfigsWith4Points = 0, numCoplanarConfigsWithMoreThan4Points = 0;

		for ( int mask = 0; mask < numConfigs; ++mask )
		{
			final List< SubTile > subTilesConfig = new ArrayList<>();
			for ( int i = 0; i < subTiles.size(); ++i )
				if ( ( mask & ( 1 << i ) ) > 0 )
					subTilesConfig.add( subTiles.get( i ) );

			final List< SubTile > subTilesConfigWithDuplicates = new ArrayList<>();
			for ( int k = 0; k < 3; ++k )
				subTilesConfigWithDuplicates.addAll( subTilesConfig );

			final TreeMap< ComparableTuple< Long >, Integer > groupedSubTiles = CheckSubTileMatchesCoplanarity.groupSubTilesByTheirLocalPosition( subTilesConfigWithDuplicates );
			Assert.assertEquals( subTilesConfig.size(), groupedSubTiles.size() );

			if ( subTilesConfig.size() < 3 )
				++numConfigsWithFewerThan3Points;
			else if ( subTilesConfig.size() == 3 )
				++numConfigsWith3Points;

			if ( CheckSubTileMatchesCoplanarity.isCollinear( groupedSubTiles ) )
			{
				++numCollinearConfigs;
			}
			else if ( CheckSubTileMatchesCoplanarity.isCoplanar( groupedSubTiles ) )
			{
				if ( groupedSubTiles.size() == 3 )
					++numCoplanarConfigsWith3Points;
				else if ( groupedSubTiles.size() == 4 )
					++numCoplanarConfigsWith4Points;
				else if ( groupedSubTiles.size() > 4 )
					++numCoplanarConfigsWithMoreThan4Points;
			}
		}

		Assert.assertEquals( numConfigsWithFewerThan3Points, numCollinearConfigs );
		Assert.assertEquals( numConfigsWith3Points, numCoplanarConfigsWith3Points );
		Assert.assertEquals( 12, numCoplanarConfigsWith4Points ); // 6 orthogonal configs + 6 tilted configs
		Assert.assertEquals( 0, numCoplanarConfigsWithMoreThan4Points );
	}
}
