package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.FinalInterval;
import net.imglib2.util.Intervals;

/**
 * @author Igor Pisarev
 */

public class CollectionBoundariesTest {

	@Test
	public void test1d() {
		final TileInfo[] tiles = new TileInfo[ 3 ];
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ] = new TileInfo( 1 );

		tiles[ 0 ].setStagePosition( new double[] { -19.7 } );	tiles[ 0 ].setSize( new long[] { 5 } );
		tiles[ 1 ].setStagePosition( new double[] { 1.2 } ); 	tiles[ 1 ].setSize( new long[] { 10 } );
		tiles[ 2 ].setStagePosition( new double[] { 6. } ); 	tiles[ 2 ].setSize( new long[] { 3 } );
		Assert.assertTrue( Intervals.equals(
				new FinalInterval( new long[] { -20 }, new long[] { 10 } ),
				TileOperations.getCollectionBoundaries( tiles ) )
			);

		tiles[ 0 ].setStagePosition( new double[] { 6. } ); 	tiles[ 0 ].setSize( new long[] { 3 } );
		tiles[ 1 ].setStagePosition( new double[] { 1.2 } ); 	tiles[ 1 ].setSize( new long[] { 10 } );
		tiles[ 2 ].setStagePosition( new double[] { -19.7 } ); 	tiles[ 2 ].setSize( new long[] { 5 } );
		Assert.assertTrue( Intervals.equals(
				new FinalInterval( new long[] { -20 }, new long[] { 10 } ),
				TileOperations.getCollectionBoundaries( tiles ) )
			);

		tiles[ 0 ].setStagePosition( new double[] { 3.3 } ); 	tiles[ 0 ].setSize( new long[] { 1 } );
		tiles[ 1 ].setStagePosition( new double[] { 2.2 } ); 	tiles[ 1 ].setSize( new long[] { 3 } );
		tiles[ 2 ].setStagePosition( new double[] { 1.1 } ); 	tiles[ 2 ].setSize( new long[] { 5 } );
		Assert.assertTrue( Intervals.equals(
				new FinalInterval( new long[] { 1 }, new long[] { 5 } ),
				TileOperations.getCollectionBoundaries( tiles ) )
			);
	}

	@Test
	public void test2d() {
		final TileInfo[] tiles = new TileInfo[ 2 ];
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ] = new TileInfo( 2 );

		tiles[ 0 ].setStagePosition( new double[] { -5.7, 4.1 } );
		tiles[ 0 ].setSize( new long[] { 8, 6 } );

		tiles[ 1 ].setStagePosition( new double[] { 1.2, 1.8 } );
		tiles[ 1 ].setSize( new long[] { 9, 4 } );

		Assert.assertTrue( Intervals.equals(
				new FinalInterval( new long[] { -6, 2 }, new long[] { 9, 9 } ),
				TileOperations.getCollectionBoundaries( tiles ) )
			);
	}

	@Test
	public void test3d() {
		final TileInfo t = new TileInfo( 3 );
		t.setStagePosition( new double[] { -5.7, 4.1, -8.2 } );
		t.setSize( new long[] { 3, 4, 5 } );

		Assert.assertTrue( Intervals.equals(
				new FinalInterval( new long[] { -6, 4, -8 }, new long[] { -4, 7, -4 } ),
				TileOperations.getCollectionBoundaries( new TileInfo[] { t } ) )
			);
	}
}
