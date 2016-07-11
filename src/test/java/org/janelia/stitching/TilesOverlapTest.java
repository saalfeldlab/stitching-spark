package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

public class TilesOverlapTest {

	@Test
	public void test1dOverlap() {
		
		final TileInfo t1 = new TileInfo(), t2 = new TileInfo();
		
		// overlap
		t1.setPosition( new float[] { 5.f } ); 	t1.setSize( new int[] { 10 } );
		t2.setPosition( new float[] { -5.f } ); t2.setSize( new int[] { 12 } );
		Assert.assertTrue( Utils.overlap( t1, t2 ) );
		Assert.assertTrue( Utils.overlap( t2, t1 ) );
		
		// intersection
		t1.setPosition( new float[] { 2.f } ); t1.setSize( new int[] { 3 } );
		t2.setPosition( new float[] { 5.f } ); t2.setSize( new int[] { 2 } );
		Assert.assertTrue( Utils.overlap( t1, t2 ) );
		Assert.assertTrue( Utils.overlap( t2, t1 ) );
		
		// one inside another
		t1.setPosition( new float[] { 0.f } ); t1.setSize( new int[] { 5 } );
		t2.setPosition( new float[] { 1.f } ); t2.setSize( new int[] { 2 } );
		Assert.assertTrue( Utils.overlap( t1, t2 ) );
		Assert.assertTrue( Utils.overlap( t2, t1 ) );
		
		// no intersection
		t1.setPosition( new float[] { 100.f } ); t1.setSize( new int[] { 1 } );
		t2.setPosition( new float[] { 500.f } ); t2.setSize( new int[] { 1 } );
		Assert.assertFalse( Utils.overlap( t1, t2 ) );
		Assert.assertFalse( Utils.overlap( t2, t1 ) );
	}
	
	@Test
	public void test2dOverlap() {
		
		final TileInfo t1 = new TileInfo(), t2 = new TileInfo();
		
		// overlap
		t1.setPosition( new float[] { 5.f, 2.5f } );  t1.setSize( new int[] { 10, 10 } );
		t2.setPosition( new float[] { -5.f, 10.f } ); t2.setSize( new int[] { 20, 20 } );
		Assert.assertTrue( Utils.overlap( t1, t2 ) );
		Assert.assertTrue( Utils.overlap( t2, t1 ) );
		
		// intersection (segment)
		t1.setPosition( new float[] { 0.f, 0.f } ); t1.setSize( new int[] { 3, 4 } );
		t2.setPosition( new float[] { 3.f, 2.f } ); t2.setSize( new int[] { 2, 5 } );
		Assert.assertTrue( Utils.overlap( t1, t2 ) );
		Assert.assertTrue( Utils.overlap( t2, t1 ) );
		
		// intersection (point)
		t1.setPosition( new float[] { 0.f, 0.f } ); t1.setSize( new int[] { 3, 4 } );
		t2.setPosition( new float[] { 3.f, 4.f } ); t2.setSize( new int[] { 2, 5 } );
		Assert.assertTrue( Utils.overlap( t1, t2 ) );
		Assert.assertTrue( Utils.overlap( t2, t1 ) );
		
		// one inside another
		t1.setPosition( new float[] { 2.f, 3.f } ); t1.setSize( new int[] { 50, 30 } );
		t2.setPosition( new float[] { 8.f, 6.f } ); t2.setSize( new int[] { 2, 5 } );
		Assert.assertTrue( Utils.overlap( t1, t2 ) );
		Assert.assertTrue( Utils.overlap( t2, t1 ) );
		
		// no intersection
		t1.setPosition( new float[] { 0.f, 0.f } );  t1.setSize( new int[] { 5, 6 } );
		t2.setPosition( new float[] { 2.f, 10.f } ); t2.setSize( new int[] { 3, 4 } );
		Assert.assertFalse( Utils.overlap( t1, t2 ) );
		Assert.assertFalse( Utils.overlap( t2, t1 ) );
	}

	@Test
	public void test3dOverlap() {
		
		final TileInfo t1 = new TileInfo(), t2 = new TileInfo();
		
		// overlap
		t1.setPosition( new float[] { 0.f, 0.f, 0.f } ); t1.setSize( new int[] { 3, 4, 5 } );
		t2.setPosition( new float[] { 2.f, 3.f, 4.f } ); t2.setSize( new int[] { 20, 20, 20 } );
		Assert.assertTrue( Utils.overlap( t1, t2 ) );
		Assert.assertTrue( Utils.overlap( t2, t1 ) );
		
		// intersection
		t1.setPosition( new float[] { 0.f, 0.f, 0.f } ); t1.setSize( new int[] { 3, 4, 5 } );
		t2.setPosition( new float[] { 3.f, 4.f, 5.f } ); t2.setSize( new int[] { 1, 2, 3 } );
		Assert.assertTrue( Utils.overlap( t1, t2 ) );
		Assert.assertTrue( Utils.overlap( t2, t1 ) );
		
		// no intersection
		t1.setPosition( new float[] { 0.f, 0.f, 0.f } );  t1.setSize( new int[] { 3, 4, 5 } );
		t2.setPosition( new float[] { 1.f, 2.f, 10.f } ); t2.setSize( new int[] { 3, 4, 5 } );
		Assert.assertFalse( Utils.overlap( t1, t2 ) );
		Assert.assertFalse( Utils.overlap( t2, t1 ) );
	}
	
	@Test
	public void testOverlappingPairs() {
		
		final TileInfo[] tiles = new TileInfo[ 5 ];
	
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ] = new TileInfo();
		
		tiles[ 0 ].setPosition( new float[] { 0.f, 0.f, 0.f } ); 												 tiles[ 0 ].setSize( new int[] { 991, 992, 880 } );
		tiles[ 1 ].setPosition( new float[] { 716.932762003862f, -694.0887500300357f, -77.41783189603937f } );   tiles[ 1 ].setSize( new int[] { 991, 992, 953 } );
		tiles[ 2 ].setPosition( new float[] { 190.0026915705376f, -765.0362103552288f, -79.91642981085911f } );  tiles[ 2 ].setSize( new int[] { 991, 992, 953 } );
		tiles[ 3 ].setPosition( new float[] { 644.9707923831013f, -1160.9781698781098f, -79.17565799404196f } ); tiles[ 3 ].setSize( new int[] { 991, 992, 953 } );
		tiles[ 4 ].setPosition( new float[] { 1419.1341448553956f, -1337.414351167819f, -76.84995500091385f } ); tiles[ 4 ].setSize( new int[] { 991, 992, 953 } );
		
		Assert.assertEquals( Utils.findOverlappingTiles( tiles ).size(), 7 );
	}
}
