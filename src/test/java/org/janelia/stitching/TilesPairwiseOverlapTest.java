package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

public class TilesPairwiseOverlapTest {

	@Test
	public void test1d() {
		
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
	public void test2d() {
		
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
	public void test3d() {
		
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
}
