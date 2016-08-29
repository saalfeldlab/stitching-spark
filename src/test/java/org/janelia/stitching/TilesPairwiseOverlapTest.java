package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Igor Pisarev
 */

public class TilesPairwiseOverlapTest {

	private void testGlobalRegion( final Boundaries r1, final Boundaries r2 )
	{
		Assert.assertArrayEquals( r1.getMin(), r2.getMin() );
		Assert.assertArrayEquals( r1.getMax(), r2.getMax() );
	}

	@Test
	public void test1d() {
		final TileInfo t1 = new TileInfo(), t2 = new TileInfo();

		// overlap
		t1.setPosition( new double[] { 5. } ); 	t1.setSize( new long[] { 10 } );
		t2.setPosition( new double[] { -5. } ); t2.setSize( new long[] { 12 } );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t1, t2 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t1, t2 ).validate() );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t2, t1 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t2, t1 ).validate() );
		testGlobalRegion( TileOperations.getOverlappingRegionGlobal( t1, t2 ), TileOperations.getOverlappingRegionGlobal( t2, t1 ) );

		// one inside another
		t1.setPosition( new double[] { 0. } ); t1.setSize( new long[] { 5 } );
		t2.setPosition( new double[] { 1. } ); t2.setSize( new long[] { 2 } );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t1, t2 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t1, t2 ).validate() );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t2, t1 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t2, t1 ).validate() );
		testGlobalRegion( TileOperations.getOverlappingRegionGlobal( t1, t2 ), TileOperations.getOverlappingRegionGlobal( t2, t1 ) );

		// intersection (don't treat it as an overlap)
		t1.setPosition( new double[] { 2. } ); t1.setSize( new long[] { 3 } );
		t2.setPosition( new double[] { 5. } ); t2.setSize( new long[] { 2 } );
		Assert.assertNull( TileOperations.getOverlappingRegion( t1, t2 ) );
		Assert.assertNull( TileOperations.getOverlappingRegion( t2, t1 ) );

		// no intersection
		t1.setPosition( new double[] { 100. } ); t1.setSize( new long[] { 1 } );
		t2.setPosition( new double[] { 500. } ); t2.setSize( new long[] { 1 } );
		Assert.assertNull( TileOperations.getOverlappingRegion( t1, t2 ) );
		Assert.assertNull( TileOperations.getOverlappingRegion( t2, t1 ) );
	}

	@Test
	public void test2d() {
		final TileInfo t1 = new TileInfo(), t2 = new TileInfo();

		// overlap
		t1.setPosition( new double[] { 5., 2.5 } );  t1.setSize( new long[] { 10, 10 } );
		t2.setPosition( new double[] { -5., 10. } ); t2.setSize( new long[] { 20, 20 } );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t1, t2 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t1, t2 ).validate() );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t2, t1 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t2, t1 ).validate() );
		testGlobalRegion( TileOperations.getOverlappingRegionGlobal( t1, t2 ), TileOperations.getOverlappingRegionGlobal( t2, t1 ) );

		// one inside another
		t1.setPosition( new double[] { 2., 3. } ); t1.setSize( new long[] { 50, 30 } );
		t2.setPosition( new double[] { 8., 6. } ); t2.setSize( new long[] { 2, 5 } );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t1, t2 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t1, t2 ).validate() );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t2, t1 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t2, t1 ).validate() );
		testGlobalRegion( TileOperations.getOverlappingRegionGlobal( t1, t2 ), TileOperations.getOverlappingRegionGlobal( t2, t1 ) );

		// intersection (segment)
		t1.setPosition( new double[] { 0., 0. } ); t1.setSize( new long[] { 3, 4 } );
		t2.setPosition( new double[] { 3., 2. } ); t2.setSize( new long[] { 2, 5 } );
		Assert.assertNull( TileOperations.getOverlappingRegion( t1, t2 ) );
		Assert.assertNull( TileOperations.getOverlappingRegion( t2, t1 ) );

		// intersection (point)
		t1.setPosition( new double[] { 0., 0. } ); t1.setSize( new long[] { 3, 4 } );
		t2.setPosition( new double[] { 3., 4. } ); t2.setSize( new long[] { 2, 5 } );
		Assert.assertNull( TileOperations.getOverlappingRegion( t1, t2 ) );
		Assert.assertNull( TileOperations.getOverlappingRegion( t2, t1 ) );

		// no intersection
		t1.setPosition( new double[] { 0., 0. } );  t1.setSize( new long[] { 5, 6 } );
		t2.setPosition( new double[] { 2., 10. } ); t2.setSize( new long[] { 3, 4 } );
		Assert.assertNull( TileOperations.getOverlappingRegion( t1, t2 ) );
		Assert.assertNull( TileOperations.getOverlappingRegion( t2, t1 ) );
	}

	@Test
	public void test3d() {
		final TileInfo t1 = new TileInfo(), t2 = new TileInfo();

		// overlap
		t1.setPosition( new double[] { 0., 0., 0. } ); t1.setSize( new long[] { 3, 4, 5 } );
		t2.setPosition( new double[] { 2., 3., 4. } ); t2.setSize( new long[] { 20, 20, 20 } );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t1, t2 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t1, t2 ).validate() );
		Assert.assertNotNull( TileOperations.getOverlappingRegion( t2, t1 ) );	Assert.assertTrue( TileOperations.getOverlappingRegion( t2, t1 ).validate() );
		testGlobalRegion( TileOperations.getOverlappingRegionGlobal( t1, t2 ), TileOperations.getOverlappingRegionGlobal( t2, t1 ) );

		// intersection
		t1.setPosition( new double[] { 0., 0., 0. } ); t1.setSize( new long[] { 3, 4, 5 } );
		t2.setPosition( new double[] { 3., 4., 5. } ); t2.setSize( new long[] { 1, 2, 3 } );
		Assert.assertNull( TileOperations.getOverlappingRegion( t1, t2 ) );
		Assert.assertNull( TileOperations.getOverlappingRegion( t2, t1 ) );

		// no intersection
		t1.setPosition( new double[] { 0., 0., 0. } );  t1.setSize( new long[] { 3, 4, 5 } );
		t2.setPosition( new double[] { 1., 2., 10. } ); t2.setSize( new long[] { 3, 4, 5 } );
		Assert.assertNull( TileOperations.getOverlappingRegion( t1, t2 ) );
		Assert.assertNull( TileOperations.getOverlappingRegion( t2, t1 ) );
	}
}
