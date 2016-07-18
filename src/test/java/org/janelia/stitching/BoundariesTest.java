package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

public class BoundariesTest {

	private int cnt;
	private Boundaries expected;
	
	private Boundaries create( final int[] min, final int[] max ) {
		assert min.length == max.length;
		final Boundaries b = new Boundaries( min.length );
		for ( int d = 0; d < b.getDimensionality(); d++ ) {
			b.setMin( d, min[ d ] );
			b.setMax( d, max[ d ] );
		}
		return b;
	}
	
	private boolean same( final Boundaries b1, final Boundaries b2 ) {
		if ( b1.getDimensionality() != b2.getDimensionality() )
			return false;
		for ( int d = 0; d < b1.getDimensionality(); d++ )
			if ( b1.getMin(d) != b2.getMin(d) || b1.getMax(d) != b2.getMax(d) )
				return false;
		return true;
	}
	
	@Test
	public void test1d() {
		final TileInfo[] tiles = new TileInfo[ 3 ];
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ] = new TileInfo( 1 );
		
		cnt = 0;
		tiles[ cnt ].setPosition( new float[] { -19.7f } );	tiles[ cnt++ ].setSize( new int[] { 5 } );
		tiles[ cnt ].setPosition( new float[] { 1.2f } ); 	tiles[ cnt++ ].setSize( new int[] { 10 } );
		tiles[ cnt ].setPosition( new float[] { 6.f } ); 	tiles[ cnt++ ].setSize( new int[] { 3 } );
		expected = create( new int[] { -20 }, new int[] { 12 } );
		Assert.assertTrue( same( expected, Utils.findBoundaries( tiles ) ) );
		
		cnt = 0;
		tiles[ cnt ].setPosition( new float[] { 6.f } ); 	tiles[ cnt++ ].setSize( new int[] { 3 } );
		tiles[ cnt ].setPosition( new float[] { 1.2f } ); 	tiles[ cnt++ ].setSize( new int[] { 10 } );
		tiles[ cnt ].setPosition( new float[] { -19.7f } ); tiles[ cnt++ ].setSize( new int[] { 5 } );
		expected = create( new int[] { -20 }, new int[] { 12 } );
		Assert.assertTrue( same( expected, Utils.findBoundaries( tiles ) ) );
		
		cnt = 0;
		tiles[ cnt ].setPosition( new float[] { 3.3f } ); 	tiles[ cnt++ ].setSize( new int[] { 1 } );
		tiles[ cnt ].setPosition( new float[] { 2.2f } ); 	tiles[ cnt++ ].setSize( new int[] { 3 } );
		tiles[ cnt ].setPosition( new float[] { 1.1f } ); 	tiles[ cnt++ ].setSize( new int[] { 5 } );
		expected = create( new int[] { 1 }, new int[] { 7 } );
		Assert.assertTrue( same( expected, Utils.findBoundaries( tiles ) ) );
	}
	
	@Test
	public void test2d() {
		final TileInfo[] tiles = new TileInfo[ 2 ];
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ] = new TileInfo( 2 );
		
		cnt = 0;
		tiles[ cnt ].setPosition( new float[] { -5.7f, 4.1f } );	tiles[ cnt++ ].setSize( new int[] { 8, 6 } );
		tiles[ cnt ].setPosition( new float[] { 1.2f, 1.8f } ); 	tiles[ cnt++ ].setSize( new int[] { 9, 4 } );
		expected = create( new int[] { -6, 1 }, new int[] { 11, 11 } );
		Assert.assertTrue( same( expected, Utils.findBoundaries( tiles ) ) );
	}
	
	@Test
	public void test3d() {
		final TileInfo t = new TileInfo( 3 );
		t.setPosition( new float[] { -5.7f, 4.1f, -8.2f } );	t.setSize( new int[] { 3, 4, 5 } );
		expected = create( new int[] { -6, 4, -9 }, new int[] { -2, 9, -3 } );
		Assert.assertTrue( same( expected, Utils.findBoundaries( new TileInfo[] { t } ) ) );
	}
}
