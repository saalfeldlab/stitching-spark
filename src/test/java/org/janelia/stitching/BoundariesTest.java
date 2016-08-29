package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Igor Pisarev
 */

public class BoundariesTest {

	private Boundaries expected;

	private Boundaries create( final long[] min, final long[] max ) {
		assert min.length == max.length;
		final Boundaries b = new Boundaries( min.length );
		for ( int d = 0; d < b.numDimensions(); d++ ) {
			b.setMin( d, min[ d ] );
			b.setMax( d, max[ d ] );
		}
		return b;
	}

	private boolean same( final Boundaries b1, final Boundaries b2 ) {
		if ( b1.numDimensions() != b2.numDimensions() )
			return false;
		for ( int d = 0; d < b1.numDimensions(); d++ )
			if ( b1.min(d) != b2.min(d) || b1.max(d) != b2.max(d) )
				return false;
		return true;
	}

	@Test
	public void test1d() {
		final TileInfo[] tiles = new TileInfo[ 3 ];
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ] = new TileInfo( 1 );

		tiles[ 0 ].setPosition( new double[] { -19.7 } );	tiles[ 0 ].setSize( new long[] { 5 } );
		tiles[ 1 ].setPosition( new double[] { 1.2 } ); 	tiles[ 1 ].setSize( new long[] { 10 } );
		tiles[ 2 ].setPosition( new double[] { 6. } ); 		tiles[ 2 ].setSize( new long[] { 3 } );
		expected = create( new long[] { -20 }, new long[] { 10 } );
		Assert.assertTrue( same( expected, TileOperations.getCollectionBoundaries( tiles ) ) );

		tiles[ 0 ].setPosition( new double[] { 6. } ); 		tiles[ 0 ].setSize( new long[] { 3 } );
		tiles[ 1 ].setPosition( new double[] { 1.2 } ); 	tiles[ 1 ].setSize( new long[] { 10 } );
		tiles[ 2 ].setPosition( new double[] { -19.7 } ); 	tiles[ 2 ].setSize( new long[] { 5 } );
		expected = create( new long[] { -20 }, new long[] { 10 } );
		Assert.assertTrue( same( expected, TileOperations.getCollectionBoundaries( tiles ) ) );

		tiles[ 0 ].setPosition( new double[] { 3.3 } ); 	tiles[ 0 ].setSize( new long[] { 1 } );
		tiles[ 1 ].setPosition( new double[] { 2.2 } ); 	tiles[ 1 ].setSize( new long[] { 3 } );
		tiles[ 2 ].setPosition( new double[] { 1.1 } ); 	tiles[ 2 ].setSize( new long[] { 5 } );
		expected = create( new long[] { 1 }, new long[] { 5 } );
		Assert.assertTrue( same( expected, TileOperations.getCollectionBoundaries( tiles ) ) );
	}

	@Test
	public void test2d() {
		final TileInfo[] tiles = new TileInfo[ 2 ];
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ] = new TileInfo( 2 );

		tiles[ 0 ].setPosition( new double[] { -5.7, 4.1 } );
		tiles[ 0 ].setSize( new long[] { 8, 6 } );

		tiles[ 1 ].setPosition( new double[] { 1.2, 1.8 } );
		tiles[ 1 ].setSize( new long[] { 9, 4 } );

		expected = create( new long[] { -6, 2 }, new long[] { 9, 9 } );
		Assert.assertTrue( same( expected, TileOperations.getCollectionBoundaries( tiles ) ) );
	}

	@Test
	public void test3d() {
		final TileInfo t = new TileInfo( 3 );
		t.setPosition( new double[] { -5.7, 4.1, -8.2 } );
		t.setSize( new long[] { 3, 4, 5 } );

		expected = create( new long[] { -6, 4, -8 }, new long[] { -4, 7, -4 } );
		Assert.assertTrue( same( expected, TileOperations.getCollectionBoundaries( new TileInfo[] { t } ) ) );
	}
}
