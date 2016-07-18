package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class DivideSpaceTest {

	@Test
	public void test1d() {
		
		final Boundaries space = new Boundaries( 1 );
		space.setMin( 0, 10 );	space.setMax( 0, 40 );
		final ArrayList< TileInfo > res = Utils.divideSpace( space, 14 );
		Assert.assertEquals( 3, res.size() );
		
		Assert.assertEquals( 10.f, res.get( 0 ).getPosition( 0 ), 0.f );	Assert.assertEquals( 14, res.get( 0 ).getSize( 0 ) );
		Assert.assertEquals( 24.f, res.get( 1 ).getPosition( 0 ), 0.f );	Assert.assertEquals( 14, res.get( 1 ).getSize( 0 ) );
		Assert.assertEquals( 38.f, res.get( 2 ).getPosition( 0 ), 0.f );	Assert.assertEquals( 2, res.get( 2 ).getSize( 0 ) );
	}
	
	@Test
	public void randomTestSubregionsCount() {

		final Random rnd = new Random();
		final Boundaries space = new Boundaries( rnd.nextInt(5) + 1 );
		for ( int d = 0; d < space.getDimensionality(); d++ ) {
			space.setMin( d, rnd.nextInt( 2000 ) - 1000 );
			space.setMax( d, rnd.nextInt( 2000 ) + space.getMin( d ) );
		}
		Assert.assertTrue( space.validate() );
		
		final int subregionSize = rnd.nextInt( 100 ) + 50;
		int expectedCount = 1;
		for ( int d = 0; d < space.getDimensionality(); d++ )
			expectedCount *= (int)Math.ceil( (float)( space.getMax(d) - space.getMin(d) ) / subregionSize );
		
		System.out.println( "[DivideSpaceTest] Random test for subregions count:" );
		System.out.println( "Dim = " + space.getDimensionality() );
		System.out.println( "Subregion size = " + subregionSize );
		System.out.println( "Expected subregions count = " + expectedCount );
		
		Assert.assertEquals( expectedCount, Utils.divideSpace( space, subregionSize).size() );
	}
}
