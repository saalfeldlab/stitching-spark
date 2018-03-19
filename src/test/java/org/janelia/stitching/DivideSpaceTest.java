package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import net.imglib2.FinalDimensions;

/**
 * @author Igor Pisarev
 */

public class DivideSpaceTest {

	@Test
	public void test() {
		final Boundaries space = new Boundaries( 1 );
		space.setMin( 0, 10 );	space.setMax( 0, 40 );
		final ArrayList< TileInfo > res = TileOperations.divideSpaceBySize( space, 14 );
		Assert.assertEquals( 3, res.size() );

		for ( int i = 0; i < res.size(); i++ )
			Assert.assertEquals( i, (int)res.get(i).getIndex() );

		Assert.assertEquals( 10, res.get( 0 ).getPosition( 0 ), 0.f );	Assert.assertEquals( 14, res.get( 0 ).getSize( 0 ) );
		Assert.assertEquals( 24, res.get( 1 ).getPosition( 0 ), 0.f );	Assert.assertEquals( 14, res.get( 1 ).getSize( 0 ) );
		Assert.assertEquals( 38, res.get( 2 ).getPosition( 0 ), 0.f );	Assert.assertEquals( 3,  res.get( 2 ).getSize( 0 ) );
	}

	@Test
	public void testIgnoreSmaller() {
		final Boundaries space = new Boundaries( 1 );
		space.setMin( 0, 10 );	space.setMax( 0, 40 );
		final ArrayList< TileInfo > res = TileOperations.divideSpaceIgnoreSmaller( space, new FinalDimensions( 14 ) );
		Assert.assertEquals( 2, res.size() );

		for ( int i = 0; i < res.size(); i++ )
			Assert.assertEquals( i, (int)res.get(i).getIndex() );

		Assert.assertEquals( 10, res.get( 0 ).getPosition( 0 ), 0.f );	Assert.assertEquals( 14, res.get( 0 ).getSize( 0 ) );
		Assert.assertEquals( 24, res.get( 1 ).getPosition( 0 ), 0.f );	Assert.assertEquals( 14, res.get( 1 ).getSize( 0 ) );
	}

	@Test
	public void randomTestBySize()
	{
		final Random rnd = new Random();

		final Boundaries space = new Boundaries( rnd.nextInt(5) + 1 );
		for ( int d = 0; d < space.numDimensions(); d++ ) {
			space.setMin( d, rnd.nextInt( 1000 ) - 500 );
			space.setMax( d, rnd.nextInt( 1000 ) + space.min( d ) );
		}
		Assert.assertTrue( space.validate() );

		final int subregionSize = rnd.nextInt( 100 ) + 50;
		int expectedCount = 1;
		for ( int d = 0; d < space.numDimensions(); d++ )
			expectedCount *= ( int ) Math.ceil( ( double ) space.dimension( d ) / subregionSize );

		final int actualCount = TileOperations.divideSpaceBySize( space, subregionSize).size();

		System.out.println( "[DivideSpaceTest] Random test by size:" );
		System.out.println( "Dim = " + space.numDimensions() );
		System.out.println( "Subregion size = " + subregionSize );
		System.out.println( "Expected subregions count = " + expectedCount );
		System.out.println( "Actual subregions count = " + actualCount );

		Assert.assertEquals( expectedCount, actualCount );
	}

	@Test
	public void randomTestByCount()
	{
		final Random rnd = new Random();

		final Boundaries space = new Boundaries( rnd.nextInt(5) + 1 );
		for ( int d = 0; d < space.numDimensions(); d++ ) {
			space.setMin( d, rnd.nextInt( 1000 ) - 500 );
			space.setMax( d, rnd.nextInt( 1000 ) + space.min( d ) );
		}
		Assert.assertTrue( space.validate() );

		int subregionCountPerDim = rnd.nextInt( 15 ) + 1;
		for ( int d = 0; d < space.numDimensions(); d++ )
			subregionCountPerDim = ( int ) Math.min( space.dimension( d ), subregionCountPerDim );

		final int expectedCount = ( int ) Math.pow( subregionCountPerDim, space.numDimensions() );

		final int actualCount = TileOperations.divideSpaceByCount( space, subregionCountPerDim ).size();

		System.out.println( "[DivideSpaceTest] Random test by count:" );
		System.out.println( "Dim = " + space.numDimensions() );
		System.out.println( "Subregion count per dimension = " + subregionCountPerDim );
		System.out.println( "Expected subregions count = " + expectedCount );
		System.out.println( "Actual subregions count = " + actualCount );

		Assert.assertEquals( expectedCount, actualCount );
	}
}
