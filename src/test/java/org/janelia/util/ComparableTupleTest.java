package org.janelia.util;

import org.janelia.util.ComparableTuple;
import org.junit.Assert;
import org.junit.Test;

public class ComparableTupleTest
{
	@Test
	public void testIdentity()
	{
		final ComparableTuple< Integer > a = new ComparableTuple<>( 5 );
		Assert.assertTrue( a.compareTo( a ) == 0 );
	}

	@Test
	public void test1()
	{
		final ComparableTuple< Integer > a = new ComparableTuple<>( 5 );
		final ComparableTuple< Integer > b = new ComparableTuple<>( 8 );
		Assert.assertTrue( a.compareTo( b ) < 0 );
		Assert.assertTrue( b.compareTo( a ) > 0 );
	}

	@Test
	public void test2()
	{
		final ComparableTuple< Integer > a = new ComparableTuple<>( 5, 2, 4 );
		final ComparableTuple< Integer > b = new ComparableTuple<>( 5, 5, 5 );
		Assert.assertTrue( a.compareTo( b ) < 0 );
		Assert.assertTrue( b.compareTo( a ) > 0 );
	}

	@Test
	public void test3()
	{
		final ComparableTuple< Integer > a = new ComparableTuple<>( 5, 2, 4, 7, 1 );
		final ComparableTuple< Integer > b = new ComparableTuple<>( 5, 2, 4, 7, 1 );
		Assert.assertTrue( a.compareTo( b ) == 0 );
	}

	@Test
	public void testTransitivity1()
	{
		final ComparableTuple< Integer > a = new ComparableTuple<>( 5, 2, 8, 1, 2 );
		final ComparableTuple< Integer > b = new ComparableTuple<>( 5, 2, 4, 7, 1 );
		final ComparableTuple< Integer > c = new ComparableTuple<>( 5, 2, 4, 6, 9 );

		Assert.assertTrue( a.compareTo( b ) > 0 && b.compareTo( c ) > 0 && a.compareTo( c ) > 0 );
	}

	@Test
	public void testTransitivity2()
	{
		final ComparableTuple< Integer > a = new ComparableTuple<>( 5, 2, 8, 1, 2 );
		final ComparableTuple< Integer > b = new ComparableTuple<>( 5, 2, 8, 1, 2 );
		final ComparableTuple< Integer > c = new ComparableTuple<>( 4, 6, 9, 1, 3 );

		Assert.assertTrue( a.compareTo( b ) == 0 && a.compareTo( c ) > 0 && b.compareTo( c ) > 0 );
	}
}
