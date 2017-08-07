package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.janelia.util.Conversions;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.FinalInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.algorithm.neighborhood.RectangleNeighborhoodUnsafe;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class RectangleNeighborhoodTest
{
	@Test
	public void test()
	{
		final long[] cellGridSize = new long[] { 4, 4, 8 };
		final int[] arr = new int[ ( int ) Intervals.numElements( cellGridSize ) ];
		for ( int i = 0; i < arr.length; ++i )
			arr[ i ] = i;

		final RandomAccessibleInterval< IntType > cellsIndexMap = ArrayImgs.ints( arr, cellGridSize );

		final RectangleShape.NeighborhoodsAccessible< IntType > neighborhood = new RectangleShape.NeighborhoodsAccessible<>(
				cellsIndexMap,
				new FinalInterval( 3, 3, 3 ),
				RectangleNeighborhoodUnsafe.< IntType >factory() );

		final RandomAccess< Neighborhood< IntType > > block = neighborhood.randomAccess();
		block.setPosition( new int[] { 0, 0, 0 } );
		final TreeSet< Integer > visited = new TreeSet<>();
		for ( final IntType item : block.get() )
			visited.add( item.get() );

		Assert.assertArrayEquals(
				Conversions.toBoxedArray( new int[] { 0,1,2, 4,5,6, 8,9,10,   16,17,18, 20,21,22, 24,25,26,   32,33,34, 36,37,38, 40,41,42 } ),
				visited.toArray( new Integer[ 0 ] ) );
	}

	@Test
	public void testOutOfBounds()
	{
		final long[] cellGridSize = new long[] { 4, 4, 8 };
		final int[] arr = new int[ ( int ) Intervals.numElements( cellGridSize ) ];
		for ( int i = 0; i < arr.length; ++i )
			arr[ i ] = i;

		final RandomAccessibleInterval< IntType > cellsIndexMap = ArrayImgs.ints( arr, cellGridSize );
		final RandomAccessible< IntType > extendedCellsIndexMap = Views.extendValue( cellsIndexMap, new IntType( -1 ) );

		final RectangleShape.NeighborhoodsAccessible< IntType > neighborhood = new RectangleShape.NeighborhoodsAccessible<>(
				extendedCellsIndexMap,
				new FinalInterval( 2, 2, 2 ),
				RectangleNeighborhoodUnsafe.< IntType >factory() );

		final RandomAccess< Neighborhood< IntType > > block = neighborhood.randomAccess();
		block.setPosition( new int[] { -1, -1, -1 } );
		final List< Integer > visited = new ArrayList<>();
		for ( final IntType item : block.get() )
			visited.add( item.get() );

		Collections.sort( visited );

		final int[] expecteds = new int[ 8 ];
		Arrays.fill( expecteds, -1 );
		expecteds[ expecteds.length - 1 ] = 0;
		Assert.assertArrayEquals( Conversions.toBoxedArray( expecteds ), visited.toArray( new Integer[ 0 ] ) );
	}
}
