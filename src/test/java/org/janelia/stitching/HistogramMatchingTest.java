package org.janelia.stitching;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import mpicbg.models.PointMatch;

public class HistogramMatchingTest
{
	private int histMinValue, histMaxValue, bins;

	@Test
	public void testBinning()
	{
		bins = 4096;
		histMinValue = 0;
		histMaxValue = 8360;

		for ( int i = -20000; i <= 0; i += 100 )
			Assert.assertEquals( 0, getBinIndex( i ) );

		for ( int i = 8360; i <= 20000; i += 100 )
			Assert.assertEquals( 4095, getBinIndex( i ) );

		for ( int i = histMinValue; i <= histMaxValue; i++ )
		{
			final int binIndex = getBinIndex( i );
			Assert.assertTrue( binIndex >= 0 && binIndex < bins );
		}
	}

	@Test
	public void testBinningAndMatching()
	{
		bins = 8;
		histMinValue = 0;
		histMaxValue = 15;

		Assert.assertEquals( 2.0, getBinWidth(), 0.f );

		final short[] arr1 = new short[] { 0, 1, 4, 4, 4, 5, 5, 9, 9, 14 };
		final short[] arr2 = new short[] { 1, 2, 3, 4, 5, 5, 6, 8, 9, 13 };

		final long[] hist1 = new long[ bins ], hist2 = new long[ bins ];

		for ( final short val1 : arr1 )
			hist1[ getBinIndex( val1 ) ]++;

		for ( final short val2 : arr2 )
			hist2[ getBinIndex( val2 ) ]++;

		Assert.assertArrayEquals( new long[] { 2, 0, 5, 0, 2, 0, 0, 1 }, hist1 );
		Assert.assertArrayEquals( new long[] { 1, 2, 3, 1, 2, 0, 1, 0 }, hist2 );

		final List< PointMatch > matches = generateHistogramMatches( hist1, hist2, 1 );
		final int[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new int[] { 5, 5, 5, 9 }, arrays[ 0 ] );
		Assert.assertArrayEquals( new int[] { 3, 5, 7, 9 }, arrays[ 1 ] );
		Assert.assertArrayEquals( new int[] { 1, 3, 1, 2 }, arrays[ 2 ] );
	}

	@Test
	public void testMatchingFirstWithLastBin()
	{
		bins = 4;
		histMinValue = 0;
		histMaxValue = 15;
		final List< PointMatch > matches = generateHistogramMatches(
				new long[] { 0, 6, 0, 0 },
				new long[] { 0, 0, 2, 0 },
				3 );
		final int[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new int[] {  6 }, arrays[ 0 ] );
		Assert.assertArrayEquals( new int[] { 10 }, arrays[ 1 ] );
		Assert.assertArrayEquals( new int[] {  6 }, arrays[ 2 ] );
	}

	@Test
	public void testMatchingEveryBinWithLastBin()
	{
		bins = 5;
		histMinValue = 0;
		histMaxValue = 19;
		final List< PointMatch > matches = generateHistogramMatches(
				new long[] { 0, 1, 1, 1, 0 },
				new long[] { 0, 0, 0, 1, 0 },
				3 );
		final int[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new int[] {  6, 10, 14 }, arrays[ 0 ] );
		Assert.assertArrayEquals( new int[] { 14, 14, 14 }, arrays[ 1 ] );
		Assert.assertArrayEquals( new int[] {  1,  1,  1 }, arrays[ 2 ] );
	}

	@Test
	public void testMatchingEmptyHistograms()
	{
		bins = 4;
		histMinValue = 0;
		histMaxValue = 15;
		final List< PointMatch > matches = generateHistogramMatches( new long[ bins ], new long[ bins ], 10000000 );
		Assert.assertTrue( matches.isEmpty() );
	}

	@Test
	public void testMatchingUndersaturatedValuesWithOversaturatedValues()
	{
		bins = 4;
		histMinValue = 0;
		histMaxValue = 15;
		final List< PointMatch > matches = generateHistogramMatches(
				new long[] { 8, 0, 0, 0 },
				new long[] { 0, 0, 0, 2 },
				4 );
		Assert.assertTrue( matches.isEmpty() );
	}

	private int[][] matchesToArrays( final List< PointMatch > matches )
	{
		final int[] p = new int[ matches.size() ], q = new int[ matches.size() ], w = new int[ matches.size() ];
		for ( int i = 0; i < matches.size(); i++ )
		{
			Assert.assertEquals( 1, matches.get( i ).getP1().getL().length );
			Assert.assertEquals( 1, matches.get( i ).getP2().getL().length );
			p[ i ] = ( int ) matches.get( i ).getP1().getL()[ 0 ];
			q[ i ] = ( int ) matches.get( i ).getP2().getL()[ 0 ];
			w[ i ] = ( int ) matches.get( i ).getWeight();
		}
		return new int[][] { p, q, w };
	}

	private double getBinWidth()
	{
		return IlluminationCorrection.getBinWidth( histMinValue, histMaxValue, bins );
	}
	private int getBinIndex( final int value )
	{
		return IlluminationCorrection.getBinIndex( value, histMinValue, histMaxValue, bins );
	}
	private List< PointMatch > generateHistogramMatches( final long[] histogram, final long[] referenceHistogram, final long referenceHistogramMultiplier )
	{
		return IlluminationCorrection.generateHistogramMatches(
				histogram, referenceHistogram, referenceHistogramMultiplier,
				histMinValue, histMaxValue, bins );
	}
}
