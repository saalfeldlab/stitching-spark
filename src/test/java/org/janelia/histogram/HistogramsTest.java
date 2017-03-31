package org.janelia.histogram;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import mpicbg.models.PointMatch;

public class HistogramsTest
{
	private static final double EPSILON = 1e-10;

	@Test
	public void testBinWidth()
	{
		Assert.assertEquals( 2, new Histogram( 10, 20, 5 ).getBinWidth(), 0.f );
		Assert.assertEquals( 1, new Histogram( 10, 20, 10 ).getBinWidth(), 0.f );
	}

	@Test
	public void testBinValues()
	{
		Assert.assertEquals( 14, new Histogram( 10, 20, 5 ).getBinValue( 2 ), 0.f );
		Assert.assertEquals( 12, new Histogram( 10, 20, 10 ).getBinValue( 2 ), 0.f );
	}

	@Test
	public void testPutSet()
	{
		final Histogram histogram = new Histogram( 10, 20, 5 );
		histogram.put( 15.5, 5 );
		histogram.put( 8.1, 2.5 );
		histogram.put( 25.7, 0.2 );
		histogram.put( 11.2 );
		Assert.assertEquals( 8.7, histogram.getQuantityTotal(), EPSILON );
		Assert.assertEquals( 2.5, histogram.getQuantityLessThanMin(), EPSILON );
		Assert.assertEquals( 0.2, histogram.getQuantityGreaterThanMax(), EPSILON );

		histogram.set( 2, 2.5 );
		histogram.set( 4, 0.1 );
		histogram.set( 3, 0.25 );
		Assert.assertEquals( 6.35, histogram.getQuantityTotal(), EPSILON );
		Assert.assertEquals( 2.5, histogram.getQuantityLessThanMin(), EPSILON );
		Assert.assertEquals( 0, histogram.getQuantityGreaterThanMax(), EPSILON );
	}

	@Test
	public void testBinningAndMatching()
	{
		final Histogram[] histograms = new Histogram[] { new Histogram( 0, 16, 8 ), new Histogram( 0, 16, 8 ) };
		final int[][] values = new int[][] {
			new int[] { 0, 1, 4, 4, 4, 5, 5, 9, 9, 14 },
			new int[] { 1, 2, 3, 4, 5, 5, 6, 8, 9, 13 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ].put( value );

		Assert.assertArrayEquals( new double[] { 2, 0, 5, 0, 2, 0, 0, 1 }, getHistogramArray( histograms[ 0 ] ), EPSILON );
		Assert.assertArrayEquals( new double[] { 1, 2, 3, 1, 2, 0, 1, 0 }, getHistogramArray( histograms[ 1 ] ), EPSILON );

		final List< PointMatch > matches = generateHistogramMatches( histograms[ 0 ], histograms[ 1 ] );
		final int[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new int[] { 0, 0, 4, 4, 4, 8, 14 }, arrays[ 0 ] );
		Assert.assertArrayEquals( new int[] { 0, 2, 2, 4, 6, 8, 12 }, arrays[ 1 ] );
		Assert.assertArrayEquals( new int[] { 1, 1, 1, 3, 1, 2,  1 }, arrays[ 2 ] );
	}

	@Test
	public void testMatchingFirstWithLastBin()
	{
		final Histogram[] histograms = new Histogram[] { new Histogram( 0, 16, 4 ), new Histogram( 0, 16, 4 ) };
		final int[][] values = new int[][] {
			new int[] { 0, 1, 1, 2, 2, 3 },
			new int[] { 12, 12, 13, 14, 15, 15 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ].put( value );

		final List< PointMatch > matches = generateHistogramMatches( histograms[ 0 ], histograms[ 1 ] );
		final int[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new int[] {  0 }, arrays[ 0 ] );
		Assert.assertArrayEquals( new int[] { 12 }, arrays[ 1 ] );
		Assert.assertArrayEquals( new int[] {  6 }, arrays[ 2 ] );
	}

	@Test
	public void testMatchingEveryBinWithLastBin()
	{
		final Histogram[] histograms = new Histogram[] { new Histogram( 0, 20, 5 ), new Histogram( 0, 20, 5 ) };
		final int[][] values = new int[][] {
			new int[] { 0, 6, 11, 12 },
			new int[] { 16, 17, 18, 19 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ].put( value );

		final List< PointMatch > matches = generateHistogramMatches( histograms[ 0 ], histograms[ 1 ] );
		final int[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new int[] {  0,  4,  8, 12 }, arrays[ 0 ] );
		Assert.assertArrayEquals( new int[] { 16, 16, 16, 16 }, arrays[ 1 ] );
		Assert.assertArrayEquals( new int[] {  1,  1,  1,  1 }, arrays[ 2 ] );
	}

	@Test
	public void testMatchingEmptyHistograms()
	{
		final Histogram[] histograms = new Histogram[] { new Histogram( 0, 16, 4 ), new Histogram( 0, 16, 4 ) };
		final List< PointMatch > matches = generateHistogramMatches( histograms[ 0 ], histograms[ 1 ] );
		Assert.assertTrue( matches.isEmpty() );
	}

	@Test
	public void testMatchingUndersaturatedValuesWithOversaturatedValues()
	{
		final Histogram[] histograms = new Histogram[] { new Histogram( 2, 12, 5 ), new Histogram( 2, 12, 5 ) };
		final int[][] values = new int[][] {
			new int[] { 0, 1, 1, 2, 2, 5 },
			new int[] { 12, 12, 13, 14, 15, 15 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ].put( value );

		final List< PointMatch > matches = generateHistogramMatches( histograms[ 0 ], histograms[ 1 ] );
		Assert.assertTrue( matches.isEmpty() );
	}

	@Test
	public void testNonEmptyMatchingUndersaturatedAndOversaturatedValues()
	{
		final Histogram[] histograms = new Histogram[] { new Histogram( 2, 12, 5 ), new Histogram( 2, 12, 5 ) };
		final int[][] values = new int[][] {
			new int[] { 0, 1, 2, 3, 4, 5,  6,  7,  8,  9 },
			new int[] { 5, 5, 6, 7, 8, 9, 10, 12, 14, 15 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ].put( value );

		final List< PointMatch > matches = generateHistogramMatches( histograms[ 0 ], histograms[ 1 ] );

		final int[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new int[] { 2, 4,  6 }, arrays[ 0 ] );
		Assert.assertArrayEquals( new int[] { 6, 8, 10 }, arrays[ 1 ] );
		Assert.assertArrayEquals( new int[] { 2, 2,  1 }, arrays[ 2 ] );
	}

	@Test
	public void testInnerMatchingUndersaturatedAndOversaturatedValues()
	{
		final Histogram[] histograms = new Histogram[] { new Histogram( 2, 12, 5 ), new Histogram( 2, 12, 5 ) };
		final int[][] values = new int[][] {
			new int[] { 0, 1, 1, 1, 1, 7, 10, 11, 11 },
			new int[] { 3, 4, 5, 6, 6, 8, 12, 13, 15 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ].put( value );

		final List< PointMatch > matches = generateHistogramMatches( histograms[ 0 ], histograms[ 1 ] );

		final int[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new int[] { 6 }, arrays[ 0 ] );
		Assert.assertArrayEquals( new int[] { 8 }, arrays[ 1 ] );
		Assert.assertArrayEquals( new int[] { 1 }, arrays[ 2 ] );
	}

	private List< PointMatch > generateHistogramMatches( final Histogram hist1, final Histogram hist2 )
	{
		Assert.assertTrue( Math.abs( hist1.getQuantityTotal() - hist2.getQuantityTotal() ) < EPSILON );
		return HistogramsMatching.generateHistogramMatches( hist1, hist2 );
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

	private double[] getHistogramArray( final Histogram histogram )
	{
		final double[] array = new double[ histogram.getNumBins() ];
		for ( int i = 0; i < array.length; ++i )
			array[ i ] = histogram.get( i );
		return array;
	}
}
