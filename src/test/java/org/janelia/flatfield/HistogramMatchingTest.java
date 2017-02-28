package org.janelia.flatfield;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import mpicbg.models.PointMatch;

public class HistogramMatchingTest
{
	private HistogramSettings histogramSettings;

	@Test
	public void testBinValues()
	{
		histogramSettings = new HistogramSettings( 10, 19, 5 );
		Assert.assertEquals( 15, histogramSettings.getBinValue( 2 ), 0.f );

		histogramSettings = new HistogramSettings( 10, 19, 10 );
		Assert.assertEquals( 12, histogramSettings.getBinValue( 2 ), 0.f );
	}

	@Test
	public void testBinning()
	{
		histogramSettings = new HistogramSettings( 0, 8360, 4096 );

		for ( int i = -20000; i <= histogramSettings.minValue; i += 100 )
			Assert.assertEquals( 0, histogramSettings.getBinIndex( i ) );

		for ( int i = histogramSettings.maxValue; i <= 20000; i += 100 )
			Assert.assertEquals( 4095, histogramSettings.getBinIndex( i ) );

		for ( int i = histogramSettings.minValue; i <= histogramSettings.maxValue; i++ )
		{
			final int bin = histogramSettings.getBinIndex( i );
			Assert.assertTrue( bin >= 0 && bin < histogramSettings.bins );
		}
	}

	@Test
	public void testBinningAndMatching()
	{
		histogramSettings = new HistogramSettings( 0, 15, 8 );

		Assert.assertEquals( 2.0, histogramSettings.getBinWidth(), 0.f );

		final short[] arr1 = new short[] { 0, 1, 4, 4, 4, 5, 5, 9, 9, 14 };
		final short[] arr2 = new short[] { 1, 2, 3, 4, 5, 5, 6, 8, 9, 13 };

		final long[] hist1 = new long[ histogramSettings.bins ], hist2 = new long[ histogramSettings.bins ];

		for ( final short val1 : arr1 )
			hist1[ histogramSettings.getBinIndex( val1 ) ]++;

		for ( final short val2 : arr2 )
			hist2[ histogramSettings.getBinIndex( val2 ) ]++;

		Assert.assertArrayEquals( new long[] { 2, 0, 5, 0, 2, 0, 0, 1 }, hist1 );
		Assert.assertArrayEquals( new long[] { 1, 2, 3, 1, 2, 0, 1, 0 }, hist2 );

		final List< PointMatch > matches = FlatfieldCorrectionSolver.generateHistogramMatches( histogramSettings, hist1, hist2, 1 );
		final int[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new int[] { 5, 5, 5, 9 }, arrays[ 0 ] );
		Assert.assertArrayEquals( new int[] { 3, 5, 7, 9 }, arrays[ 1 ] );
		Assert.assertArrayEquals( new int[] { 1, 3, 1, 2 }, arrays[ 2 ] );
	}

	@Test
	public void testMatchingFirstWithLastBin()
	{
		histogramSettings = new HistogramSettings( 0, 15, 4 );

		final List< PointMatch > matches = FlatfieldCorrectionSolver.generateHistogramMatches(
				histogramSettings,
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
		histogramSettings = new HistogramSettings( 0, 19, 5 );

		final List< PointMatch > matches = FlatfieldCorrectionSolver.generateHistogramMatches(
				histogramSettings,
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
		histogramSettings = new HistogramSettings( 0, 15, 4 );

		final List< PointMatch > matches = FlatfieldCorrectionSolver.generateHistogramMatches(
				histogramSettings,
				new long[ histogramSettings.bins ],
				new long[ histogramSettings.bins ],
				10000000 );

		Assert.assertTrue( matches.isEmpty() );
	}

	@Test
	public void testMatchingUndersaturatedValuesWithOversaturatedValues()
	{
		histogramSettings = new HistogramSettings( 0, 15, 4 );

		final List< PointMatch > matches = FlatfieldCorrectionSolver.generateHistogramMatches(
				histogramSettings,
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
}
