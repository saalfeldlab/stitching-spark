package org.janelia.flatfield;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import mpicbg.models.PointMatch;
import net.imglib2.histogram.Real1dBinMapper;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.composite.RealComposite;

public class HistogramMatchingTest
{
	private static final double EPSILON = 1e-10;

	@Test
	public void testBinValues()
	{
		Assert.assertArrayEquals( new double[] { Double.NEGATIVE_INFINITY, 11, 13, 15, 17, 19, Double.POSITIVE_INFINITY }, HistogramMatching.getBinValues( 10, 20, 7 ), EPSILON );
		Assert.assertArrayEquals( new double[] { Double.NEGATIVE_INFINITY, 10.5, 11.5, 12.5, 13.5, 14.5, Double.POSITIVE_INFINITY }, HistogramMatching.getBinValues( 10, 15, 7 ), EPSILON );
	}

	@Test
	public void testBinningAndMatching()
	{
		final double minVal = 0, maxVal = 16;
		final int bins = 8;

		final Real1dBinMapper< IntType > binMapper = new Real1dBinMapper<>( minVal, maxVal, bins, true );
		final double[][] histograms = new double[ 2 ][ bins ];

		final int[][] values = new int[][] {
			new int[] { 0, 1, 4, 4, 4, 5, 5, 9, 9, 14 },
			new int[] { 1, 2, 3, 4, 5, 5, 6, 8, 9, 13 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ][ ( int ) binMapper.map( new IntType( value ) ) ] += 1;

		Assert.assertArrayEquals( new double[] { 0, 2, 5, 0, 2, 0, 1, 0 }, histograms[ 0 ], EPSILON );
		Assert.assertArrayEquals( new double[] { 0, 2, 4, 1, 2, 1, 0, 0 }, histograms[ 1 ], EPSILON );

		final RealComposite< DoubleType >[] wrappedHistograms = new RealComposite[ 2 ];
		for ( int i = 0; i < 2; ++i )
			wrappedHistograms[ i ] = new RealComposite<>( ArrayImgs.doubles( histograms[ i ], histograms[ i ].length ).randomAccess(), histograms[ i ].length );

		final double[] binValues = HistogramMatching.getBinValues( minVal, maxVal, bins );
		final double binWidth = 16. / 6;
		final double[] expectedBinValues = new double[ bins ];
		for ( int bin = 1; bin < bins - 1; ++bin )
			expectedBinValues[ bin ] = 16. / 6 * ( bin - 1 ) + binWidth / 2;
		expectedBinValues[ 0 ] = Double.NEGATIVE_INFINITY;
		expectedBinValues[ bins - 1 ] = Double.POSITIVE_INFINITY;
		Assert.assertArrayEquals( expectedBinValues, binValues, EPSILON );

		final List< PointMatch > matches = HistogramMatching.generateHistogramMatches(
				wrappedHistograms[ 0 ],
				wrappedHistograms[ 1 ],
				binValues
			);

		final double[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new double[] { binValues[ 1 ], binValues[ 2 ], binValues[ 2 ], binValues[ 4 ], binValues[ 6 ] }, arrays[ 0 ], EPSILON );
		Assert.assertArrayEquals( new double[] { binValues[ 1 ], binValues[ 2 ], binValues[ 3 ], binValues[ 4 ], binValues[ 5 ] }, arrays[ 1 ], EPSILON );
		Assert.assertArrayEquals( new double[] { 2, 4, 1, 2, 1 }, arrays[ 2 ], EPSILON );
	}

	@Test
	public void testMatchingFirstWithLastBin()
	{
		final double minVal = 0, maxVal = 16;
		final int bins = 4;

		final Real1dBinMapper< IntType > binMapper = new Real1dBinMapper<>( minVal, maxVal, bins, true );
		final double[][] histograms = new double[ 2 ][ bins ];

		final int[][] values = new int[][] {
			new int[] { 0, 1, 1, 2, 2, 3 },
			new int[] { 12, 12, 13, 14, 15, 15 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ][ ( int ) binMapper.map( new IntType( value ) ) ] += 1;

		final RealComposite< DoubleType >[] wrappedHistograms = new RealComposite[ 2 ];
		for ( int i = 0; i < 2; ++i )
			wrappedHistograms[ i ] = new RealComposite<>( ArrayImgs.doubles( histograms[ i ], histograms[ i ].length ).randomAccess(), histograms[ i ].length );

		final double[] binValues = HistogramMatching.getBinValues( minVal, maxVal, bins );

		final List< PointMatch > matches = HistogramMatching.generateHistogramMatches(
				wrappedHistograms[ 0 ],
				wrappedHistograms[ 1 ],
				binValues
			);

		final double[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new double[] { binValues[ 1 ] }, arrays[ 0 ], EPSILON );
		Assert.assertArrayEquals( new double[] { binValues[ 2 ] }, arrays[ 1 ], EPSILON );
		Assert.assertArrayEquals( new double[] { 6 }, arrays[ 2 ], EPSILON );
	}

	@Test
	public void testMatchingEveryBinWithLastBin()
	{
		final double minVal = 0, maxVal = 20;
		final int bins = 7;

		final Real1dBinMapper< IntType > binMapper = new Real1dBinMapper<>( minVal, maxVal, bins, true );
		final double[][] histograms = new double[ 2 ][ bins ];

		final int[][] values = new int[][] {
			new int[] { 0, 6, 11, 12 },
			new int[] { 16, 17, 18, 19 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ][ ( int ) binMapper.map( new IntType( value ) ) ] += 1;

		final RealComposite< DoubleType >[] wrappedHistograms = new RealComposite[ 2 ];
		for ( int i = 0; i < 2; ++i )
			wrappedHistograms[ i ] = new RealComposite<>( ArrayImgs.doubles( histograms[ i ], histograms[ i ].length ).randomAccess(), histograms[ i ].length );

		final double[] binValues = HistogramMatching.getBinValues( minVal, maxVal, bins );

		final List< PointMatch > matches = HistogramMatching.generateHistogramMatches(
				wrappedHistograms[ 0 ],
				wrappedHistograms[ 1 ],
				binValues
			);

		final double[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new double[] { binValues[ 1 ], binValues[ 2 ], binValues[ 3 ], binValues[ 4 ] }, arrays[ 0 ], EPSILON );
		Assert.assertArrayEquals( new double[] { binValues[ 5 ], binValues[ 5 ], binValues[ 5 ], binValues[ 5 ] }, arrays[ 1 ], EPSILON );
		Assert.assertArrayEquals( new double[] {  1.0,  1.0,  1.0,  1.0 }, arrays[ 2 ], EPSILON );
	}

	@Test
	public void testMatchingEmptyHistograms()
	{
		final double minVal = 0, maxVal = 16;
		final int bins = 4;

		final double[][] histograms = new double[ 2 ][ bins ];

		final RealComposite< DoubleType >[] wrappedHistograms = new RealComposite[ 2 ];
		for ( int i = 0; i < 2; ++i )
			wrappedHistograms[ i ] = new RealComposite<>( ArrayImgs.doubles( histograms[ i ], histograms[ i ].length ).randomAccess(), histograms[ i ].length );

		final double[] binValues = HistogramMatching.getBinValues( minVal, maxVal, bins );

		final List< PointMatch > matches = HistogramMatching.generateHistogramMatches(
				wrappedHistograms[ 0 ],
				wrappedHistograms[ 1 ],
				binValues
			);

		Assert.assertTrue( matches.isEmpty() );
	}

	@Test
	public void testMatchingUndersaturatedValuesWithOversaturatedValues()
	{
		final double minVal = 2, maxVal = 12;
		final int bins = 7;

		final Real1dBinMapper< IntType > binMapper = new Real1dBinMapper<>( minVal, maxVal, bins, true );
		final double[][] histograms = new double[ 2 ][ bins ];

		final int[][] values = new int[][] {
			new int[] { 0, 1, 1, 2, 2, 5 },
			new int[] { 12, 12, 13, 14, 15, 15 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ][ ( int ) binMapper.map( new IntType( value ) ) ] += 1;

		final RealComposite< DoubleType >[] wrappedHistograms = new RealComposite[ 2 ];
		for ( int i = 0; i < 2; ++i )
			wrappedHistograms[ i ] = new RealComposite<>( ArrayImgs.doubles( histograms[ i ], histograms[ i ].length ).randomAccess(), histograms[ i ].length );

		final double[] binValues = HistogramMatching.getBinValues( minVal, maxVal, bins );

		final List< PointMatch > matches = HistogramMatching.generateHistogramMatches(
				wrappedHistograms[ 0 ],
				wrappedHistograms[ 1 ],
				binValues
			);

		Assert.assertTrue( matches.isEmpty() );
	}

	@Test
	public void testNonEmptyMatchingUndersaturatedAndOversaturatedValues()
	{
		final double minVal = 2, maxVal = 12;
		final int bins = 7;

		final Real1dBinMapper< IntType > binMapper = new Real1dBinMapper<>( minVal, maxVal, bins, true );
		final double[][] histograms = new double[ 2 ][ bins ];

		final int[][] values = new int[][] {
			new int[] { 0, 1, 2, 3, 4, 5,  6,  7,  8,  9 },
			new int[] { 5, 5, 6, 7, 8, 9, 10, 12, 13, 15 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ][ ( int ) binMapper.map( new IntType( value ) ) ] += 1;

		final RealComposite< DoubleType >[] wrappedHistograms = new RealComposite[ 2 ];
		for ( int i = 0; i < 2; ++i )
			wrappedHistograms[ i ] = new RealComposite<>( ArrayImgs.doubles( histograms[ i ], histograms[ i ].length ).randomAccess(), histograms[ i ].length );

		final double[] binValues = HistogramMatching.getBinValues( minVal, maxVal, bins );

		final List< PointMatch > matches = HistogramMatching.generateHistogramMatches(
				wrappedHistograms[ 0 ],
				wrappedHistograms[ 1 ],
				binValues
			);

		final double[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new double[] { binValues[ 1 ], binValues[ 2 ], binValues[ 3 ] }, arrays[ 0 ], EPSILON );
		Assert.assertArrayEquals( new double[] { binValues[ 3 ], binValues[ 4 ], binValues[ 5 ] }, arrays[ 1 ], EPSILON );
		Assert.assertArrayEquals( new double[] { 2.0, 2.0, 2.0 }, arrays[ 2 ], EPSILON );
	}

	@Test
	public void testInnerMatchingUndersaturatedAndOversaturatedValues()
	{
		final double minVal = 2, maxVal = 12;
		final int bins = 7;

		final Real1dBinMapper< IntType > binMapper = new Real1dBinMapper<>( minVal, maxVal, bins, true );
		final double[][] histograms = new double[ 2 ][ bins ];

		final int[][] values = new int[][] {
			new int[] { 0, 1, 1, 1, 1, 7, 10, 11, 11 },
			new int[] { 3, 4, 5, 6, 6, 8, 13, 14, 15 }
		};
		for ( int i = 0; i < 2; ++i )
			for ( final int value : values[ i ] )
				histograms[ i ][ ( int ) binMapper.map( new IntType( value ) ) ] += 1;

		final RealComposite< DoubleType >[] wrappedHistograms = new RealComposite[ 2 ];
		for ( int i = 0; i < 2; ++i )
			wrappedHistograms[ i ] = new RealComposite<>( ArrayImgs.doubles( histograms[ i ], histograms[ i ].length ).randomAccess(), histograms[ i ].length );

		final double[] binValues = HistogramMatching.getBinValues( minVal, maxVal, bins );

		final List< PointMatch > matches = HistogramMatching.generateHistogramMatches(
				wrappedHistograms[ 0 ],
				wrappedHistograms[ 1 ],
				binValues
			);

		final double[][] arrays = matchesToArrays( matches );
		Assert.assertArrayEquals( new double[] { binValues[ 3 ] }, arrays[ 0 ], EPSILON );
		Assert.assertArrayEquals( new double[] { binValues[ 4 ] }, arrays[ 1 ], EPSILON );
		Assert.assertArrayEquals( new double[] { 1.0 }, arrays[ 2 ], EPSILON );
	}

	private double[][] matchesToArrays( final List< PointMatch > matches )
	{
		final double[] p = new double[ matches.size() ], q = new double[ matches.size() ], w = new double[ matches.size() ];
		for ( int i = 0; i < matches.size(); i++ )
		{
			Assert.assertEquals( 1, matches.get( i ).getP1().getL().length );
			Assert.assertEquals( 1, matches.get( i ).getP2().getL().length );
			p[ i ] = matches.get( i ).getP1().getL()[ 0 ];
			q[ i ] = matches.get( i ).getP2().getL()[ 0 ];
			w[ i ] = matches.get( i ).getWeight();
		}
		return new double[][] { p, q, w };
	}
}
