package org.janelia.flatfield;

import java.util.Random;

import org.janelia.flatfield.FlatfieldCorrectionSolver.FlatfieldSolution;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;

public class UnpivotSolutionTest
{
	private static final double EPSILON = 1e-10;

	@Test
	public void test()
	{
		final Random rnd = new Random();

		final long[] dimensions = new long[ rnd.nextInt( 4 ) + 1 ];
		for ( int d = 0; d < dimensions.length; ++d )
			dimensions[ d ] = rnd.nextInt( 8 ) + 1;

		final double[] scalingArr = new double[ ( int ) Intervals.numElements( dimensions ) ], translationArr = new double[ ( int ) Intervals.numElements( dimensions ) ], offsets = new double[ ( int ) Intervals.numElements( dimensions ) ];
		for ( int i = 0; i < Intervals.numElements( dimensions ); ++i )
		{
			scalingArr[ i ] = ( rnd.nextDouble() - 0.5 ) * 4;
			translationArr[ i ] = ( rnd.nextDouble() - 0.5 ) * 100;
			offsets[ i ] = ( rnd.nextDouble() - 0.5 ) * 50;
		}

		final FlatfieldSolution flatfieldSolution = new FlatfieldSolution(
				new ValuePair<>( ArrayImgs.doubles( scalingArr, dimensions ), ArrayImgs.doubles( translationArr, dimensions ) ),
				ArrayImgs.doubles( offsets, dimensions ) );

		final Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > expected = unpivotSolutionUsingFormula( flatfieldSolution );
		final Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > actual = FlatfieldCorrectionSolver.unpivotSolution( flatfieldSolution );

		Assert.assertArrayEquals( raiToArray( expected.getA() ), raiToArray( actual.getA() ), EPSILON );
		Assert.assertArrayEquals( raiToArray( expected.getB() ), raiToArray( actual.getB() ), EPSILON );
	}

	private < T extends RealType< T > > double[] raiToArray( final RandomAccessibleInterval< T > rai )
	{
		final double[] arr = new double[ ( int ) Intervals.numElements( rai ) ];
		final RandomAccessibleInterval< DoubleType > arrImg = ArrayImgs.doubles( arr, Intervals.dimensionsAsLongArray( rai ) );
		final Cursor< T > srcCursor = Views.flatIterable( rai ).cursor();
		final Cursor< DoubleType > dstCursor = Views.flatIterable( arrImg ).cursor();
		while ( dstCursor.hasNext() || srcCursor.hasNext() )
			dstCursor.next().set( srcCursor.next().getRealDouble() );
		return arr;
	}

	private Pair< RandomAccessibleInterval< DoubleType >, RandomAccessibleInterval< DoubleType > > unpivotSolutionUsingFormula( final FlatfieldSolution solution )
	{
		final RandomAccessibleInterval< DoubleType > unpivotedTranslation = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( solution.correctionFields.getB() ) );
		final Cursor< DoubleType > scalingCursor = Views.flatIterable( solution.correctionFields.getA() ).cursor();
		final Cursor< DoubleType > translationCursor = Views.flatIterable( solution.correctionFields.getB() ).cursor();
		final Cursor< DoubleType > offsetsCursor = Views.flatIterable( solution.pivotValues ).cursor();
		final Cursor< DoubleType > unpivotedTranslationCursor = Views.flatIterable( unpivotedTranslation ).cursor();
		while ( scalingCursor.hasNext() || translationCursor.hasNext() || offsetsCursor.hasNext() || unpivotedTranslationCursor.hasNext() )
			unpivotedTranslationCursor.next().set( translationCursor.next().get() + offsetsCursor.next().get() * ( 1 - scalingCursor.next().get() ) );
		return new ValuePair<>( solution.correctionFields.getA(), unpivotedTranslation );
	}
}
