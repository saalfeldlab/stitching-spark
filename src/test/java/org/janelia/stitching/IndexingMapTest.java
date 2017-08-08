package org.janelia.stitching;

import java.util.Random;

import org.janelia.util.Conversions;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.Cursor;
import net.imglib2.FinalDimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.view.Views;

public class IndexingMapTest
{
	@Test
	public void test()
	{
		final Random rnd = new Random();
		final int nDims = rnd.nextInt( 3 ) + 1;

		final long[] coeffsDim = new long[ nDims ];
		for ( int d = 0; d < nDims; ++d )
			coeffsDim[ d ] = rnd.nextInt( 10 ) + 1;

		final long[] imgDim = new long[ nDims ];
		for ( int d = 0; d < nDims; ++d )
			imgDim[ d ] = rnd.nextInt( 500 - ( int ) coeffsDim[ d ] + 1 ) + coeffsDim[ d ];

		final RandomAccessibleInterval< ByteType > img = ArrayImgs.bytes( imgDim );
		final RandomAccessibleInterval< IntType > coeffsImg = PipelineIntensityCorrectionStepExecutor.createCoefficientsIndexingMap( new FinalDimensions( imgDim ), Conversions.toIntArray( coeffsDim ) );

		final Cursor< ByteType > imgCursor = Views.flatIterable( img ).localizingCursor();
		final Cursor< IntType > coeffsScaledImgCursor = Views.flatIterable( coeffsImg ).cursor();

		final long[] imgPos = new long[ nDims ], coeffPos = new long[ nDims ];
		final double[] scaling = new double[ nDims ];
		for ( int d = 0; d < nDims; ++d )
			scaling[ d ] = ( double ) imgDim[ d ] / coeffsDim[ d ];
		while ( imgCursor.hasNext() || coeffsScaledImgCursor.hasNext() )
		{
			imgCursor.fwd();
			imgCursor.localize( imgPos );
			for ( int d = 0; d < nDims; ++d )
				coeffPos[ d ] = Math.min( ( long ) Math.floor( imgPos[ d ] / scaling[ d ] ), coeffsDim[ d ] - 1 );
			final int coeffIndexCalculated = ( int ) IntervalIndexer.positionToIndex( coeffPos, coeffsDim );

			final int coeffIndexStretched = coeffsScaledImgCursor.next().get();

			Assert.assertEquals( coeffIndexCalculated, coeffIndexStretched );
		}
	}
}
