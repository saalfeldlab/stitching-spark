/*-
 * #%L
 * Multiview stitching of large datasets.
 * %%
 * Copyright (C) 2016 - 2017 Big Stitcher developers.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */
package net.imglib2.algorithm.phasecorrelation;

import java.util.Arrays;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.SerializableStitchingParameters;
import org.janelia.stitching.StitchSubTilePair;
import org.janelia.stitching.Utils;
import org.janelia.util.ImageImporter;
import org.janelia.util.concurrent.SameThreadExecutorService;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.stitcher.algorithm.PairwiseStitching;
import net.preibisch.stitcher.algorithm.PairwiseStitchingParameters;

public class PhaseCorrelationTest2
{
	public static < T extends NativeType< T > & RealType< T > > void main(final String[] args) throws Exception
	{
		final ImagePlus impA = ImageImporter.openImage( args[ 0 ] );
		final ImagePlus impB = ImageImporter.openImage( args[ 1 ] );

//		final ImagePlus[] impPair = new ImagePlus[] { subtractMean( impA ), subtractMean( impB ) };
		final ImagePlus[] impPair = new ImagePlus[] { impA, impB };

		new ImageJ();

		testNewPhaseCorrelation( impPair[ 0 ], impPair[ 1 ] );

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static < T extends NativeType< T > & RealType< T > > void testOldPhaseCorrelation( final ImagePlus impA, final ImagePlus impB )
	{
		final SerializableStitchingParameters params = new SerializableStitchingParameters();
		params.channel1 = params.channel2 = 1;
		params.checkPeaks = 50;
		params.computeOverlap = true;
		params.subpixelAccuracy = true;
		final SerializablePairWiseStitchingResult shift = StitchSubTilePair.stitchPairwise( new ImagePlus[] { impA, impB }, null, params );
		if ( shift == null ) {
			System.out.println( "got null" );
		} else {
			System.out.println( "shift: " + Arrays.toString( shift.getOffset() ) );
			System.out.println( "cr.corr: " + shift.getCrossCorrelation() );
			System.out.println( "ph.corr: " + shift.getPhaseCorrelation() );
			System.out.println( "isValid: " + shift.getIsValidOverlap() );
		}
	}

	private static < T extends NativeType< T > & RealType< T > > void testNewPhaseCorrelation( final ImagePlus impA, final ImagePlus impB )
	{
		final RandomAccessibleInterval< T > imgA = ImagePlusImgs.from( impA );
		final RandomAccessibleInterval< T > imgB = ImagePlusImgs.from( impB );

		final PairwiseStitchingParameters params = new PairwiseStitchingParameters(0, 50, true, false, false);
		final Pair<Translation, Double> shift = PairwiseStitching.getShift(
				imgA,
				imgB,
				new Translation(imgA.numDimensions()),
				new Translation(imgB.numDimensions()),
				params,
				new SameThreadExecutorService()
			);
		if ( shift == null ) {
			System.out.println( "got null" );
		} else {
			System.out.println( "shift: " + Arrays.toString( shift.getA().getTranslationCopy() ) );
			System.out.println( "cr.corr: " + shift.getB() );
		}
	}

	private static < T extends NativeType< T > & RealType< T > > ImagePlus subtractMean( final ImagePlus src ) throws ImgLibException
	{
		final RandomAccessibleInterval< T > srcImg = ImagePlusImgs.from( src );
		final ImagePlusImg< T, ? > dstImg = new ImagePlusImgFactory< T >().create( srcImg, Util.getTypeFromInterval( srcImg ) );

		double mean = 0;
		for ( final T val : Views.iterable( srcImg ) )
			mean += val.getRealDouble();
		mean /= Intervals.numElements( srcImg );

		final Cursor< T > srcCursor = Views.flatIterable( srcImg ).cursor();
		final Cursor< T > dstCursor = Views.flatIterable( dstImg ).cursor();
		while ( dstCursor.hasNext() || srcCursor.hasNext() )
			dstCursor.next().setReal( srcCursor.next().getRealDouble() - mean );

		final ImagePlus dst = dstImg.getImagePlus();
		Utils.workaroundImagePlusNSlices( dst );
		return dst;
	}
}
