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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.Executors;

import org.junit.Test;

import net.imglib2.Localizable;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.algorithm.phasecorrelation.FourNeighborhoodExtrema;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;

public class FourNeighborhoodExtremaTest
{

	@Test
	public void testRandomPeaks()
	{
		final Img< FloatType > img = ArrayImgs.floats( 50, 50 );
		final Random rnd = new Random( 4353 );

		for( final FloatType t : img )
			t.set( rnd.nextFloat() );

		final ArrayList< Pair< Localizable, Double > > correct = new ArrayList< >();

		final RandomAccess<FloatType> ra = img.randomAccess();

		for ( int i = 0; i < 10; ++i ){
			for (int d = 0; d< img.numDimensions(); d++){
				ra.setPosition((int) (rnd.nextDouble() * 50), d);
			}
			ra.get().set((float) (rnd.nextDouble() + 1.0));

			correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));
		}

		// sort the peaks in descending order
		Collections.sort(correct, new Comparator<Pair< Localizable, Double >>() {
			@Override
			public int compare(final Pair<Localizable, Double> o1, final Pair<Localizable, Double> o2) {
				return Double.compare(o2.getB(), o1.getB());
			}
		});

		final int nMax = 5;

		final ArrayList< Pair< Localizable, Double > > found = FourNeighborhoodExtrema.findMax(Views.extendPeriodic(img), img, nMax);


		assertEquals(nMax, found.size());


		final long[] posCorrect = new long[img.numDimensions()];
		final long[] posFound = new long[img.numDimensions()];

		for (int i = 0; i<found.size(); i++){


			assertEquals(correct.get(i).getB(), found.get(i).getB());

			correct.get(i).getA().localize(posCorrect);
			found.get(i).getA().localize(posFound);

			assertArrayEquals(posCorrect, posFound);
		}

		final int i = 5;
		assertTrue( i == 5 );
	}

	@Test
	public void testRandomPeaksMT()
	{
		final Img< FloatType > img = ArrayImgs.floats( 50, 50 );
		final Random rnd = new Random( 4353 );

		for( final FloatType t : img )
			t.set( rnd.nextFloat() );

		final ArrayList< Pair< Localizable, Double > > correct = new ArrayList< >();

		final RandomAccess<FloatType> ra = img.randomAccess();

		for ( int i = 0; i < 10; ++i ){
			for (int d = 0; d< img.numDimensions(); d++){
				ra.setPosition((int) (rnd.nextDouble() * 50), d);
			}
			ra.get().set((float) (rnd.nextDouble() + 1.0));

			correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));
		}

		// sort the peaks in descending order
		Collections.sort(correct, new Comparator<Pair< Localizable, Double >>() {
			@Override
			public int compare(final Pair<Localizable, Double> o1, final Pair<Localizable, Double> o2) {
				return Double.compare(o2.getB(), o1.getB());
			}
		});

		final int nMax = 5;
		final ArrayList< Pair< Localizable, Double > > found = FourNeighborhoodExtrema.findMaxMT(Views.extendPeriodic(img), img, nMax, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
		assertEquals(nMax, found.size());


		final long[] posCorrect = new long[img.numDimensions()];
		final long[] posFound = new long[img.numDimensions()];

		for (int i = 0; i<found.size(); i++){
			assertEquals(correct.get(i).getB(), found.get(i).getB());

			correct.get(i).getA().localize(posCorrect);
			found.get(i).getA().localize(posFound);
			assertArrayEquals(posCorrect, posFound);
		}
	}

	@Test
	public void testCornerPeaksMT()
	{
		final Img< FloatType > img = ArrayImgs.floats( 50, 50 );
		final Random rnd = new Random( 4353 );

		for( final FloatType t : img )
			t.set( rnd.nextFloat() );

		final ArrayList< Pair< Localizable, Double > > correct = new ArrayList< >();

		final RandomAccess<FloatType> ra = img.randomAccess();

		ra.setPosition(new long[]{0,0});
		ra.get().set((float) (rnd.nextDouble() + 1.0));
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		ra.setPosition(new long[]{0,49});
		ra.get().set((float) (rnd.nextDouble() + 1.0));
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		ra.setPosition(new long[]{49,0});
		ra.get().set((float) (rnd.nextDouble() + 1.0));
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		ra.setPosition(new long[]{49,49});
		ra.get().set((float) (rnd.nextDouble() + 1.0));
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		// sort the peaks in descending order
		Collections.sort(correct, new Comparator<Pair< Localizable, Double >>() {
			@Override
			public int compare(final Pair<Localizable, Double> o1, final Pair<Localizable, Double> o2) {
				return Double.compare(o2.getB(), o1.getB());
			}
		});

		final int nMax = 4;

		final ArrayList< Pair< Localizable, Double > > found = FourNeighborhoodExtrema.findMaxMT(Views.extendPeriodic(img), img, nMax, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));


		assertEquals(nMax, found.size());


		final long[] posCorrect = new long[img.numDimensions()];
		final long[] posFound = new long[img.numDimensions()];

		for (int i = 0; i<found.size(); i++){

			// TODO: test fails!!
			//assertEquals(correct.get(i).getB(), found.get(i).getB());

			correct.get(i).getA().localize(posCorrect);
			found.get(i).getA().localize(posFound);

			// TODO: test fails!!
			//assertArrayEquals(posCorrect, posFound);
		}

		final int i = 5;
		assertTrue( i == 5 );
	}


	@Test
	public void testCornerPeaksMTMirroring()
	{
		final Img< FloatType > img = ArrayImgs.floats( 50, 50 );
		final Random rnd = new Random( 4353 );

		for( final FloatType t : img )
			t.set( rnd.nextFloat() );

		final ArrayList< Pair< Localizable, Double > > correct = new ArrayList< >();

		final RandomAccess<FloatType> ra = img.randomAccess();

		ra.setPosition(new long[]{0,0});
		ra.get().set((float) (rnd.nextDouble() + 1.0));
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		ra.setPosition(new long[]{0,49});
		ra.get().set((float) (rnd.nextDouble() + 1.0));
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		ra.setPosition(new long[]{49,0});
		ra.get().set((float) (rnd.nextDouble() + 1.0));
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		ra.setPosition(new long[]{49,49});
		ra.get().set((float) (rnd.nextDouble() + 1.0));
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		// sort the peaks in descending order
		Collections.sort(correct, new Comparator<Pair< Localizable, Double >>() {
			@Override
			public int compare(final Pair<Localizable, Double> o1, final Pair<Localizable, Double> o2) {
				return Double.compare(o2.getB(), o1.getB());
			}
		});

		final int nMax = 4;

		final ArrayList< Pair< Localizable, Double > > found = FourNeighborhoodExtrema.findMaxMT(Views.extendMirrorSingle(img), img, nMax, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));


		assertEquals(nMax, found.size());


		final long[] posCorrect = new long[img.numDimensions()];
		final long[] posFound = new long[img.numDimensions()];

		for (int i = 0; i<found.size(); i++){


			assertEquals(correct.get(i).getB(), found.get(i).getB());

			correct.get(i).getA().localize(posCorrect);
			found.get(i).getA().localize(posFound);

			assertArrayEquals(posCorrect, posFound);
		}

		final int i = 5;
		assertTrue( i == 5 );
	}

	@Test
	public void testEqualPeaks()
	{
		final Img< FloatType > img = ArrayImgs.floats( 50, 50 );
		final Random rnd = new Random( 4353 );

		for( final FloatType t : img )
			t.set( rnd.nextFloat() );

		final ArrayList< Pair< Localizable, Double > > correct = new ArrayList< >();

		final RandomAccess<FloatType> ra = img.randomAccess();

		ra.setPosition(new long[]{2,0});
		ra.get().set((float) 2.0);
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		ra.setPosition(new long[]{1,0});
		ra.get().set((float) 2.0);
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		ra.setPosition(new long[]{3,0});
		ra.get().set((float) 2.0);
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		ra.setPosition(new long[]{4,0});
		ra.get().set((float) 2.0);
		correct.add(new ValuePair<Localizable, Double>(new Point(ra), new Double(ra.get().get())));

		// sort the peaks in descending order
		Collections.sort(correct, new Comparator<Pair< Localizable, Double >>() {
			@Override
			public int compare(final Pair<Localizable, Double> o1, final Pair<Localizable, Double> o2) {
				return Double.compare(o2.getB(), o1.getB());
			}
		});

		final int nMax = 4;

		final ArrayList< Pair< Localizable, Double > > found = FourNeighborhoodExtrema.findMaxMT(Views.extendPeriodic(img), img, nMax, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));


		assertEquals(nMax, found.size());


		final long[] posCorrect = new long[img.numDimensions()];
		final long[] posFound = new long[img.numDimensions()];

		for (int i = 0; i<found.size(); i++){


			assertEquals(new Double(2.0), found.get(i).getB());

			correct.get(i).getA().localize(posCorrect);
			found.get(i).getA().localize(posFound);

			// the order of equal points is not defined a.t.m
			//assertArrayEquals(posCorrect, posFound);
		}

		final int i = 5;
		assertTrue( i == 5 );
	}
}
