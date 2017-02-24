package org.janelia.flatfield;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import net.imglib2.Cursor;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import scala.Tuple2;

public class ShiftedDownsampling
{
	private final JavaSparkContext sparkContext;
	private final Interval workingInterval;
	private final AffineTransform3D downsamplingTransform;

	private final List< long[] > scaleLevelPixelSize;
	private final List< long[] > scaleLevelOffset;
	private final List< long[] > scaleLevelDimensions;

	public ShiftedDownsampling(
			final JavaSparkContext sparkContext,
			final Interval workingInterval,
			final AffineTransform3D downsamplingTransform )
	{
		this.sparkContext = sparkContext;
		this.workingInterval = workingInterval;
		this.downsamplingTransform = downsamplingTransform;

		scaleLevelPixelSize = new ArrayList<>();
		scaleLevelOffset = new ArrayList<>();
		scaleLevelDimensions = new ArrayList<>();
		long smallestDimension;
		do
		{
			final int scale = scaleLevelDimensions.size();
			final long[]
					pixelSize 		= new long[ workingInterval.numDimensions() ],
					offset 			= new long[ workingInterval.numDimensions() ],
					downsampledSize = new long[ workingInterval.numDimensions() ];
			for ( int d = 0; d < downsampledSize.length; d++ )
			{
				// how many original pixels form a single pixel on this scale level
				pixelSize[ d ] = ( long ) ( scale == 0 ? 1 : scaleLevelPixelSize.get( scale - 1 )[ d ] / downsamplingTransform.get( d, d ) );

				// how many original pixels form the leftmost pixel on this scale level (which could be different from pixelSize[d] because of the translation component)
				offset[ d ] = ( long ) ( scale == 0 ? 0 : Math.min( pixelSize[ d ] * downsamplingTransform.get( d, 4 ) + pixelSize[ d ], workingInterval.dimension( d ) ) );

				// how many original pixels form the rightmost pixel on this scale level
				final long remaining = workingInterval.dimension( d ) - offset[ d ];

				// how many downsampled pixels are on this scale level
				downsampledSize[ d ] = ( offset[ d ] == 0 ? 0 : 1 ) + remaining / pixelSize[ d ] + ( remaining % pixelSize[ d ] == 0 ? 0 : 1 );
			}

			scaleLevelPixelSize.add( pixelSize );
			scaleLevelOffset.add( offset );
			scaleLevelDimensions.add( downsampledSize );

			smallestDimension = Long.MAX_VALUE;
			for ( int d = 0; d < workingInterval.numDimensions(); d++ )
				smallestDimension = Math.min( downsampledSize[ d ], smallestDimension );
		}
		while ( smallestDimension > 1 );

		System.out.println( "Scale level to dimensions:" );
		for ( int i = 0; i < scaleLevelDimensions.size(); i++ )
			System.out.println( String.format( " %d: %s", i, Arrays.toString( scaleLevelDimensions.get( i ) ) ) );
	}

	public int getNumScales()
	{
		return scaleLevelDimensions.size();
	}


	public JavaPairRDD< Long, long[] > downsampleHistograms(
			final JavaPairRDD< Long, long[] > rddHistograms,
			final PixelsMapping pixelsMapping )
	{
		if ( pixelsMapping.scale == 0 )
			return rddHistograms;

		final Broadcast< int[] > broadcastedFullPixelToDownsampledPixel = pixelsMapping.broadcastedFullPixelToDownsampledPixel;
		return rddHistograms
				.mapToPair( tuple -> new Tuple2<>( ( long ) broadcastedFullPixelToDownsampledPixel.value()[ tuple._1().intValue() ], tuple._2() ) )
				.reduceByKey(
						( ret, other ) ->
						{
							for ( int i = 0; i < ret.length; i++ )
								ret[ i ] += other[ i ];
							return ret;
						} );
	}

	@SuppressWarnings("unchecked")
	public < T extends NativeType< T > & RealType< T > > RandomAccessibleInterval< T > downsampleSolutionComponent(
			final RandomAccessibleInterval< T > fullComponent,
			final PixelsMapping pixelsMapping )
	{
		if ( pixelsMapping.scale == 0 )
			return fullComponent;

		final long[] downsampledSize = pixelsMapping.getDimensions();
		final RandomAccessibleInterval< T > downsampledComponent = new ArrayImgFactory< T >().create( downsampledSize, Util.getTypeFromInterval( fullComponent ).createVariable() );
		final Cursor< T > downsampledComponentCursor = Views.iterable( downsampledComponent ).localizingCursor();
		final long[] downsampledPosition = new long[ downsampledComponent.numDimensions() ];
		while ( downsampledComponentCursor.hasNext() )
		{
			double fullPixelsSum = 0;
			final T downsampledVal = downsampledComponentCursor.next();

			downsampledComponentCursor.localize( downsampledPosition );
			final long downsampledPixel = IntervalIndexer.positionToIndex( downsampledPosition, downsampledSize );

			final IntervalView< T > fullComponentInterval = Views.interval( fullComponent, pixelsMapping.downsampledPixelToFullPixels.get( downsampledPixel ) );
			final Cursor< T > fullComponentIntervalCursor = Views.iterable( fullComponentInterval ).cursor();
			while ( fullComponentIntervalCursor.hasNext() )
				fullPixelsSum += fullComponentIntervalCursor.next().getRealDouble();

			downsampledVal.setReal( fullPixelsSum / fullComponentInterval.size() );
		}

		return downsampledComponent;
	}

	public < T extends RealType< T > & NativeType< T > > RandomAccessible< T > upsample( final RandomAccessibleInterval< T > downsampledImg, final int scale )
	{
		final RealRandomAccessible< T > interpolatedDownsampledImage = Views.interpolate( Views.extendBorder( downsampledImg ), new NLinearInterpolatorFactory<>() );

		// Preapply the transform with a positive translation in order to align the downsampled image to the upsampled image (in its coordinate space)
		final double[] translation = downsamplingTransform.getTranslation();
		for ( int d = 0; d < translation.length; d++ )
			translation[ d ] = scale > 0 ? -translation[ d ] : 0;
		final AffineTransform3D translationTransform = new AffineTransform3D();
		translationTransform.setTranslation( translation );

		final RealRandomAccessible< T > alignedDownsampledImg = RealViews.affine( interpolatedDownsampledImage, translationTransform );

		// Then, apply the inverse of the downsampling transform in order to map the downsampled image to the upsampled image
		return RealViews.affine( alignedDownsampledImg, downsamplingTransform.inverse() );
	}


	public class PixelsMapping implements Serializable, AutoCloseable
	{
		private static final long serialVersionUID = -4186213887570439705L;

		public final int scale;
		public final Map< Long, Interval > downsampledPixelToFullPixels;
		public final Broadcast< int[] > broadcastedFullPixelToDownsampledPixel;
		public final Broadcast< int[] > broadcastedDownsampledPixelToFullPixelsCount;

		@Override
		public void close()
		{
			if ( broadcastedFullPixelToDownsampledPixel != null && broadcastedFullPixelToDownsampledPixel.isValid() )
				broadcastedFullPixelToDownsampledPixel.destroy();

			if ( broadcastedDownsampledPixelToFullPixelsCount != null && broadcastedDownsampledPixelToFullPixelsCount.isValid() )
				broadcastedDownsampledPixelToFullPixelsCount.destroy();
		}

		public long[] getPixelSize()
		{
			return scaleLevelPixelSize.get( scale );
		}
		public long[] getOffset()
		{
			return scaleLevelOffset.get( scale );
		}
		public long[] getDimensions()
		{
			return scaleLevelDimensions.get( scale );
		}


		public PixelsMapping( final int scale )
		{
			this.scale = scale;

			final long[] pixelSize = getPixelSize();
			final long[] offset = getOffset();
			final long[] downsampledSize = getDimensions();

			final long[] fullDimensions = Intervals.dimensionsAsLongArray( workingInterval );

			if ( scale == 0 )
			{
				downsampledPixelToFullPixels = null;
				broadcastedFullPixelToDownsampledPixel = null;
				broadcastedDownsampledPixelToFullPixelsCount = null;
				return;
			}

			// First, generate the following mapping: downsampled pixel -> corresponding interval of full-scale pixels
			downsampledPixelToFullPixels = new HashMap<>();
			final long numDownsampledPixels = Intervals.numElements( new FinalDimensions( downsampledSize ) );
			final int[] downsampledPosition = new int[ downsampledSize.length ];
			for ( long downsampledPixel = 0; downsampledPixel < numDownsampledPixels; downsampledPixel++ )
			{
				IntervalIndexer.indexToPosition( downsampledPixel, downsampledSize, downsampledPosition );
				final long[] mins = new long[ fullDimensions.length ], maxs = new long[ fullDimensions.length ];
				for ( int d = 0; d < mins.length; d++ )
				{
					if ( downsampledPosition[ d ] == 0 )
					{
						mins[ d ] = 0;
						maxs[ d ] = offset[ d ] - 1;
					}
					else
					{
						mins[ d ] = offset[ d ] + pixelSize[ d ] * ( downsampledPosition[ d ] - 1 );
						maxs[ d ] = Math.min( mins[ d ] + pixelSize[ d ], fullDimensions[ d ] ) - 1;
					}
				}
				final Interval srcInterval = new FinalInterval( mins, maxs );
				downsampledPixelToFullPixels.put( downsampledPixel, srcInterval );
			}

			// Next, generate some additional mappings:
			// 1) full pixel -> corresponding downsampled pixel
			// 2) downsampled pixel -> size of the corresponding interval of full-scale pixels
			final int[] fullPixelToDownsampledPixel = new int[ ( int ) Intervals.numElements( workingInterval ) ];
			final int[] downsampledPixelToFullPixelsCount = new int[ downsampledPixelToFullPixels.size() ];
			for ( final Entry< Long, Interval > entry : downsampledPixelToFullPixels.entrySet() )
			{
				final long downsampledPixel = entry.getKey();
				final long[] mins = Intervals.minAsLongArray( entry.getValue() );
				final long[] maxs = Intervals.maxAsLongArray( entry.getValue() );

				final long[] position = mins.clone();
				final int n = entry.getValue().numDimensions();
				for ( int d = 0; d < n; )
				{
					fullPixelToDownsampledPixel[ ( int ) IntervalIndexer.positionToIndex( position, fullDimensions ) ] = ( int ) downsampledPixel;

					for ( d = 0; d < n; ++d )
					{
						position[ d ]++;
						if ( position[ d ] <= maxs[ d ] )
							break;
						else
							position[ d ] = mins[ d ];
					}
				}

				downsampledPixelToFullPixelsCount[ ( int ) downsampledPixel ] = ( int ) Intervals.numElements( entry.getValue() );
			}

			broadcastedFullPixelToDownsampledPixel = sparkContext.broadcast( fullPixelToDownsampledPixel );
			broadcastedDownsampledPixelToFullPixelsCount = sparkContext.broadcast( downsampledPixelToFullPixelsCount );
		}
	}
}
