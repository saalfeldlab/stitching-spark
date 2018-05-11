package org.janelia.stitching;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import org.janelia.dataaccess.DataProvider;
import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;

import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealConverter;
import net.imglib2.interpolation.randomaccess.ClampingNLinearInterpolatorFactory;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

public class TransformedTileImageLoader
{
	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final Optional< RandomAccessiblePairNullable< U, U > > flatfield ) throws IOException
	{
		return loadTile(
				tile,
				dataProvider,
				flatfield,
				new FinalInterval( tile.getSize() )
			);
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final Optional< RandomAccessiblePairNullable< U, U > > flatfield,
			final Interval tileInterval ) throws IOException
	{
		return loadTile(
				tile,
				dataProvider,
				flatfield,
				tileInterval,
				TileOperations.getTileTransform( tile )
			);
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final Optional< RandomAccessiblePairNullable< U, U > > flatfield,
			final InvertibleRealTransform tileTransform ) throws IOException
	{
		return loadTile(
				tile,
				dataProvider,
				flatfield,
				new FinalInterval( tile.getSize() ),
				tileTransform
			);
	}

	public static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final Optional< RandomAccessiblePairNullable< U, U > > flatfield,
			final Interval tileInterval,
			final InvertibleRealTransform tileTransform ) throws IOException
	{
		final RandomAccessibleInterval< T > rawImg = TileLoader.loadTile( tile, dataProvider );
		final RandomAccessibleInterval< T > source;
		if ( flatfield.isPresent() )
		{
			final FlatfieldCorrectedRandomAccessible< T, U > flatfieldCorrectedImg = new FlatfieldCorrectedRandomAccessible<>( rawImg, flatfield.get().toRandomAccessiblePair() );
			final RandomAccessible< T > correctedConvertedRandomAccessible = Converters.convert( flatfieldCorrectedImg, new RealConverter<>(), Util.getTypeFromInterval( rawImg ) );
			source = Views.interval( correctedConvertedRandomAccessible, tileInterval );
		}
		else
		{
			source = Views.interval( rawImg, tileInterval );
		}

		final RandomAccessible< T > extendedSource = Views.extendZero( source );
		final RealRandomAccessible< T > interpolatedSource = Views.interpolate( extendedSource, new ClampingNLinearInterpolatorFactory<>() );
		final RandomAccessible< T > transformedSource = RealViews.transform( interpolatedSource, tileTransform );
		final Interval boundingBox = getTransformedBoundingBox( source, tileTransform );
		final RandomAccessibleInterval< T > transformedImg = Views.interval( transformedSource, boundingBox );

		return transformedImg;
	}

	public static Interval getTransformedBoundingBox( final RealInterval interval, final InvertibleRealTransform transform )
	{
		return Intervals.smallestContainingInterval( getTransformedBoundingBoxReal( interval, transform ) );
	}

	public static RealInterval getTransformedBoundingBoxReal( final RealInterval interval, final InvertibleRealTransform transform )
	{
		final double[] transformedMin = new double[ interval.numDimensions() ], transformedMax = new double[ interval.numDimensions() ];
		Arrays.fill( transformedMin, Double.POSITIVE_INFINITY );
		Arrays.fill( transformedMax, Double.NEGATIVE_INFINITY );

		final double[] cornerPosition = new double[ interval.numDimensions() ], transformedCornerPosition = new double[ interval.numDimensions() ];

		final int[] cornerDimensions = new int[ interval.numDimensions() ];
		Arrays.fill( cornerDimensions, 2 );
		final IntervalIterator cornerIterator = new IntervalIterator( cornerDimensions );

		while ( cornerIterator.hasNext() )
		{
			cornerIterator.fwd();
			for ( int d = 0; d < interval.numDimensions(); ++d )
				cornerPosition[ d ] = cornerIterator.getIntPosition( d ) == 0 ? interval.realMin( d ) : interval.realMax( d );

			transform.apply( cornerPosition, transformedCornerPosition );

			for ( int d = 0; d < interval.numDimensions(); ++d )
			{
				transformedMin[ d ] = Math.min( transformedCornerPosition[ d ], transformedMin[ d ] );
				transformedMax[ d ] = Math.max( transformedCornerPosition[ d ], transformedMax[ d ] );
			}
		}

		return new FinalRealInterval( transformedMin, transformedMax );
	}
}
