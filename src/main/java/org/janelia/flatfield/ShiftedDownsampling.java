package org.janelia.flatfield;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.histogram.Histogram;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;

import net.imglib2.Cursor;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.list.ListCursor;
import net.imglib2.img.list.WrappedListImg;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsNullable;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class ShiftedDownsampling< A extends AffineGet & AffineSet >
{
	private final JavaSparkContext sparkContext;
	private final Interval workingInterval;
	private final A downsamplingTransform;

	private final List< long[] > scaleLevelPixelSize;
	private final List< long[] > scaleLevelOffset;
	private final List< long[] > scaleLevelDimensions;

	public ShiftedDownsampling(
			final Interval workingInterval,
			final A downsamplingTransform )
	{
		// The version that doesn't use Spark
		this( null, workingInterval, downsamplingTransform );
	}
	public ShiftedDownsampling(
			final JavaSparkContext sparkContext,
			final Interval workingInterval,
			final A downsamplingTransform )
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
				offset[ d ] = ( long ) ( scale == 0 ? 0 : Math.min( pixelSize[ d ] * downsamplingTransform.get( d, downsamplingTransform.numDimensions() ) + pixelSize[ d ], workingInterval.dimension( d ) ) );

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

	public long[] getDimensionsAtScale( final int scale )
	{
		return scaleLevelDimensions.get( scale );
	}


	/**
	 * Downsamples histograms as an N5 dataset with respect to the given pixel mapping (the mapping between input and output accumulated pixels).
	 * Returns the path to the dataset where the resulting downsampled histograms are stored (within the same N5 container).
	 *
	 * @param histogramsProvider
	 * @param pixelsMapping
	 * @return path to downsampled histograms dataset
	 * @throws IOException
	 */
	public String downsampleHistogramsN5(
			final PixelsMapping pixelsMapping,
			final DataProvider dataProvider,
			final String histogramsN5BasePath, final String histogramsDataset,
			final double histMinValue, final double histMaxValue, final int bins ) throws IOException
	{
		if ( pixelsMapping.scale == 0 )
			return histogramsDataset;

		final Broadcast< int[] > broadcastedFullPixelToDownsampledPixel = pixelsMapping.broadcastedFullPixelToDownsampledPixel;
		final Broadcast< int[] > broadcastedDownsampledPixelToFullPixelsCount = pixelsMapping.broadcastedDownsampledPixelToFullPixelsCount;

		// additionally broadcast the downsampled pixel to full pixels mapping
		final Broadcast< Map< Long, Interval > > broadcastedDownsampledPixelToFullPixels = sparkContext.broadcast( pixelsMapping.downsampledPixelToFullPixels );

		final String downsampledHistogramsDataset = histogramsDataset + "-downsampled";

		final DataAccessType dataAccessType = dataProvider.getType();
		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		final DatasetAttributes histogramsDatasetAttributes = n5.getDatasetAttributes( histogramsDataset );

		final long[] histogramsDimensions = histogramsDatasetAttributes.getDimensions();
		final int [] blockSize = histogramsDatasetAttributes.getBlockSize();
		final long[] downsampledHistogramsDimensions = pixelsMapping.getDimensions();
		final int[] downsampledBlockSize = histogramsDatasetAttributes.getBlockSize();

		n5.createDataset(
				downsampledHistogramsDataset,
				downsampledHistogramsDimensions,
				downsampledBlockSize,
				histogramsDatasetAttributes.getDataType(),
				histogramsDatasetAttributes.getCompression()
			);

		final List< long[] > downsampledBlockPositions = HistogramsProvider.getBlockPositions( downsampledHistogramsDimensions, downsampledBlockSize );
		sparkContext.parallelize( downsampledBlockPositions, downsampledBlockPositions.size() ).foreach( downsampledBlockPosition ->
			{
				final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataAccessType );
				final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsN5BasePath ) );
				final WrappedSerializableDataBlockWriter< Histogram > downsampledHistogramsBlock = new WrappedSerializableDataBlockWriter<>(
						n5Local,
						downsampledHistogramsDataset,
						downsampledBlockPosition
					);

				if ( downsampledHistogramsBlock.wasLoadedSuccessfully() )
					throw new RuntimeException( "previous downsampled histograms dataset was not properly cleaned up" );

				// find the interval in full resolution pixels that this downsampled block maps into
				final CellGrid downsampledCellGrid = new CellGrid( downsampledHistogramsDimensions, downsampledBlockSize );
				final long[] downsampledCellMin = new long[ downsampledCellGrid.numDimensions() ], downsampledCellMax = new long[ downsampledCellGrid.numDimensions() ];
				final int[] downsampledCellDimensions = new int[ downsampledCellGrid.numDimensions() ];
				downsampledCellGrid.getCellDimensions( downsampledBlockPosition, downsampledCellMin, downsampledCellDimensions );
				for ( int d = 0; d < downsampledCellGrid.numDimensions(); ++d )
					downsampledCellMax[ d ] = downsampledCellMin[ d ] + downsampledCellDimensions[ d ] - 1;

				final long[] downsampledBlockToFullPixelsMin = new long[ downsampledCellGrid.numDimensions() ], downsampledBlockToFullPixelsMax = new long[ downsampledCellGrid.numDimensions() ];
				Arrays.fill( downsampledBlockToFullPixelsMin, Long.MAX_VALUE );
				Arrays.fill( downsampledBlockToFullPixelsMax, Long.MIN_VALUE );

				final Interval downsampledCellInterval = new FinalInterval( downsampledCellMin, downsampledCellMax );
				final IntervalIterator downsampledCellIntervalIterator = new IntervalIterator( downsampledCellInterval );
				final long[] downsampledPosition = new long[ downsampledCellGrid.numDimensions() ];

				// TODO: this can be optimized by only checking the corners of downsampledCellInterval
				while ( downsampledCellIntervalIterator.hasNext() )
				{
					downsampledCellIntervalIterator.fwd();
					downsampledCellIntervalIterator.localize( downsampledPosition );
					final long downsampledPixelIndex = IntervalIndexer.positionToIndex( downsampledPosition, downsampledHistogramsDimensions );
					final Interval fullPixelsInterval = broadcastedDownsampledPixelToFullPixels.value().get( downsampledPixelIndex );
					for ( int d = 0; d < downsampledCellGrid.numDimensions(); ++d )
					{
						downsampledBlockToFullPixelsMin[ d ] = Math.min( fullPixelsInterval.min( d ), downsampledBlockToFullPixelsMin[ d ] );
						downsampledBlockToFullPixelsMax[ d ] = Math.max( fullPixelsInterval.max( d ), downsampledBlockToFullPixelsMax[ d ] );
					}
				}
				final Interval downsampledBlockToFullPixelsInterval = new FinalInterval( downsampledBlockToFullPixelsMin, downsampledBlockToFullPixelsMax );

				// wrap the downsampled histograms block
				final WrappedListImg< Histogram > downsampledHistogramsBlockImg = downsampledHistogramsBlock.wrap();
				// initialize all histograms
				final ListCursor< Histogram > downsampledHistogramsBlockImgCursor = downsampledHistogramsBlockImg.cursor();
				while ( downsampledHistogramsBlockImgCursor.hasNext() )
				{
					downsampledHistogramsBlockImgCursor.fwd();
					downsampledHistogramsBlockImgCursor.set( new Histogram( histMinValue, histMaxValue, bins ) );
				}
				// translate the block view to its correct position in the downsampled histograms space
				final IntervalView< Histogram > translatedDownsampledHistogramsBlockImg = Views.translate( downsampledHistogramsBlockImg, downsampledCellMin );
				final RandomAccess< Histogram > downsampledHistogramsRandomAccess = translatedDownsampledHistogramsBlockImg.randomAccess();

				// find a set of data blocks in original resolution with all pixels required to be accumulated in this downsampled block
				final CellGrid cellGrid = new CellGrid( histogramsDimensions, blockSize );
				final long[] cellGridDimensions = cellGrid.getGridDimensions();
				final long numCells = Intervals.numElements( cellGridDimensions );
				final long[] cellMin = new long[ cellGrid.numDimensions() ], cellMax = new long[ cellGrid.numDimensions() ];
				final int[] cellDimensions = new int[ cellGrid.numDimensions() ];
				final long[] cellGridPosition = new long[ cellGrid.numDimensions() ], pixelPosition = new long[ cellGrid.numDimensions() ];
				for ( long cellIndex = 0; cellIndex < numCells; ++cellIndex )
				{
					cellGrid.getCellDimensions( cellIndex, cellMin, cellDimensions );
					for ( int d = 0; d < cellGrid.numDimensions(); ++d )
						cellMax[ d ] = cellMin[ d ] + cellDimensions[ d ] - 1;
					final Interval cellPixelsInterval = new FinalInterval( cellMin, cellMax );

					final Interval fullPixelsInDownsampledBlockInterval = IntervalsNullable.intersect( cellPixelsInterval, downsampledBlockToFullPixelsInterval );
					if ( fullPixelsInDownsampledBlockInterval != null )
					{
						cellGrid.getCellGridPositionFlat( cellIndex, cellGridPosition );
						final WrappedSerializableDataBlockReader< Histogram > histogramsBlock = new WrappedSerializableDataBlockReader<>(
								n5Local,
								histogramsDataset,
								cellGridPosition
							);

						final WrappedListImg< Histogram > histogramsBlockImg = histogramsBlock.wrap();
						final IntervalView< Histogram > translatedHistogramsBlockImg = Views.translate( histogramsBlockImg, cellMin );
						final IntervalView< Histogram > histogramsBlockIntersection = Views.interval( translatedHistogramsBlockImg, fullPixelsInDownsampledBlockInterval );
						final Cursor< Histogram > histogramsBlockIntersectionCursor = Views.iterable( histogramsBlockIntersection ).localizingCursor();
						while ( histogramsBlockIntersectionCursor.hasNext() )
						{
							final Histogram histogram = histogramsBlockIntersectionCursor.next();
							histogramsBlockIntersectionCursor.localize( pixelPosition );
							final long pixelIndex = IntervalIndexer.positionToIndex( pixelPosition, histogramsDimensions );
							final long downsampledPixelIndex = broadcastedFullPixelToDownsampledPixel.value()[ ( int ) pixelIndex ];
							IntervalIndexer.indexToPosition( downsampledPixelIndex, downsampledHistogramsDimensions, downsampledPosition );
							downsampledHistogramsRandomAccess.setPosition( downsampledPosition );
							final Histogram accumulatedHistogram = downsampledHistogramsRandomAccess.get();
							accumulatedHistogram.add( histogram );
						}
					}
				}

				// average all accumulated histograms
				downsampledCellIntervalIterator.reset();
				while ( downsampledCellIntervalIterator.hasNext() )
				{
					downsampledCellIntervalIterator.fwd();
					downsampledCellIntervalIterator.localize( downsampledPosition );
					downsampledHistogramsRandomAccess.setPosition( downsampledPosition );
					final Histogram accumulatedHistogram = downsampledHistogramsRandomAccess.get();
					final long downsampledPixelIndex = IntervalIndexer.positionToIndex( downsampledPosition, downsampledHistogramsDimensions );
					final int accumulatedHistogramsCount = broadcastedDownsampledPixelToFullPixelsCount.value()[ ( int ) downsampledPixelIndex ];
					accumulatedHistogram.average( accumulatedHistogramsCount );
				}

				downsampledHistogramsBlock.save();
			} );

		broadcastedDownsampledPixelToFullPixels.destroy();

		return downsampledHistogramsDataset;
	}

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

	@SuppressWarnings("unchecked")
	public < T extends RealType< T > & NativeType< T > > RealRandomAccessible< T > upsample( final RandomAccessibleInterval< T > downsampledImg, final int newScale )
	{
		// find the scale level of downsampledImg by comparing the dimensions
		int oldScale = -1;
		for ( int scale = getNumScales() - 1; scale >= 0; scale-- )
		{
			boolean found = true;
			for ( int d = 0; d < downsampledImg.numDimensions(); d++ )
				found &= downsampledImg.dimension( d ) == scaleLevelDimensions.get( scale )[ d ];

			if ( found )
			{
				oldScale = scale;
				break;
			}
		}
		if ( oldScale == -1 )
			throw new IllegalArgumentException( "Cannot identify scale level of the given image" );

		RealRandomAccessible< T > img = Views.interpolate( Views.extendBorder( downsampledImg ), new NLinearInterpolatorFactory<>() );
		for ( int scale = oldScale - 1; scale >= newScale; scale-- )
		{
			// Preapply the shifting transform in order to align the downsampled image to the upsampled image (in its coordinate space)
			final A translationTransform = ( A ) downsamplingTransform.copy();
			for ( int d = 0; d < translationTransform.numDimensions(); d++ )
			{
				translationTransform.set( 1, d, d );
				if ( scale == 0 )
					translationTransform.set( 1.5 * translationTransform.get(
							d, translationTransform.numDimensions() ),
							d, translationTransform.numDimensions() );
			}

			final RealRandomAccessible< T > alignedDownsampledImg = RealViews.affine( img, translationTransform );

			// Then, apply the inverse of the downsampling transform in order to map the downsampled image to the upsampled image
			img = RealViews.affine( alignedDownsampledImg, downsamplingTransform.inverse() );
		}
		return img;
	}


	public class PixelsMapping implements AutoCloseable
	{
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

			broadcastedFullPixelToDownsampledPixel = sparkContext != null ? sparkContext.broadcast( fullPixelToDownsampledPixel ) : null;
			broadcastedDownsampledPixelToFullPixelsCount = sparkContext != null ? sparkContext.broadcast( downsampledPixelToFullPixelsCount ) : null;
		}
	}
}
