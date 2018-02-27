package org.janelia.flatfield;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.CloudURI;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileLoader;
import org.janelia.stitching.TileLoader.TileType;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.histogram.Real1dBinMapper;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class HistogramsProvider implements Serializable
{
	private static final long serialVersionUID = 2090264857259429741L;

	private static final double REFERENCE_HISTOGRAM_POINTS_PERCENT = 0.25;
	private static final int HISTOGRAMS_DEFAULT_BLOCK_SIZE = 64;
	private static final String HISTOGRAMS_N5_DATASET_NAME = "histograms-n5";

	private static final String ALL_HISTOGRAMS_EXIST_KEY = "allHistogramsExist";
	private static final String HISTOGRAM_MIN_VALUE_KEY = "histogramMinValue";
	private static final String HISTOGRAM_MAX_VALUE_KEY = "histogramMaxValue";
	private static final String HISTOGRAM_NUM_BINS_KEY = "histogramNumBins";
	private static final String REFERENCE_HISTOGRAM_KEY = "referenceHistogram";

	private static final int MAX_PARTITIONS = 15000;

	private transient final JavaSparkContext sparkContext;
	private transient final DataProvider dataProvider;
	private transient final TileInfo[] tiles;
	private transient final Interval workingInterval;

	private final DataAccessType dataAccessType;

	private final String histogramsN5BasePath;
	private final String histogramsDataset;

	private final double histMinValue, histMaxValue;
	private final int bins;

	private final long[] fieldOfViewSize;
	private final int[] blockSize;

	private double[] referenceHistogram;

	public HistogramsProvider(
			final JavaSparkContext sparkContext,
			final DataProvider dataProvider,
			final Interval workingInterval,
			final String basePath,
			final TileInfo[] tiles,
			final long[] fullTileSize,
			final double histMinValue, final double histMaxValue, final int innerBins ) throws IOException, URISyntaxException
	{
		this.sparkContext = sparkContext;
		this.dataProvider = dataProvider;
		this.workingInterval = workingInterval;
		this.tiles = tiles;

		this.histMinValue = histMinValue;
		this.histMaxValue = histMaxValue;
		this.bins = innerBins + 2; // add two extra bins to store tails of the distribution

		dataAccessType = dataProvider.getType();

		if ( dataAccessType == DataAccessType.FILESYSTEM )
		{
			histogramsN5BasePath = basePath;
			histogramsDataset = HISTOGRAMS_N5_DATASET_NAME;
		}
		else
		{
			final CloudURI cloudUri = new CloudURI( URI.create( basePath ) );
			histogramsN5BasePath = DataProviderFactory.createBucketUri( cloudUri.getType(), cloudUri.getBucket() ).toString();
			histogramsDataset = PathResolver.get( cloudUri.getKey(), HISTOGRAMS_N5_DATASET_NAME );
		}

		// set field of view size and block size
		// check if tiles are single image files, or N5 datasets
		final TileType tileType = TileLoader.getTileType( tiles[ 0 ], dataProvider );
		// TODO: check that all tiles are of the same type

		final boolean use2D = workingInterval.numDimensions() < fullTileSize.length;

		fieldOfViewSize = use2D ? new long[] { fullTileSize[ 0 ], fullTileSize[ 1 ] } : fullTileSize.clone();
		blockSize = new int[ fieldOfViewSize.length ];
		if ( tileType == TileType.N5_DATASET )
		{
			final int[] tileBlockSize = TileLoader.getTileN5DatasetAttributes( tiles[ 0 ], dataProvider ).getBlockSize();
			System.arraycopy( tileBlockSize, 0, blockSize, 0, blockSize.length );
		}
		else if ( tileType == TileType.IMAGE_FILE )
		{
			Arrays.fill( blockSize, HISTOGRAMS_DEFAULT_BLOCK_SIZE );
		}
		else
		{
			throw new NotImplementedException( "Backend storage not supported for tiles: " + tileType );
		}

		// reduce memory requirement by shrinking block size for the histogram dataset
		while (
				( workingInterval.numDimensions() == 2 && Arrays.stream( blockSize ).min().getAsInt() > 128 ) ||
				( workingInterval.numDimensions() == 3 && Arrays.stream( blockSize ).min().getAsInt() > 32 ) )
		{
			for ( int d = 0; d < blockSize.length; ++d )
				blockSize[ d ] >>= 1;
		}

		if ( !use2D && sliceHistogramsExist() )
		{
			// if the histograms are stored in the old format, convert them to the new N5 format first
//			convertHistogramsToN5();
			throw new NotImplementedException( "conversion to n5 histograms is not implemented yet" );
		}
		else
		{
			populateHistogramsN5();
		}
	}

	public DataProvider getDataProvider() { return dataProvider; }
	public DataAccessType getDataAccessType() { return dataAccessType; }
	public Interval getWorkingInterval() { return workingInterval; };

	public String getHistogramsN5BasePath() { return histogramsN5BasePath; }
	public String getHistogramsDataset() { return histogramsDataset; }

	public double getHistogramMinValue() { return histMinValue; }
	public double getHistogramMaxValue() { return histMaxValue; }
	public int getHistogramBins() { return bins; }

	private < T extends NativeType< T > & RealType< T >, R extends RealType< R > > void populateHistogramsN5() throws IOException, URISyntaxException
	{
		System.out.println( "Binning the input stack and saving as N5 blocks..." );

		final long[] extendedDimensions = new long[ fieldOfViewSize.length + 1 ];
		System.arraycopy( fieldOfViewSize, 0, extendedDimensions, 0, fieldOfViewSize.length );
		extendedDimensions[ fieldOfViewSize.length ] = bins;

		final int[] extendedBlockSize = new int[ blockSize.length + 1 ];
		System.arraycopy( blockSize, 0, extendedBlockSize, 0, blockSize.length );
		extendedBlockSize[ blockSize.length ] = bins;

		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		if ( !n5.datasetExists( histogramsDataset ) )
		{
			n5.createDataset(
					histogramsDataset,
					extendedDimensions,
					extendedBlockSize,
					DataType.FLOAT64,
					new GzipCompression()
				);
		}
		else
		{
			// validate existing histograms
			if ( n5.getDatasetAttributes( histogramsDataset ).getNumDimensions() != extendedDimensions.length )
				throw new RuntimeException( "histograms-n5 has different dimensionality than the field of view" );

			if (
					!Util.isApproxEqual( n5.getAttribute( histogramsDataset, HISTOGRAM_MIN_VALUE_KEY, Double.class ), histMinValue, 1e-10 ) ||
					!Util.isApproxEqual( n5.getAttribute( histogramsDataset, HISTOGRAM_MAX_VALUE_KEY, Double.class ), histMaxValue, 1e-10 ) )
				throw new RuntimeException( "histograms-n5 has different value range" );

			if ( n5.getAttribute( histogramsDataset, HISTOGRAM_NUM_BINS_KEY, Integer.class ) != bins )
				throw new RuntimeException( "histograms-n5 has different number of bins" );

			// skip this step if the flag 'allHistogramsExist' is set
			final Boolean allHistogramsExist = n5.getAttribute( histogramsDataset, ALL_HISTOGRAMS_EXIST_KEY, Boolean.class );
			if ( allHistogramsExist != null && allHistogramsExist )
				return;
		}

		final Map< String, Object > histogramAttributes = new HashMap<>();
		histogramAttributes.put( HISTOGRAM_MIN_VALUE_KEY, new Double( histMinValue ) );
		histogramAttributes.put( HISTOGRAM_MAX_VALUE_KEY, new Double( histMaxValue ) );
		histogramAttributes.put( HISTOGRAM_NUM_BINS_KEY, new Integer( bins ) );
		n5.setAttributes( histogramsDataset, histogramAttributes );

		final Broadcast< TileInfo[] > broadcastedTiles = sparkContext.broadcast( tiles );

		final List< long[] > blockPositions = getBlockPositions( fieldOfViewSize, blockSize );
		sparkContext.parallelize( blockPositions, Math.min( blockPositions.size(), MAX_PARTITIONS ) ).foreach( blockPosition ->
			{
				// create correct block interval including the 'bins' dimension
				final long[] extendedBlockPosition = new long[ extendedBlockSize.length ];
				System.arraycopy( blockPosition, 0, extendedBlockPosition, 0, blockPosition.length );
				final CellGrid extendedCellGrid = new CellGrid( extendedDimensions, extendedBlockSize );
				final long[] extendedCellMin = new long[ extendedCellGrid.numDimensions() ], extendedCellMax = new long[ extendedCellGrid.numDimensions() ];
				final int[] extendedCellDimensions = new int[ extendedCellGrid.numDimensions() ];
				extendedCellGrid.getCellDimensions( extendedBlockPosition, extendedCellMin, extendedCellDimensions );
				for ( int d = 0; d < extendedCellGrid.numDimensions(); ++d )
					extendedCellMax[ d ] = extendedCellMin[ d ] + extendedCellDimensions[ d ] - 1;
				final Interval extendedBlockInterval = new FinalInterval( extendedCellMin, extendedCellMax );

				// create histogram block
				final RandomAccessibleInterval< DoubleType > histogramsStorageBlockImg = ArrayImgs.doubles( Intervals.dimensionsAsLongArray( extendedBlockInterval ) );
				final RandomAccessibleInterval< R > histogramsGenericStorageBlockImg = ( RandomAccessibleInterval< R > ) histogramsStorageBlockImg;
				final RandomAccessibleInterval< RealComposite< R > > histogramsBlockImg = Views.collapseReal( histogramsGenericStorageBlockImg );
				final Real1dBinMapper< R > binMapper = new Real1dBinMapper<>( histMinValue, histMaxValue, bins, true );

				// create an interval to be processed in each tile image
				final CellGrid cellGrid = new CellGrid( fieldOfViewSize, blockSize );
				final long[] cellMin = new long[ cellGrid.numDimensions() ], cellMax = new long[ cellGrid.numDimensions() ];
				final int[] cellDimensions = new int[ cellGrid.numDimensions() ];
				cellGrid.getCellDimensions( blockPosition, cellMin, cellDimensions );
				for ( int d = 0; d < cellGrid.numDimensions(); ++d )
					cellMax[ d ] = cellMin[ d ] + cellDimensions[ d ] - 1;
				final Interval blockInterval = new FinalInterval( cellMin, cellMax );

				final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataAccessType );

				// loop over tile images and populate the histograms using the corresponding part of each tile image
				int done = 0;
				for ( final TileInfo tile : broadcastedTiles.value() )
				{
					final RandomAccessibleInterval< T > tileStorageImg = TileLoader.loadTile( tile, dataProviderLocal );
					final Interval tileImgOffsetInterval;
					if ( tileStorageImg.numDimensions() == 3 )
					{
						tileImgOffsetInterval = new FinalInterval(
								new long[] { blockInterval.min( 0 ), blockInterval.min( 1 ), blockInterval.numDimensions() >= 3 ? blockInterval.min( 2 ) : tileStorageImg.min( 2 ) },
								new long[] { blockInterval.max( 0 ), blockInterval.max( 1 ), blockInterval.numDimensions() >= 3 ? blockInterval.max( 2 ) : tileStorageImg.max( 2 ) }
							);
					}
					else
					{
						tileImgOffsetInterval = new FinalInterval(
								new long[] { blockInterval.min( 0 ), blockInterval.min( 1 ) },
								new long[] { blockInterval.max( 0 ), blockInterval.max( 1 ) }
							);
					}

					final RandomAccessibleInterval< T > tileStorageImgInterval = Views.offsetInterval( tileStorageImg, tileImgOffsetInterval );
					final Cursor< RealComposite< R > > histogramsBlockImgCursor = Views.flatIterable( histogramsBlockImg ).cursor();

					if ( tileStorageImg.numDimensions() == blockInterval.numDimensions() )
					{
						// handles the following cases:
						// 1) FoV is 2D, tile is 2D
						// 2) FoV is 3D, tile is 3D
						final RandomAccessibleInterval< T > tileImgInterval = tileStorageImgInterval;
						final Cursor< T > tileCursor = Views.flatIterable( tileImgInterval ).cursor();
						while ( histogramsBlockImgCursor.hasNext() || tileCursor.hasNext() )
						{
							final RealComposite< R > histogram = histogramsBlockImgCursor.next();
							final T value = tileCursor.next();
							final long bin = binMapper.map( ( R ) value );
							histogram.get( bin ).inc();
						}
					}
					else
					{
						final RandomAccessibleInterval< RealComposite< T > > tileImgCompositeInterval = Views.collapseReal( tileStorageImgInterval );
						final Cursor< RealComposite< T > > tileCompositeCursor = Views.flatIterable( tileImgCompositeInterval ).cursor();

						// handles the following cases:
						// 3) FoV is 2D, tile is 3D (last dimension in tile space is collapsed and used as additional data points)
						while ( histogramsBlockImgCursor.hasNext() || tileCompositeCursor.hasNext() )
						{
							final RealComposite< R > histogram = histogramsBlockImgCursor.next();
							final RealComposite< T > compositeValue = tileCompositeCursor.next();
							for ( final T value : compositeValue )
							{
								final long bin = binMapper.map( ( R ) value );
								histogram.get( bin ).inc();
							}
						}
					}

					if ( ++done % 20 == 0 )
						System.out.println( "Block min=" + Arrays.toString( Intervals.minAsLongArray( blockInterval ) ) + ", max=" + Arrays.toString( Intervals.maxAsLongArray( blockInterval ) ) + ": processed " + done + " tiles" );
				}

				System.out.println( "Block min=" + Arrays.toString( Intervals.minAsLongArray( blockInterval ) ) + ", max=" + Arrays.toString( Intervals.maxAsLongArray( blockInterval ) ) + ": populated histograms" );

				final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsN5BasePath ) );
				N5Utils.saveBlock( histogramsStorageBlockImg, n5Local, histogramsDataset, extendedBlockPosition );
			} );

		broadcastedTiles.destroy();

		// mark all histograms as ready to skip block existence check and save time for subsequent runs
		n5.setAttribute( histogramsDataset, ALL_HISTOGRAMS_EXIST_KEY, true );
	}

	public double[] getReferenceHistogram() throws IOException
	{
		if ( referenceHistogram == null )
		{
			// try to load cached reference histogram from the attributes
			final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
			final double[] referenceHistogramAttribute = n5.getAttribute( histogramsDataset, REFERENCE_HISTOGRAM_KEY, double[].class );
			if ( referenceHistogramAttribute != null )
			{
				if ( referenceHistogramAttribute.length != bins )
					throw new RuntimeException( "reference histogram has different number of bins" );

				referenceHistogram = referenceHistogramAttribute;
			}
			else
			{
				referenceHistogram = estimateReferenceHistogram(
						sparkContext,
						dataProvider, dataAccessType,
						histogramsN5BasePath, histogramsDataset,
						fieldOfViewSize, blockSize,
						REFERENCE_HISTOGRAM_POINTS_PERCENT,
						histMinValue, histMaxValue, bins
					);

				// cache reference histogram in the attributes
				n5.setAttribute( histogramsDataset, REFERENCE_HISTOGRAM_KEY, referenceHistogram );
			}
		}
		return referenceHistogram;
	}
	public static < T extends RealType< T > > double[] estimateReferenceHistogram(
			final JavaSparkContext sparkContext,
			final DataProvider dataProvider, final DataAccessType dataAccessType,
			final String histogramsN5BasePath, final String histogramsDataset,
			final long[] fieldOfViewSize, final int[] blockSize,
			final double medianPointsPercent,
			final double histMinValue, final double histMaxValue, final int bins )
	{
		final long numPixels = Intervals.numElements( fieldOfViewSize );
		final long numMedianPoints = Math.round( numPixels * medianPointsPercent );
		final long mStart = Math.round( numPixels / 2.0 ) - Math.round( numMedianPoints / 2.0 );
		final long mEnd = mStart + numMedianPoints;

		final List< long[] > blockPositions = getBlockPositions( fieldOfViewSize, blockSize );
		final double[] accumulatedFilteredHistogram = sparkContext.parallelize( blockPositions, Math.min( blockPositions.size(), MAX_PARTITIONS ) )
			// compute mean value for each histogram
			.flatMapToPair( blockPosition ->
				{
					final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataAccessType );
					final N5Reader n5Local = dataProviderLocal.createN5Reader( URI.create( histogramsN5BasePath ) );
					final RandomAccessibleInterval< T > histogramsStorageImg = ( RandomAccessibleInterval< T > ) N5Utils.open( n5Local, histogramsDataset );
					final CompositeIntervalView< T, RealComposite< T > > histogramsImg = Views.collapseReal( histogramsStorageImg );

					final Real1dBinMapper< T > binMapper = new Real1dBinMapper<>( histMinValue, histMaxValue, bins, true );
					final T binCenterValue = ( T ) new DoubleType();

					final CellGrid cellGrid = new CellGrid( fieldOfViewSize, blockSize );
					final long[] cellMin = new long[ cellGrid.numDimensions() ], cellMax = new long[ cellGrid.numDimensions() ];
					final int[] cellDimensions = new int[ cellGrid.numDimensions() ];
					cellGrid.getCellDimensions( blockPosition, cellMin, cellDimensions );
					for ( int d = 0; d < cellGrid.numDimensions(); ++d )
						cellMax[ d ] = cellMin[ d ] + cellDimensions[ d ] - 1;
					final Interval blockInterval = new FinalInterval( cellMin, cellMax );

					final IntervalView< RealComposite< T > > histogramsBlockImg = Views.interval( histogramsImg, blockInterval );
					final Cursor< RealComposite< T > > histogramsBlockImgCursor = Views.iterable( histogramsBlockImg ).localizingCursor();

					final List< Tuple2< Float, Long > > histogramMeanAndPixelIndex = new ArrayList<>();
					while ( histogramsBlockImgCursor.hasNext() )
					{
						final RealComposite< T > histogram = histogramsBlockImgCursor.next();

						// compute mean value of the histogram (excluding tail bins)
						double histogramValueSum = 0, histogramQuantitySum = 0;
						for ( long bin = 1; bin < bins - 1; ++bin )
						{
							final double binQuantity = histogram.get( bin ).getRealDouble();
							binMapper.getCenterValue( bin, binCenterValue );
							histogramValueSum += binQuantity * binCenterValue.getRealDouble();
							histogramQuantitySum += binQuantity;
						}
						final double histogramMean = histogramValueSum / histogramQuantitySum;

						final long pixelIndex = IntervalIndexer.positionToIndex( histogramsBlockImgCursor, histogramsImg );
						histogramMeanAndPixelIndex.add( new Tuple2<>( ( float ) histogramMean, pixelIndex ) );
					}
					return histogramMeanAndPixelIndex.iterator();
				}
			)
			// sort histograms by their mean values
			.sortByKey()
			.zipWithIndex()
			// choose subset of these histograms (e.g. >25% and <75%)
			.filter( tuple -> tuple._2() >= mStart && tuple._2() < mEnd )
			// map filtered histograms to their respective N5 blocks where they belong
			.mapToPair( tuple ->
				{
					final long pixelIndex = tuple._1()._2();
					final long[] pixelPosition = new long[ fieldOfViewSize.length ], blockPosition = new long[ fieldOfViewSize.length ];
					IntervalIndexer.indexToPosition( pixelIndex, fieldOfViewSize, pixelPosition );

					final CellGrid cellGrid = new CellGrid( fieldOfViewSize, blockSize );
					final long[] cellGridDimensions = cellGrid.getGridDimensions();
					cellGrid.getCellPosition( pixelPosition, blockPosition );

					final long blockIndex = IntervalIndexer.positionToIndex( blockPosition, cellGridDimensions );
					return new Tuple2<>( blockIndex, pixelPosition );
				}
			)
			// group filtered histograms by their respective N5 blocks
			.groupByKey()
			// for each N5 block, accumulate all filtered histograms within this block
			.map( tuple ->
				{
					final Iterable< long[] > pixelPositions = tuple._2();
					final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataAccessType );
					final N5Reader n5Local = dataProviderLocal.createN5Reader( URI.create( histogramsN5BasePath ) );
					final RandomAccessibleInterval< T > histogramsStorageImg = ( RandomAccessibleInterval< T > ) N5Utils.open( n5Local, histogramsDataset );
					final CompositeIntervalView< T, RealComposite< T > > histogramsImg = Views.collapseReal( histogramsStorageImg );
					final RandomAccess< RealComposite< T > > histogramsImgRandomAccess = histogramsImg.randomAccess();

					final double[] accumulatedFilteredBlockHistogram = new double[ bins ];
					for ( final Iterator< long[] > it = pixelPositions.iterator(); it.hasNext(); )
					{
						final long[] pixelPosition = it.next();
						histogramsImgRandomAccess.setPosition( pixelPosition );
						final RealComposite< T > histogram = histogramsImgRandomAccess.get();
						for ( int bin = 0; bin < bins; ++bin )
							accumulatedFilteredBlockHistogram[ bin ] += histogram.get( bin ).getRealDouble();
					}
					return accumulatedFilteredBlockHistogram;
				}
			)
			.treeReduce( ( histogram, other ) ->
				{
					for ( int bin = 0; bin < bins; ++bin )
						histogram[ bin ] += other[ bin ];
					return histogram;
				},
				Integer.MAX_VALUE // max possible aggregation depth
			);

		// average the accumulated histogram
		for ( int bin = 0; bin < bins; ++bin )
			accumulatedFilteredHistogram[ bin ] /= numMedianPoints;

		return accumulatedFilteredHistogram;
	}

	public static List< long[] > getBlockPositions( final long[] dimensions, final int[] blockSize )
	{
		final List< long[] > blockPositions = new ArrayList<>();
		final CellGrid cellGrid = new CellGrid( dimensions, blockSize );
		for ( long blockIndex = 0; blockIndex < Intervals.numElements( cellGrid.getGridDimensions() ); ++blockIndex )
		{
			final long[] blockPosition = new long[ cellGrid.numDimensions() ];
			cellGrid.getCellGridPositionFlat( blockIndex, blockPosition );
			blockPositions.add( blockPosition );
		}
		return blockPositions;
	}

	private boolean sliceHistogramsExist() throws IOException, URISyntaxException
	{
		// check if histograms exist in old slice-based format
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
			if ( !dataProvider.fileExists( dataProvider.getUri( generateSliceHistogramsPath( 0, slice ) ) ) )
				return false;
		return true;
	}

	private String generateSliceHistogramsPath( final int scale, final int slice )
	{
		if ( !histogramsDataset.endsWith( "-n5" ) )
			throw new RuntimeException( "wrong path" );

		return PathResolver.get( histogramsN5BasePath, histogramsDataset.substring( 0, histogramsDataset.lastIndexOf( "-n5" ) ), Integer.toString( scale ), Integer.toString( slice ) + ".hist" );
	}

	private int getNumSlices()
	{
		return ( int ) ( workingInterval.numDimensions() == 3 ? workingInterval.dimension( 2 ) : 1 );
	}
}
