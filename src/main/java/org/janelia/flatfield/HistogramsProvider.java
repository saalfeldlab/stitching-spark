package org.janelia.flatfield;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.janelia.histogram.Histogram;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileLoader;
import org.janelia.stitching.TileLoader.TileType;
import org.janelia.util.Conversions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.histogram.Histogram1d;
import net.imglib2.histogram.Real1dBinMapper;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.list.ListImg;
import net.imglib2.img.list.ListLocalizingCursor;
import net.imglib2.img.list.WrappedListImg;
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

	private transient final JavaSparkContext sparkContext;
	private transient final DataProvider dataProvider;
	private transient final TileInfo[] tiles;

	private final DataAccessType dataAccessType;
	private final Interval workingInterval;
	private final long[] fullTileSize;

	private final String histogramsN5BasePath;
	private final String histogramsDataset;

	private final double histMinValue, histMaxValue;
	private final int bins;

	private final long[] fieldOfViewSize;
	private final int[] blockSize;

	private transient Histogram referenceHistogram;

	public HistogramsProvider(
			final JavaSparkContext sparkContext,
			final DataProvider dataProvider,
			final Interval workingInterval,
			final String basePath,
			final TileInfo[] tiles,
			final long[] fullTileSize,
			final double histMinValue, final double histMaxValue, final int bins ) throws IOException, URISyntaxException
	{
		this.sparkContext = sparkContext;
		this.dataProvider = dataProvider;
		this.workingInterval = workingInterval;
		this.tiles = tiles;
		this.fullTileSize = fullTileSize;

		this.histMinValue = histMinValue;
		this.histMaxValue = histMaxValue;
		this.bins = bins;

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
		sparkContext.parallelize( blockPositions, blockPositions.size() ).foreach( blockPosition ->
			{
				final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataAccessType );

				// create histogram block
				final RandomAccessibleInterval< DoubleType > histogramsStorageImg = ArrayImgs.doubles( Conversions.toLongArray( extendedBlockSize ) );
				final RandomAccessibleInterval< R > histogramsGenericStorageImg = ( RandomAccessibleInterval< R > ) histogramsStorageImg;
				final CompositeIntervalView< R, RealComposite< R > > histogramsImg = Views.collapseReal( histogramsGenericStorageImg );

				// create an interval to be processed in each tile image
				final long[] blockIntervalMin = new long[ blockSize.length ], blockIntervalMax = new long[ blockSize.length ];
				for ( int d = 0; d < blockSize.length; ++d )
				{
					blockIntervalMin[ d ] = blockPosition[ d ] * blockSize[ d ];
					blockIntervalMax[ d ] = Math.min( ( blockPosition[ d ] + 1 ) * blockSize[ d ], fieldOfViewSize[ d ] ) - 1;
				}
				final Interval blockInterval = new FinalInterval( blockIntervalMin, blockIntervalMax );

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

					final RandomAccessibleInterval< ? > tileImgInterval;
					if ( tileStorageImg.numDimensions() < blockInterval.numDimensions() )
						tileImgInterval = Views.collapseReal( tileStorageImgInterval );
					else
						tileImgInterval = tileStorageImgInterval;

					final Cursor< RealComposite< R > > histogramsImgCursor = Views.flatIterable( histogramsImg ).cursor();
					final Cursor< ? > tileCursor = Views.flatIterable( tileImgInterval ).cursor();

					// handles all possible cases:
					// 1) FoV is 2D, tile is 2D
					// 2) FoV is 3D, tile is 3D
					// 3) FoV is 2D, tile is 3D (last dimension in tile space is collapsed and used as additional data points)
					while ( histogramsImgCursor.hasNext() || tileCursor.hasNext() )
					{
						final RealComposite< R > histogramStorage = histogramsImgCursor.next();
						final Real1dBinMapper< R > binMapper = new Real1dBinMapper<>( histMinValue, histMaxValue, bins, true );
						final Histogram1d< R > histogram = new Histogram1d<>( histogramStorage, binMapper );

						final Object value = tileCursor.next();
						if ( value instanceof RealComposite< ? > )
							histogram.addData( ( RealComposite< R > ) value );
						else if ( value instanceof RealType< ? > )
							histogram.increment( ( R ) value );
						else
							throw new RuntimeException( "unknown type" );
					}

					if ( ++done % 20 == 0 )
						System.out.println( "Block min=" + Arrays.toString( Intervals.minAsLongArray( blockInterval ) ) + ", max=" + Arrays.toString( Intervals.maxAsLongArray( blockInterval ) ) + ": processed " + done + " tiles" );
				}

				System.out.println( "Block min=" + Arrays.toString( Intervals.minAsLongArray( blockInterval ) ) + ", max=" + Arrays.toString( Intervals.maxAsLongArray( blockInterval ) ) + ": populated histograms" );

				final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsN5BasePath ) );
				final long[] extendedBlockPosition = new long[ extendedBlockSize.length ];
				System.arraycopy( blockPosition, 0, extendedBlockPosition, 0, blockPosition.length );
				N5Utils.saveBlock( histogramsStorageImg, n5Local, histogramsDataset, extendedBlockPosition );
			} );

		broadcastedTiles.destroy();

		// mark all histograms as ready to skip block existence check and save time for subsequent runs
		n5.setAttribute( histogramsDataset, ALL_HISTOGRAMS_EXIST_KEY, true );
	}

	@SuppressWarnings( "unchecked" )
	private ListImg< HashMap< Integer, Integer > > readSliceHistograms( final DataProvider dataProvider, final int slice ) throws IOException
	{
		return new ListImg<>( Arrays.asList( readSliceHistogramsArray( dataProvider, 0, slice ) ), new long[] { fullTileSize[ 0 ], fullTileSize[ 1 ] } );
	}
	@SuppressWarnings( "rawtypes" )
	private HashMap[] readSliceHistogramsArray( final DataProvider dataProvider, final int scale, final int slice ) throws IOException
	{
		System.out.println( "Loading slice " + slice );
		final String path = generateSliceHistogramsPath( scale, slice );

		if ( !dataProvider.fileExists( URI.create( path ) ) )
			return null;

//		final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();
		final MapSerializer serializer = new MapSerializer();
		serializer.setKeysCanBeNull( false );
		serializer.setKeyClass( Integer.class, kryo.getSerializer( Integer.class ) );
		serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
		kryo.register( HashMap.class, serializer );

		try ( final InputStream is = new FileInputStream( path ) )
		{
			try ( final Input input = new Input( is ) )
			{
				return kryo.readObject( input, HashMap[].class );
			}
		}
	}

	/*private void convertHistogramsToN5() throws IOException
	{
		final int[] blockSize = new int[ fullTileSize.length ];
		Arrays.fill( blockSize, HISTOGRAMS_DEFAULT_BLOCK_SIZE );

		final List< long[] > blockGridPositions = new ArrayList<>();
		final CellGrid cellGrid = new CellGrid( fullTileSize, blockSize );
		for ( int index = 0; index < Intervals.numElements( cellGrid.getGridDimensions() ); ++index )
		{
			final long[] blockGridPosition = new long[ cellGrid.numDimensions() ];
			cellGrid.getCellGridPositionFlat( index, blockGridPosition );
			blockGridPositions.add( blockGridPosition );
		}

		final N5Writer n5 = dataProvider.createN5Writer( URI.create( histogramsN5BasePath ) );
		if ( !n5.datasetExists( histogramsDataset ) )
		{
			n5.createDataset(
					histogramsDataset,
					fullTileSize,
					blockSize,
					DataType.SERIALIZABLE,
					new GzipCompression()
				);
		}

		sparkContext.parallelize( blockGridPositions, blockGridPositions.size() ).foreach( blockGridPosition ->
			{
				final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataProviderType );
				final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsN5BasePath ) );
				final WrappedSerializableDataBlockWriter< HashMap< Integer, Integer > > histogramsBlock = new WrappedSerializableDataBlockWriter<>(
						n5Local,
						histogramsDataset,
						blockGridPosition
					);

				if ( histogramsBlock.wasLoadedSuccessfully() )
				{
					System.out.println( "Skipping block at " + Arrays.toString( blockGridPosition ) + " (already exists)" );
					return;
				}

				final long[] blockPixelOffset = new long[ blockSize.length ];
				for ( int d = 0; d < blockPixelOffset.length; ++d )
					blockPixelOffset[ d ] = blockGridPosition[ d ] * blockSize[ d ];

				// create an interval to be processed in each tile image
				final long[] blockIntervalMin = new long[ blockSize.length ], blockIntervalMax = new long[ blockSize.length ];
				for ( int d = 0; d < blockSize.length; ++d )
				{
					blockIntervalMin[ d ] = blockGridPosition[ d ] * blockSize[ d ];
					blockIntervalMax[ d ] = Math.min( ( blockGridPosition[ d ] + 1 ) * blockSize[ d ], fullTileSize[ d ] ) - 1;
				}
				final Interval blockInterval = new FinalInterval( blockIntervalMin, blockIntervalMax );
				// create a 2D interval to be processed in each slice
				final Interval sliceInterval = new FinalInterval( new long[] { blockIntervalMin[ 0 ], blockIntervalMin[ 1 ] }, new long[] { blockIntervalMax[ 0 ], blockIntervalMax[ 1 ] } );


				final WrappedListImg< HashMap< Integer, Integer > > histogramsBlockImg = histogramsBlock.wrap();
				final ListCursor< HashMap< Integer, Integer > > histogramsBlockImgCursor = histogramsBlockImg.cursor();
				final long[] pixelPosition = new long[ blockGridPosition.length ];
				while ( histogramsBlockImgCursor.hasNext() )
				{
					histogramsBlockImgCursor.fwd();
					histogramsBlockImgCursor.localize( pixelPosition );

					// apply block pixel offset
					for ( int d = 0; d < pixelPosition.length; ++d )
						pixelPosition[ d ] += blockPixelOffset[ d ];

					// load histograms for corresponding slice
					final int slice = ( int ) pixelPosition[ 2 ] + 1;
					final RandomAccessibleInterval< HashMap< Integer, Integer > > sliceHistograms = readSliceHistograms( dataProviderLocal, slice );
					final RandomAccessibleInterval< HashMap< Integer, Integer > > sliceHistogramsInterval = Views.offsetInterval( sliceHistograms, sliceInterval );
					final Cursor< HashMap< Integer, Integer > > sliceHistogramsIntervalCursor = Views.flatIterable( sliceHistogramsInterval ).cursor();
					// block cursor is one step forward, make sure they are aligned throughout subsequent steps
					histogramsBlockImgCursor.set( sliceHistogramsIntervalCursor.next() );
					while ( sliceHistogramsIntervalCursor.hasNext() )
					{
						histogramsBlockImgCursor.fwd();
						histogramsBlockImgCursor.set( sliceHistogramsIntervalCursor.next() );
					}
				}

				System.out.println( "Block min=" + Arrays.toString( Intervals.minAsLongArray( blockInterval ) ) + ", max=" + Arrays.toString( Intervals.maxAsLongArray( blockInterval ) ) + ": converted slice histograms to N5" );

				histogramsBlock.save();
			} );
	}*/

	public Histogram getReferenceHistogram()
	{
		if ( referenceHistogram == null )
		{
			referenceHistogram = estimateReferenceHistogram(
					sparkContext,
					dataProvider, dataAccessType,
					histogramsN5BasePath, histogramsDataset,
					fieldOfViewSize, blockSize,
					REFERENCE_HISTOGRAM_POINTS_PERCENT,
					histMinValue, histMaxValue, bins
				);
		}
		return referenceHistogram;
	}
	public static Histogram estimateReferenceHistogram(
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
		final Histogram accumulatedHistogram = sparkContext.parallelize( blockPositions, blockPositions.size() )
			// compute mean value for each histogram
			.flatMapToPair( blockPosition ->
				{
					final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataAccessType );
					final N5Reader n5Local = dataProviderLocal.createN5Reader( URI.create( histogramsN5BasePath ) );
					final WrappedSerializableDataBlockReader< Histogram > histogramsBlock = new WrappedSerializableDataBlockReader<>(
							n5Local,
							histogramsDataset,
							blockPosition
						);
					final WrappedListImg< Histogram > histogramsBlockImg = histogramsBlock.wrap();
					final ListLocalizingCursor< Histogram > histogramsBlockImgCursor = histogramsBlockImg.localizingCursor();

					final long[] pixelPosition = new long[ blockSize.length ];
					final long[] blockPixelOffset = new long[ blockSize.length ];
					for ( int d = 0; d < blockPixelOffset.length; ++d )
						blockPixelOffset[ d ] = blockPosition[ d ] * blockSize[ d ];

					final List< Tuple2< Long, Float > > pixelIndexAndHistogramMean = new ArrayList<>();
					while ( histogramsBlockImgCursor.hasNext() )
					{
						final Histogram histogram = histogramsBlockImgCursor.next();

						// compute mean value of the histogram
						double histogramSum = 0;
						for ( int i = 0; i < histogram.getNumBins(); i++ )
							histogramSum += histogram.get( i ) * histogram.getBinValue( i );
						final double histogramMean = histogramSum / histogram.getQuantityTotal();

						// compute pixel index
						histogramsBlockImgCursor.localize( pixelPosition );
						for ( int d = 0; d < pixelPosition.length; ++d )
							pixelPosition[ d ] += blockPixelOffset[ d ];
						final long pixelIndex = IntervalIndexer.positionToIndex( pixelPosition, fieldOfViewSize );

						pixelIndexAndHistogramMean.add( new Tuple2<>( pixelIndex, ( float ) histogramMean ) );
					}
					return pixelIndexAndHistogramMean.iterator();
				}
			)
			.mapToPair( pair -> pair.swap() )
			// sort histograms by their mean values
			.sortByKey()
			.zipWithIndex()
			// choose subset of these histograms (e.g. >25% and <75%)
			.filter( tuple -> tuple._2() >= mStart && tuple._2() < mEnd )
			// map chosen histograms to their respective N5 blocks where they belong
			.mapToPair( tuple ->
				{
					final CellGrid cellGrid = new CellGrid( fieldOfViewSize, blockSize );
					final long[] cellGridDimensions = cellGrid.getGridDimensions();
					final long[] pixelPosition = new long[ fieldOfViewSize.length ], blockPosition = new long[ fieldOfViewSize.length ];
					final long pixelIndex = tuple._1()._2();
					IntervalIndexer.indexToPosition( pixelIndex, fieldOfViewSize, pixelPosition );
					cellGrid.getCellPosition( pixelPosition, blockPosition );
					final int blockIndex = ( int ) IntervalIndexer.positionToIndex( blockPosition, cellGridDimensions );
					return new Tuple2<>( blockIndex, pixelIndex );
				}
			)
			// group histogram indexes by their respective N5 blocks
			.groupByKey()
			// for each N5 block, accumulate all histograms
			.map( tuple ->
				{
					final int blockIndex = tuple._1();
					final CellGrid cellGrid = new CellGrid( fieldOfViewSize, blockSize );
					final long[] blockPosition = new long[ fieldOfViewSize.length ];
					cellGrid.getCellGridPositionFlat( blockIndex, blockPosition );

					final long[] pixelPosition = new long[ blockSize.length ];
					final long[] blockPixelOffset = new long[ blockSize.length ];
					for ( int d = 0; d < blockPixelOffset.length; ++d )
						blockPixelOffset[ d ] = blockPosition[ d ] * blockSize[ d ];

					final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataAccessType );
					final N5Reader n5Local = dataProviderLocal.createN5Reader( URI.create( histogramsN5BasePath ) );
					final WrappedSerializableDataBlockReader< Histogram > histogramsBlock = new WrappedSerializableDataBlockReader<>(
							n5Local,
							histogramsDataset,
							blockPosition
						);
					final WrappedListImg< Histogram > histogramsBlockImg = histogramsBlock.wrap();
					final IntervalView< Histogram > translatedHistogramsBlockImg = Views.translate( histogramsBlockImg, blockPixelOffset );
					final RandomAccess< Histogram > translatedHistogramsBlockImgRandomAccess = translatedHistogramsBlockImg.randomAccess();

					final Histogram accumulatedBlockHistogram = new Histogram( histMinValue, histMaxValue, bins );
					for ( final Iterator< Long > it = tuple._2().iterator(); it.hasNext(); )
					{
						final long pixelIndex = it.next();
						IntervalIndexer.indexToPosition( pixelIndex, fieldOfViewSize, pixelPosition );
						translatedHistogramsBlockImgRandomAccess.setPosition( pixelPosition );
						final Histogram histogram = translatedHistogramsBlockImgRandomAccess.get();
						accumulatedBlockHistogram.add( histogram );
					}
					return accumulatedBlockHistogram;
				}
			)
			.treeReduce( ( histogram, other ) ->
				{
					histogram.add( other );
					return histogram;
				},
				Integer.MAX_VALUE // max possible aggregation depth
			);

		accumulatedHistogram.average( numMedianPoints );
		return accumulatedHistogram;
	}

	public static List< long[] > getBlockPositions( final long[] dimensions, final int[] blockSize )
	{
		final List< long[] > blockPositions = new ArrayList<>();
		final CellGrid cellGrid = new CellGrid( dimensions, blockSize );
		for ( int blockIndex = 0; blockIndex < Intervals.numElements( cellGrid.getGridDimensions() ); ++blockIndex )
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
