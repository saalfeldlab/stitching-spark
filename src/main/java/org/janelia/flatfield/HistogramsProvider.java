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
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.janelia.dataaccess.CloudURI;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.histogram.Histogram;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.stitching.PipelineExecutionException;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileLoader;
import org.janelia.stitching.TileLoader.TileType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.list.ListCursor;
import net.imglib2.img.list.ListImg;
import net.imglib2.img.list.ListLocalizingCursor;
import net.imglib2.img.list.WrappedListImg;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import scala.Tuple2;

public class HistogramsProvider implements Serializable
{
	private static final long serialVersionUID = 2090264857259429741L;

	private static final double REFERENCE_HISTOGRAM_POINTS_PERCENT = 0.25;
	private static final int HISTOGRAMS_DEFAULT_BLOCK_SIZE = 64;
	private static final String HISTOGRAMS_N5_DATASET_NAME = "histograms-n5";
	private static final String ALL_HISTOGRAMS_EXIST_KEY = "allHistogramsExist";

	private transient final JavaSparkContext sparkContext;
	private transient final DataProvider dataProvider;
	private final DataProviderType dataProviderType;
	private final TileInfo[] tiles;
	private final Interval workingInterval;
	private final long[] fullTileSize;
	private final boolean use2D;

	private final String histogramsN5BasePath;
	private final String histogramsDataset;

	final Double histMinValue, histMaxValue;
	final int bins;

	private transient JavaPairRDD< Long, Histogram > rddHistograms;
	private transient Histogram referenceHistogram;

	public HistogramsProvider(
			final JavaSparkContext sparkContext,
			final DataProvider dataProvider,
			final Interval workingInterval,
			final String basePath,
			final TileInfo[] tiles,
			final long[] fullTileSize,
			final boolean use2D,
			final Double histMinValue, final Double histMaxValue, final int bins ) throws IOException, URISyntaxException
	{
		this.sparkContext = sparkContext;
		this.dataProvider = dataProvider;
		this.workingInterval = workingInterval;
		this.tiles = tiles;
		this.fullTileSize = fullTileSize;
		this.use2D = use2D;

		this.histMinValue = histMinValue;
		this.histMaxValue = histMaxValue;
		this.bins = bins;

		dataProviderType = dataProvider.getType();

		if ( dataProviderType == DataProviderType.FILESYSTEM )
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

		if ( !use2D && sliceHistogramsExist() )
		{
			// if the histograms are stored in the old format, convert them to the new N5 format first
			convertHistogramsToN5();
		}
		else
		{
			populateHistogramsN5();
		}
	}

	private < T extends NativeType< T > & RealType< T > > void populateHistogramsN5() throws IOException, URISyntaxException
	{
		System.out.println( "Populating histograms using n5 blocks of hash maps..." );

		// check if tiles are single image files, or N5 datasets
		final TileType tileType = TileLoader.getTileType( tiles[ 0 ], dataProvider );
		// TODO: check that all tiles are of the same type

		final long[] fieldOfViewSize = use2D ? new long[] { fullTileSize[ 0 ], fullTileSize[ 1 ] } : fullTileSize.clone();

		final int[] blockSize = new int[ fieldOfViewSize.length ];
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

		final List< long[] > blockGridPositions = new ArrayList<>();
		final CellGrid cellGrid = new CellGrid( fieldOfViewSize, blockSize );
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
					fieldOfViewSize,
					blockSize,
					DataType.SERIALIZABLE,
					new GzipCompression()
				);
		}
		else
		{
			// check the dimensionality of the existing histograms
			if ( n5.getDatasetAttributes( histogramsDataset ).getNumDimensions() != fieldOfViewSize.length )
				throw new RuntimeException( "histograms-n5 has different dimensionality than the field of view" );

			// skip this step if the flag 'allHistogramsExist' is set
			final Boolean allHistogramsExist = n5.getAttribute( histogramsDataset, ALL_HISTOGRAMS_EXIST_KEY, Boolean.class );
			if ( allHistogramsExist != null && allHistogramsExist )
				return;
		}

		sparkContext.parallelize( blockGridPositions, blockGridPositions.size() ).foreach( blockGridPosition ->
			{
				final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataProviderType );
				final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsN5BasePath ) );
				final SerializableDataBlockWrapper< HashMap< Integer, Integer > > histogramsBlock = new SerializableDataBlockWrapper<>( n5Local, histogramsDataset, blockGridPosition );

				if ( histogramsBlock.wasLoadedSuccessfully() )
				{
					System.out.println( "Skipping block at " + Arrays.toString( blockGridPosition ) + " (already exists)" );
					return;
				}

				final WrappedListImg< HashMap< Integer, Integer > > histogramsBlockImg = histogramsBlock.wrap();
				final ListCursor< HashMap< Integer, Integer > > histogramsBlockImgCursor = histogramsBlockImg.cursor();
				while ( histogramsBlockImgCursor.hasNext() )
				{
					histogramsBlockImgCursor.fwd();
					histogramsBlockImgCursor.set( new HashMap<>() );
				}

				// create an interval to be processed in each tile image
				final long[] blockIntervalMin = new long[ blockSize.length ], blockIntervalMax = new long[ blockSize.length ];
				for ( int d = 0; d < blockSize.length; ++d )
				{
					blockIntervalMin[ d ] = blockGridPosition[ d ] * blockSize[ d ];
					blockIntervalMax[ d ] = Math.min( ( blockGridPosition[ d ] + 1 ) * blockSize[ d ], fieldOfViewSize[ d ] ) - 1;
				}
				final Interval blockInterval = new FinalInterval( blockIntervalMin, blockIntervalMax );

				// loop over tile images and populate the histograms using the corresponding part of each tile image
				int done = 0;
				for ( final TileInfo tile : tiles )
				{
					final RandomAccessibleInterval< T > tileImg = TileLoader.loadTile( tile, dataProviderLocal );
					final Interval tileImgOffsetInterval;
					if ( tileImg.numDimensions() == 3 )
					{
						tileImgOffsetInterval = new FinalInterval(
								new long[] { blockInterval.min( 0 ), blockInterval.min( 1 ), blockInterval.numDimensions() >= 3 ? blockInterval.min( 2 ) : tileImg.min( 2 ) },
								new long[] { blockInterval.max( 0 ), blockInterval.max( 1 ), blockInterval.numDimensions() >= 3 ? blockInterval.max( 2 ) : tileImg.max( 2 ) }
							);
					}
					else
					{
						tileImgOffsetInterval = new FinalInterval(
								new long[] { blockInterval.min( 0 ), blockInterval.min( 1 ) },
								new long[] { blockInterval.max( 0 ), blockInterval.max( 1 ) }
							);
					}
					final RandomAccessibleInterval< T > tileImgInterval = Views.offsetInterval( tileImg, tileImgOffsetInterval );
					final IterableInterval< T > tileImgIterableInterval = Views.iterable( tileImgInterval );
					final Cursor< T > tileImgIntervalCursor = tileImgIterableInterval.localizingCursor();
					final RandomAccess< HashMap< Integer, Integer > > histogramsBlockImgRandomAccess = histogramsBlockImg.randomAccess();
					final long[] tileImgPosition = new long[ tileImgIntervalCursor.numDimensions() ], histogramsBlockPosition = new long[ histogramsBlockImgRandomAccess.numDimensions() ];
					while ( tileImgIntervalCursor.hasNext() )
					{
						final int key = ( int ) tileImgIntervalCursor.next().getRealDouble();
						tileImgIntervalCursor.localize( tileImgPosition );
						System.arraycopy( tileImgPosition, 0, histogramsBlockPosition, 0, histogramsBlockPosition.length );
						histogramsBlockImgRandomAccess.setPosition( histogramsBlockPosition );
						final HashMap< Integer, Integer > histogram = histogramsBlockImgRandomAccess.get();
						histogram.put( key, histogram.getOrDefault( key, 0 ) + 1 );
					}

					if ( ++done % 20 == 0 )
						System.out.println( "Block min=" + Arrays.toString( Intervals.minAsLongArray( blockInterval ) ) + ", max=" + Arrays.toString( Intervals.maxAsLongArray( blockInterval ) ) + ": processed " + done + " tiles" );
				}

				System.out.println( "Block min=" + Arrays.toString( Intervals.minAsLongArray( blockInterval ) ) + ", max=" + Arrays.toString( Intervals.maxAsLongArray( blockInterval ) ) + ": populated histograms" );

				histogramsBlock.save();
			} );

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


	public JavaPairRDD< Long, Histogram > getHistograms() throws IOException
	{
		if ( rddHistograms == null )
			loadHistogramsN5();

		return rddHistograms;
	}

	private void loadHistogramsN5() throws IOException
	{
		final long[] fieldOfViewSize = use2D ? new long[] { fullTileSize[ 0 ], fullTileSize[ 1 ] } : fullTileSize.clone();

		final List< long[] > blockGridPositions = new ArrayList<>();
		final int[] blockSize = dataProvider.createN5Reader( URI.create( histogramsN5BasePath ) ).getDatasetAttributes( histogramsDataset ).getBlockSize();
		if ( blockSize.length != fieldOfViewSize.length )
			throw new RuntimeException( "histograms-n5 dataset has different dimensionality than the field of view" );

		final CellGrid cellGrid = new CellGrid( fieldOfViewSize, blockSize );
		for ( int index = 0; index < Intervals.numElements( cellGrid.getGridDimensions() ); ++index )
		{
			final long[] blockGridPosition = new long[ cellGrid.numDimensions() ];
			cellGrid.getCellGridPositionFlat( index, blockGridPosition );
			blockGridPositions.add( blockGridPosition );
		}

		rddHistograms = sparkContext.parallelize( blockGridPositions, blockGridPositions.size() ) .flatMapToPair( blockGridPosition ->
					{
						final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataProviderType );
						final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsN5BasePath ) );
						final SerializableDataBlockWrapper< HashMap< Integer, Integer > > histogramsBlock = new SerializableDataBlockWrapper<>( n5Local, histogramsDataset, blockGridPosition );

						if ( !histogramsBlock.wasLoadedSuccessfully() )
							throw new PipelineExecutionException( "Data block at position " + Arrays.toString( blockGridPosition ) + " cannot be loaded" );

						final long[] blockPixelOffset = new long[ blockSize.length ];
						for ( int d = 0; d < blockPixelOffset.length; ++d )
							blockPixelOffset[ d ] = blockGridPosition[ d ] * blockSize[ d ];

						// TODO: when workingInterval is specified, add optimized version for loading only those blocks that fall within the desired interval
						final List< Tuple2< Long, HashMap< Integer, Integer > > > ret = new ArrayList<>();
						final WrappedListImg< HashMap< Integer, Integer > > histogramsBlockImg = histogramsBlock.wrap();
						final ListLocalizingCursor< HashMap< Integer, Integer > > histogramsBlockImgCursor = histogramsBlockImg.localizingCursor();
						final long[] pixelPosition = new long[ blockSize.length ];
						while ( histogramsBlockImgCursor.hasNext() )
						{
							histogramsBlockImgCursor.fwd();
							histogramsBlockImgCursor.localize( pixelPosition );

							// apply block pixel offset
							for ( int d = 0; d < pixelPosition.length; ++d )
								pixelPosition[ d ] += blockPixelOffset[ d ];

							final long pixelIndex = IntervalIndexer.positionToIndex( pixelPosition, fieldOfViewSize );
							ret.add( new Tuple2<>( pixelIndex, histogramsBlockImgCursor.get() ) );
						}
						return ret.iterator();
					} )
				.mapValues( map ->
					{
						final Histogram histogram = new Histogram( histMinValue, histMaxValue, bins );
						for ( final Entry< Integer, Integer > entry : map.entrySet() )
							histogram.put( entry.getKey(), entry.getValue() );
						return histogram;
					} )
				.persist( StorageLevel.MEMORY_ONLY_SER() );
	}

	private void convertHistogramsToN5() throws IOException
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
				final SerializableDataBlockWrapper< HashMap< Integer, Integer > > histogramsBlock = new SerializableDataBlockWrapper<>( n5Local, histogramsDataset, blockGridPosition );

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
	}

	public Histogram getReferenceHistogram()
	{
		if ( referenceHistogram == null )
			referenceHistogram = estimateReferenceHistogram( rddHistograms, REFERENCE_HISTOGRAM_POINTS_PERCENT );
		return referenceHistogram;
	}
	public static Histogram estimateReferenceHistogram( final JavaPairRDD< Long, Histogram > rddHistograms, final double medianPointsPercent )
	{
		final long numPixels = rddHistograms.count();
		final long numMedianPoints = Math.round( numPixels * medianPointsPercent );
		final long mStart = Math.round( numPixels / 2.0 ) - Math.round( numMedianPoints / 2.0 );
		final long mEnd = mStart + numMedianPoints;

		final Histogram accumulatedHistograms = rddHistograms
			.mapValues( histogram ->
				{
					double sum = 0;
					for ( int i = 0; i < histogram.getNumBins(); i++ )
						sum += histogram.get( i ) * histogram.getBinValue( i );
					return sum / histogram.getQuantityTotal();
				}
			)
			.mapToPair( pair -> pair.swap() )
			.sortByKey()
			.zipWithIndex()
			.filter( tuple -> tuple._2() >= mStart && tuple._2() < mEnd )
			.mapToPair( tuple -> tuple._1().swap() )
			.join( rddHistograms )
			.map( item -> item._2()._2() )
			.treeReduce(
				( ret, histogram ) ->
				{
					ret.add( histogram );
					return ret;
				},
				Integer.MAX_VALUE // max possible aggregation depth
			);

		accumulatedHistograms.average( numMedianPoints );

		return accumulatedHistograms;
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
