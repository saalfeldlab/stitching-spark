package org.janelia.flatfield;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.histogram.Histogram;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.stitching.PipelineExecutionException;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileLoader;
import org.janelia.stitching.TileLoader.TileType;
import org.janelia.util.TiffSliceReader;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.SerializableFinalInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.img.list.ListCursor;
import net.imglib2.img.list.ListImg;
import net.imglib2.img.list.ListLocalizingCursor;
import net.imglib2.img.list.ListRandomAccess;
import net.imglib2.img.list.WrappedListImg;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import scala.Tuple2;

public class HistogramsProvider implements Serializable
{
	private static final long serialVersionUID = 2090264857259429741L;

	private static final double REFERENCE_HISTOGRAM_POINTS_PERCENT = 0.25;

	private transient final JavaSparkContext sparkContext;
	private transient final DataProvider dataProvider;
	private final DataProviderType dataProviderType;
	private final TileInfo[] tiles;
	private final Interval workingInterval;
	private final String histogramsPath;
	private final long[] fullTileSize;

	final Double histMinValue, histMaxValue;
	final int bins;

	private transient JavaPairRDD< Long, Histogram > rddHistograms;
	private transient Histogram referenceHistogram;

	public HistogramsProvider(
			final JavaSparkContext sparkContext,
			final DataProvider dataProvider,
			final Interval workingInterval,
			final String histogramsPath,
			final TileInfo[] tiles,
			final long[] fullTileSize,
			final Double histMinValue, final Double histMaxValue, final int bins ) throws IOException, URISyntaxException
	{
		this.sparkContext = sparkContext;
		this.dataProvider = dataProvider;
		this.workingInterval = workingInterval;
		this.histogramsPath = histogramsPath;
		this.tiles = tiles;
		this.fullTileSize = fullTileSize;

		this.histMinValue = histMinValue;
		this.histMaxValue = histMaxValue;
		this.bins = bins;

		dataProviderType = dataProvider.getType();

		if ( !allHistogramsReady() )
		{
			// check if tiles are single image files, or N5 datasets
			final TileType tileType = TileLoader.getTileType( tiles[ 0 ], dataProvider );
			// TODO: check that all tiles are of the same type

			if ( tileType == TileType.N5_DATASET )
				populateHistogramsUsingN5Tiles();
			else if ( tileType == TileType.IMAGE_FILE )
				populateHistogramsUsingImageTiles();
		}
	}

	private < T extends NativeType< T > & RealType< T > > void populateHistogramsUsingN5Tiles() throws IOException, URISyntaxException
	{
		System.out.println( "Populating histograms using hash maps reading tiles as N5..." );

		final String channelDatasetPath = TileLoader.getChannelN5DatasetPath( tiles[ 0 ] );
		final int[] blockSize = TileLoader.getTileN5DatasetAttributes( tiles[ 0 ], dataProvider ).getBlockSize();

		final List< long[] > blockGridPositions = new ArrayList<>();
		final CellGrid cellGrid = new CellGrid( fullTileSize, blockSize );
		for ( int index = 0; index < Intervals.numElements( cellGrid.getGridDimensions() ); ++index )
		{
			final long[] blockGridPosition = new long[ cellGrid.numDimensions() ];
			cellGrid.getCellGridPositionFlat( index, blockGridPosition );
			blockGridPositions.add( blockGridPosition );
		}

		dataProvider.createN5Writer( URI.create( histogramsPath ) ).createDataset(
				channelDatasetPath,
				fullTileSize,
				blockSize,
				DataType.SERIALIZABLE,
				CompressionType.GZIP
			);

		sparkContext.parallelize( blockGridPositions ).foreach( blockGridPosition ->
			{
				final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataProviderType );
				final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsPath ) );

				// initialize histograms data block
				final SerializableDataBlockWrapper< HashMap< Integer, Integer > > histogramsBlock = new SerializableDataBlockWrapper<>( n5Local, channelDatasetPath, blockGridPosition );
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
					blockIntervalMax[ d ] = Math.min( ( blockGridPosition[ d ] + 1 ) * blockSize[ d ], fullTileSize[ d ] ) - 1;
				}
				final Interval blockInterval = new FinalInterval( blockIntervalMin, blockIntervalMax );

				// loop over tile images and populate the histograms using the corresponding part of each tile image
				int done = 0;
				for ( final TileInfo tile : tiles )
				{
					final RandomAccessibleInterval< T > tileImg = TileLoader.loadTile( tile, dataProviderLocal );
					final RandomAccessibleInterval< T > tileImgInterval = Views.offsetInterval( tileImg, blockInterval );
					final IterableInterval< T > tileImgIterableInterval = Views.flatIterable( tileImgInterval );
					if ( !tileImgIterableInterval.iterationOrder().equals( histogramsBlockImg.iterationOrder() ) )
						throw new PipelineExecutionException( "iteration order is different for histograms block and tile interval" );

					final Cursor< T > tileImgIntervalCursor = tileImgIterableInterval.cursor();
					histogramsBlockImgCursor.reset();
					while ( histogramsBlockImgCursor.hasNext() || tileImgIntervalCursor.hasNext() )
					{
						final int key = ( int ) tileImgIntervalCursor.next().getRealDouble();
						final HashMap< Integer, Integer > histogram = histogramsBlockImgCursor.next();
						histogram.put( key, histogram.getOrDefault( key, 0 ) + 1 );
					}

					if ( ++done % 20 == 0 )
						System.out.println( "Block min=" + Arrays.toString( blockIntervalMin ) + ", max=" + Arrays.toString( blockIntervalMax ) + ": processed " + done + " tiles" );
				}

				System.out.println( "Block min=" + Arrays.toString( blockIntervalMin ) + ", max=" + Arrays.toString( blockIntervalMax ) + ": populated histograms" );

				histogramsBlock.save();
			} );
	}

	// parallelizing over slices + cache-friendly image loading order
	@SuppressWarnings("unchecked")
	private < T extends NativeType< T > & RealType< T > > void populateHistogramsUsingImageTiles() throws IOException, URISyntaxException
	{
		// Check for existing histograms
		final List< Integer > remainingSlices = new ArrayList<>();
		final int minSlice = ( int ) ( workingInterval.numDimensions() == 3 ? workingInterval.min( 2 ) + 1 : 1 );
		final int maxSlice = ( int ) ( workingInterval.numDimensions() == 3 ? workingInterval.max( 2 ) + 1 : 1 );
		for ( int slice = minSlice; slice <= maxSlice; slice++ )
			if ( !dataProvider.fileExists( dataProvider.getUri( generateSliceHistogramsPath( 0, slice ) ) ) )
				remainingSlices.add( slice );

		final Interval workingSliceInterval = new SerializableFinalInterval(
				new long[] { workingInterval.min( 0 ), workingInterval.min( 1 ) },
				new long[] { workingInterval.max( 0 ), workingInterval.max( 1 ) } );

		System.out.println( "Populating histograms using hash maps..." );

		sparkContext.parallelize( remainingSlices ).zipWithIndex().foreach( tuple ->
			{
				final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataProviderType );

				final List< TileInfo > tilesList = Arrays.asList( tiles );
				final int slice = tuple._1(), index = tuple._2().intValue();
				System.out.println( "  Processing slice " + slice );

				final int slicePixels = ( int ) Intervals.numElements( workingSliceInterval );
				final List< HashMap< Integer, Integer > > histogramsList = new ArrayList<>( slicePixels );
				for ( int i = 0; i < slicePixels; ++i )
					histogramsList.add( new HashMap<>() );
				final ListImg< HashMap< Integer, Integer > > histogramsImg = new ListImg<>( histogramsList, Intervals.dimensionsAsLongArray( workingSliceInterval ) );
				final Cursor< HashMap< Integer, Integer > > histogramsImgCursor = Views.flatIterable( histogramsImg ).cursor();

				int done = 0;
				final int groupSize = remainingSlices.size();
				for ( int i = 0; i < tilesList.size(); i += groupSize )
				{
					final List< TileInfo > tilesGroup = new ArrayList<>( tilesList.subList( i, Math.min( i + groupSize, tilesList.size() ) ) );
					if ( index < tilesGroup.size() )
						tilesGroup.add( 0, tilesGroup.remove( index ) );
					System.out.println( "Processing group of size " + tilesGroup.size() + " with order-index = " + index );
					for ( final TileInfo tile : tilesGroup )
					{
						// TODO: consider converting the entire stack to N5 first (at least for S3)
						final ImagePlus impSlice = TiffSliceReader.readSlice( () -> dataProviderLocal.getInputStream( URI.create( tile.getFilePath() ) ), slice );
						final RandomAccessibleInterval< T > imgSlice = ImagePlusImgs.from( impSlice );
						final Cursor< T > cursor = Views.flatIterable( Views.offsetInterval( imgSlice, workingSliceInterval ) ).cursor();
						while ( cursor.hasNext() || histogramsImgCursor.hasNext() )
						{
							final int key = ( int ) cursor.next().getRealDouble();
							final HashMap< Integer, Integer > histogram = histogramsImgCursor.next();
							histogram.put( key, histogram.getOrDefault( key, 0 ) + 1 );
						}
						impSlice.close();
						histogramsImgCursor.reset();

						++done;
						if ( done % 20 == 0 )
							System.out.println( "Slice " + slice + ": processed " + done + " images" );
					}
				}

				System.out.println( "Obtained result for slice " + slice );

				final List< Tuple2< Long, HashMap< Integer, Integer > > > ret = new ArrayList<>();
				final long[] position = new long[ workingInterval.numDimensions() ], dimensions = fullTileSize;
				final RandomAccessibleInterval< HashMap< Integer, Integer > > histogramsGlobal = position.length > 2 ? Views.translate( Views.stack( histogramsImg ), new long[] { 0, 0, slice - 1 } ) : histogramsImg;
				final Cursor< HashMap< Integer, Integer > > histogramsGlobalCursor = Views.iterable( histogramsGlobal ).localizingCursor();
				while ( histogramsGlobalCursor.hasNext() )
				{
					histogramsGlobalCursor.fwd();
					histogramsGlobalCursor.localize( position );
					final long pixelGlobal = IntervalIndexer.positionToIndex( position, dimensions );
					ret.add( new Tuple2<>( pixelGlobal, histogramsGlobalCursor.get() ) );
				}

				saveSliceHistograms( dataProviderLocal, 0, slice, histogramsList.toArray( new HashMap[ 0 ] ) );
			} );
	}

	private void saveSliceHistograms( final DataProvider dataProvider, final int scale, final int slice, final HashMap[] hist ) throws IOException
	{
		final String path = generateSliceHistogramsPath( scale, slice );

//		final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();
		final MapSerializer serializer = new MapSerializer();
		serializer.setKeysCanBeNull( false );
		serializer.setKeyClass( Integer.class, kryo.getSerializer( Integer.class ) );
		serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
		kryo.register( HashMap.class, serializer );

		//try ( final Output output = kryoSerializer.newKryoOutput() )
		//{
		//	output.setOutputStream( os );

		try ( final OutputStream os = dataProvider.getOutputStream( URI.create( path ) ) )
		{
			try ( final Output output = new Output( os ) )
			{
				kryo.writeObject( output, hist );
			}
		}
	}
	private ListImg< HashMap< Integer, Integer > > readSliceHistograms( final DataProvider dataProvider, final int slice ) throws IOException
	{
		return new ListImg<>( Arrays.asList( readSliceHistogramsArray( dataProvider, 0, slice ) ), new long[] { fullTileSize[ 0 ], fullTileSize[ 1 ] } );
	}
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
		{
			// TODO: if the histograms are stored in the old format, convert them to the new N5 format
			loadHistogramsN5();
		}
		return rddHistograms;
	}

	private void loadHistogramsN5() throws IOException
	{
		final String channelDatasetPath = TileLoader.getChannelN5DatasetPath( tiles[ 0 ] );

		final List< long[] > blockGridPositions = new ArrayList<>();
		final int[] blockSize = dataProvider.createN5Reader( URI.create( histogramsPath ) ).getDatasetAttributes( channelDatasetPath ).getBlockSize();
		final CellGrid cellGrid = new CellGrid( fullTileSize, blockSize );
		for ( int index = 0; index < Intervals.numElements( cellGrid.getGridDimensions() ); ++index )
		{
			final long[] blockGridPosition = new long[ cellGrid.numDimensions() ];
			cellGrid.getCellGridPositionFlat( index, blockGridPosition );
			blockGridPositions.add( blockGridPosition );
		}

		rddHistograms = sparkContext.parallelize( blockGridPositions ) .flatMapToPair( blockGridPosition ->
					{
						final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataProviderType );
						final N5Writer n5Local = dataProviderLocal.createN5Writer( URI.create( histogramsPath ) );

						final SerializableDataBlockWrapper< HashMap< Integer, Integer > > histogramsBlock = new SerializableDataBlockWrapper<>( n5Local, channelDatasetPath, blockGridPosition );
						final WrappedListImg< HashMap< Integer, Integer > > histogramsBlockImg = histogramsBlock.wrap();

						final long[] blockPixelOffset = new long[ blockSize.length ];
						for ( int d = 0; d < blockPixelOffset.length; ++d )
							blockPixelOffset[ d ] = blockGridPosition[ d ] * blockSize[ d ];

						// TODO: when workingInterval is specified, add optimized version for loading only those blocks that fall within the desired interval
						final List< Tuple2< Long, HashMap< Integer, Integer > > > ret = new ArrayList<>();
						final ListLocalizingCursor< HashMap< Integer, Integer > > histogramsBlockImgCursor = histogramsBlockImg.localizingCursor();
						final long[] pixelPosition = new long[ blockSize.length ];
						while ( histogramsBlockImgCursor.hasNext() )
						{
							histogramsBlockImgCursor.fwd();
							histogramsBlockImgCursor.localize( pixelPosition );

							// apply block pixel offset
							for ( int d = 0; d < pixelPosition.length; ++d )
								pixelPosition[ d ] += blockPixelOffset[ d ];

							final long pixelIndex = IntervalIndexer.positionToIndex( pixelPosition, fullTileSize );
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

	private void loadHistograms()
	{
		final List< Integer > slices = new ArrayList<>();
		for ( int slice = ( workingInterval.numDimensions() > 2 ? ( int ) workingInterval.min( 2 ) + 1 : 1 ); slice <= ( workingInterval.numDimensions() > 2 ? ( int ) workingInterval.max( 2 ) + 1 : 1 ); slice++ )
			slices.add( slice );

		System.out.println( "Opening " + slices.size() + " slice histogram files" );
		final JavaRDD< Integer > rddSlices = sparkContext.parallelize( slices );

		rddHistograms = rddSlices
				.flatMapToPair( slice ->
					{
						final DataProvider dataProviderLocal = DataProviderFactory.createByType( dataProviderType );
						final RandomAccessibleInterval< HashMap< Integer, Integer > > sliceHistograms = readSliceHistograms( dataProviderLocal, slice );
						final Interval sliceInterval = Intervals.createMinMax( workingInterval.min( 0 ), workingInterval.min( 1 ), workingInterval.max( 0 ), workingInterval.max( 1 ) );
						final IntervalView< HashMap< Integer, Integer > > sliceHistogramsInterval = Views.offsetInterval( sliceHistograms, sliceInterval );
						final ListImg< Tuple2< Long, HashMap< Integer, Integer > > > ret = new ListImg<>( Intervals.dimensionsAsLongArray( sliceHistogramsInterval ), null );

						final Cursor< HashMap< Integer, Integer > > srcCursor = Views.iterable( sliceHistogramsInterval ).localizingCursor();
						final ListRandomAccess< Tuple2< Long, HashMap< Integer, Integer > > > dstRandomAccess = ret.randomAccess();

						final long[] workingDimensions = Intervals.dimensionsAsLongArray( workingInterval );

						while ( srcCursor.hasNext() )
						{
							srcCursor.fwd();
							dstRandomAccess.setPosition( srcCursor );
							dstRandomAccess.set( new Tuple2<>(
									IntervalIndexer.positionToIndex(
											new long[] { srcCursor.getLongPosition( 0 ), srcCursor.getLongPosition( 1 ), slice - 1 - ( workingInterval.numDimensions() > 2 ? workingInterval.min( 2 ) : 0 ) },
											workingDimensions ),
									srcCursor.get() ) );
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


	private boolean allHistogramsReady() throws IOException, URISyntaxException
	{
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
			if ( !dataProvider.fileExists( dataProvider.getUri( generateSliceHistogramsPath( 0, slice ) ) ) )
				return false;
		return true;
	}

	private String generateSliceHistogramsPath( final int scale, final int slice )
	{
		return histogramsPath + "/" + scale + "/" + slice + ".hist";
	}

	private int getNumSlices()
	{
		return ( int ) ( workingInterval.numDimensions() == 3 ? workingInterval.dimension( 2 ) : 1 );
	}
}
