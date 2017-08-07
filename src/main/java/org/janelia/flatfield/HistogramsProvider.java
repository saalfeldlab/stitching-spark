package org.janelia.flatfield;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.janelia.histogram.Histogram;
import org.janelia.stitching.TileInfo;
import org.janelia.util.TiffSliceReader;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.SerializableFinalInterval;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.img.list.ListImg;
import net.imglib2.img.list.ListRandomAccess;
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
			final Interval workingInterval,
			final String histogramsPath,
			final TileInfo[] tiles,
			final long[] fullTileSize,
			final Double histMinValue, final Double histMaxValue, final int bins ) throws FileNotFoundException
	{
		this.sparkContext = sparkContext;
		this.workingInterval = workingInterval;
		this.histogramsPath = histogramsPath;
		this.tiles = tiles;
		this.fullTileSize = fullTileSize;

		this.histMinValue = histMinValue;
		this.histMaxValue = histMaxValue;
		this.bins = bins;

		// TODO: smarter handling with try/catch
//		if ( !Files.exists( Paths.get( histogramsPath ) ) )
		if ( !allHistogramsReady() )
			populateHistograms();
	}

	// parallelizing over slices + cache-friendly image loading order
	@SuppressWarnings("unchecked")
	private < T extends NativeType< T > & RealType< T > > void populateHistograms() throws FileNotFoundException
	{
		// Check for existing histograms
		final List< Integer > remainingSlices = new ArrayList<>();
		final int minSlice = ( int ) ( workingInterval.numDimensions() == 3 ? workingInterval.min( 2 ) + 1 : 1 );
		final int maxSlice = ( int ) ( workingInterval.numDimensions() == 3 ? workingInterval.max( 2 ) + 1 : 1 );
		for ( int slice = minSlice; slice <= maxSlice; slice++ )
			if ( !Files.exists( Paths.get( generateSliceHistogramsPath( 0, slice ) ) ) )
				remainingSlices.add( slice );

		final Interval workingSliceInterval = new SerializableFinalInterval(
				new long[] { workingInterval.min( 0 ), workingInterval.min( 1 ) },
				new long[] { workingInterval.max( 0 ), workingInterval.max( 1 ) } );

		System.out.println( "Populating histograms using hash maps..." );

		sparkContext.parallelize( remainingSlices ).zipWithIndex().foreach( tuple ->
			{
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
						final ImagePlus imp = TiffSliceReader.readSlice( tile.getFilePath(), slice );
						final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );
						final Cursor< T > cursor = Views.flatIterable( Views.offsetInterval( img, workingSliceInterval ) ).cursor();
						while ( cursor.hasNext() || histogramsImgCursor.hasNext() )
						{
							final int key = ( int ) cursor.next().getRealDouble();
							final HashMap< Integer, Integer > histogram = histogramsImgCursor.next();
							histogram.put( key, histogram.getOrDefault( key, 0 ) + 1 );
						}
						imp.close();
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

				saveSliceHistogramsToDisk( 0, slice, histogramsList.toArray( new HashMap[ 0 ] ) );
			} );
	}

	private void saveSliceHistogramsToDisk( final int scale, final int slice, final HashMap[] hist ) throws FileNotFoundException
	{
		final String path = generateSliceHistogramsPath( scale, slice );

		Paths.get( path ).getParent().toFile().mkdirs();

		final OutputStream os = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream( path )
						)
				);

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
		try ( final Output output = new Output( os ) )
		{
			kryo.writeObject( output, hist );
		}
	}
	private ListImg< HashMap< Integer, Integer > > readSliceHistogramsFromDisk( final int slice )
	{
		return new ListImg<>( Arrays.asList( readSliceHistogramsArrayFromDisk( 0, slice ) ), new long[] { fullTileSize[ 0 ], fullTileSize[ 1 ] } );
	}
	private HashMap[] readSliceHistogramsArrayFromDisk( final int scale, final int slice )
	{
		System.out.println( "Loading slice " + slice );
		final String path = generateSliceHistogramsPath( scale, slice );

		if ( !Files.exists(Paths.get(path)) )
			return null;

		try
		{
			final InputStream is = new DataInputStream(
					new BufferedInputStream(
							new FileInputStream( path )
							)
					);

//			final Kryo kryo = kryoSerializer.newKryo();
			final Kryo kryo = new Kryo();

			final MapSerializer serializer = new MapSerializer();
			serializer.setKeysCanBeNull( false );
			serializer.setKeyClass( Integer.class, kryo.getSerializer( Integer.class ) );
			serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
			kryo.register( HashMap.class, serializer );

			try ( final Input input = new Input( is ) )
			{
				return kryo.readObject( input, HashMap[].class );
			}
		}
		catch ( final IOException e )
		{
			e.printStackTrace();
			return null;
		}
	}


	public JavaPairRDD< Long, Histogram > getHistograms()
	{
		if ( rddHistograms == null )
			loadHistograms();
		return rddHistograms;
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
						final RandomAccessibleInterval< HashMap< Integer, Integer > > sliceHistograms = readSliceHistogramsFromDisk( slice );
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


	private boolean allHistogramsReady()
	{
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
			if ( !Files.exists( Paths.get( generateSliceHistogramsPath( 0, slice ) ) ) )
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
