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
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.janelia.stitching.TiffSliceLoader;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.Utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
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
	private transient final TileInfo[] tiles;
	private final Interval workingInterval;
	private final String histogramsPath;
	private final long[] fullTileSize;
	private HistogramSettings histogramSettings;

	private transient JavaPairRDD< Long, long[] > rddHistograms;
	private transient long[] referenceHistogram;

	public HistogramsProvider(
			final JavaSparkContext sparkContext,
			final Interval workingInterval,
			final String histogramsPath,
			final TileInfo[] tiles,
			final long[] fullTileSize,
			final Integer histMinValue, final Integer histMaxValue, final int bins ) throws FileNotFoundException
	{
		this.sparkContext = sparkContext;
		this.workingInterval = workingInterval;
		this.histogramsPath = histogramsPath;
		this.tiles = tiles;
		this.fullTileSize = fullTileSize;

		histogramSettings = new HistogramSettings(
				histMinValue != null ? histMinValue : Integer.MAX_VALUE,
				histMaxValue != null ? histMaxValue : Integer.MIN_VALUE,
				bins );

		if ( !allHistogramsReady() )
			populateHistograms();
	}

	public HistogramSettings getHistogramSettings()
	{
		return histogramSettings;
	}

	// TreeMap
	@SuppressWarnings("unchecked")
	private <
		T extends NativeType< T > & RealType< T >,
		V extends TreeMap< Short, Integer > >
	void populateHistograms() throws FileNotFoundException
	{
		final JavaRDD< TileInfo > rddTiles = sparkContext.parallelize( Arrays.asList( tiles ) );

		// Check for existing histograms
		final Set< Integer > remainingSlices = new HashSet<>();
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
			if ( !Files.exists( Paths.get( generateSliceHistogramsPath( 0, slice ) ) ) )
				remainingSlices.add( slice );

		for ( final int currentSlice : remainingSlices )
		{
			System.out.println( "  Processing slice " + currentSlice );

			final V[] histograms = rddTiles.treeAggregate(
				null, // zero value

				// generator
				( intermediateHist, tile ) ->
				{
					final ImagePlus imp = TiffSliceLoader.loadSlice( tile, currentSlice );
					Utils.workaroundImagePlusNSlices( imp );

					final Img< T > img = ImagePlusImgs.from( imp );
					final Cursor< T > cursor = Views.iterable( img ).localizingCursor();
					final int[] dimensions = Intervals.dimensionsAsIntArray( img );
					final int[] position = new int[ dimensions.length ];

					final V[] ret;
					if ( intermediateHist != null )
					{
						ret = intermediateHist;
					}
					else
					{
						ret = ( V[] ) new TreeMap[ (int) img.size() ];
						for ( int i = 0; i < ret.length; i++ )
							ret[ i ] = ( V ) new TreeMap< Short, Integer >();
					}

					while ( cursor.hasNext() )
					{
						cursor.fwd();
						cursor.localize( position );
						final int pixel = IntervalIndexer.positionToIndex( position, dimensions );
						final short key = ( short ) cursor.get().getRealDouble();
						ret[ pixel ].put( key, ret[ pixel ].getOrDefault( key, 0 ) + 1 );
					}

					imp.close();
					return ret;
				},

				// reducer
				( a, b ) ->
				{
					if ( a == null )
						return b;
					else if ( b == null )
						return a;

					for ( int pixel = 0; pixel < b.length; pixel++ )
						for ( final Entry< Short, Integer > entry : b[ pixel ].entrySet() )
							a[ pixel ].put( entry.getKey(), a[ pixel ].getOrDefault( entry.getKey(), 0 ) + entry.getValue() );
					return a;
				},

				getAggregationTreeDepth() );

			System.out.println( "Obtained result for slice " + currentSlice + ", saving..");

			saveSliceHistogramsToDisk( 0, currentSlice, histograms );
		}
	}

	public JavaPairRDD< Long, long[] > getHistograms()
	{
		if ( rddHistograms == null )
			loadHistograms();
		return rddHistograms;
	}
	private < V extends TreeMap< Short, Integer > > void loadHistograms()
	{
		final List< Integer > slices = new ArrayList<>();
		for ( int slice = ( int ) workingInterval.min( 2 ) + 1; slice <= ( int ) workingInterval.max( 2 ) + 1; slice++ )
			slices.add( slice );

		final JavaRDD< Integer > rddSlices = sparkContext.parallelize( slices );

		final JavaPairRDD< Long, V > rddTreeMaps = rddSlices
				.flatMapToPair( slice ->
					{
						final RandomAccessibleInterval< V > sliceHistograms = readSliceHistogramsFromDisk( slice );
						final Interval sliceInterval = Intervals.createMinMax( workingInterval.min( 0 ), workingInterval.min( 1 ), workingInterval.max( 0 ), workingInterval.max( 1 ) );
						final IntervalView< V > sliceHistogramsInterval = Views.offsetInterval( sliceHistograms, sliceInterval );
						final ListImg< Tuple2< Long, V > > ret = new ListImg<>( Intervals.dimensionsAsLongArray( sliceHistogramsInterval ), null );

						final Cursor< V > srcCursor = Views.iterable( sliceHistogramsInterval ).localizingCursor();
						final ListRandomAccess< Tuple2< Long, V > > dstRandomAccess = ret.randomAccess();

						final long[] workingDimensions = Intervals.dimensionsAsLongArray( workingInterval );

						while ( srcCursor.hasNext() )
						{
							srcCursor.fwd();
							dstRandomAccess.setPosition( srcCursor );
							dstRandomAccess.set( new Tuple2<>(
									IntervalIndexer.positionToIndex(
											new long[] { srcCursor.getLongPosition( 0 ), srcCursor.getLongPosition( 1 ), slice - 1 - workingInterval.min( 2 ) },
											workingDimensions ),
									srcCursor.get() ) );
						}
						return ret.iterator();
					} );

		if ( histogramSettings.minValue == Integer.MAX_VALUE || histogramSettings.maxValue == Integer.MIN_VALUE )
		{
			// extract min/max value from the histograms
			rddTreeMaps.persist( StorageLevel.MEMORY_ONLY_SER() );

			final Tuple2< Integer, Integer > histMinMax = rddTreeMaps
					.map( tuple -> tuple._2() )
					.treeAggregate(
						new Tuple2<>( Integer.MAX_VALUE, Integer.MIN_VALUE ),
						( ret, map ) ->
							{
								int min = ret._1(), max = ret._2();
								for ( final Short key : map.keySet() )
								{
									final int unsignedKey = shortToUnsigned( key );
									min = Math.min( unsignedKey, min );
									max = Math.max( unsignedKey, max );
								}
								return new Tuple2<>( min, max );
							},
						( ret, val ) -> new Tuple2<>( Math.min( ret._1(), val._1() ), Math.max( ret._2(), val._2() ) ),
						getAggregationTreeDepth() );

			histogramSettings = new HistogramSettings(
					histMinMax._1(),
					histMinMax._2(),
					histogramSettings.bins );
		}

		System.out.println( "Histograms min=" + histogramSettings.minValue + ", max=" + histogramSettings.maxValue );

		rddHistograms = rddTreeMaps
				.mapValues( histogram ->
					{
						final long[] array = new long[ histogramSettings.bins ];
						for ( final Entry< Short, Integer > entry : histogram.entrySet() )
							array[ histogramSettings.getBinIndex( shortToUnsigned( entry.getKey() ) ) ] += entry.getValue();
						return array;
					} );

		// enforce the computation so we can unpersist the parent RDD after that
		System.out.println( "Total histograms (pixels) count = " + rddHistograms.persist( StorageLevel.MEMORY_ONLY_SER() ).count() );

		rddTreeMaps.unpersist();

		// for testing purposes
		final Tuple2< Long, Long > testing = rddHistograms
			.map( tuple -> new Tuple2<>( tuple._2()[ 0 ], tuple._2()[ tuple._2().length - 1 ] ) )
			.reduce( ( a, b ) -> new Tuple2<>( Math.max( a._1(), b._1() ), Math.max( a._2(), b._2() ) ) );
		System.out.println();
		System.out.println( "Max at 0: " + testing._1() );
		System.out.println( "Max at " + ( histogramSettings.bins - 1 ) +": " + testing._2() );
		System.out.println( "out of " + tiles.length + " elements" );
		System.out.println();
	}


	public long[] getReferenceHistogram()
	{
		if ( referenceHistogram == null )
			estimateReferenceHistogram();
		return referenceHistogram;
	}
	private void estimateReferenceHistogram()
	{
		final long numPixels = Intervals.numElements( workingInterval );
		final int N = tiles.length;

		int numWindowPoints = ( int ) Math.round( numPixels * REFERENCE_HISTOGRAM_POINTS_PERCENT );
		final int mStart = ( int ) ( Math.round( numPixels / 2.0 ) - Math.round( numWindowPoints / 2.0 ) ) - 1;
		final int mEnd   = ( int ) ( Math.round( numPixels / 2.0 ) + Math.round( numWindowPoints / 2.0 ) );
		numWindowPoints = mEnd - mStart;

		System.out.println("Estimating Q using mStart="+mStart+", mEnd="+mEnd+" (points="+numWindowPoints+")");

		final long[] referenceVector = rddHistograms
			.mapValues( histogram ->
				{
					double pixelMean = 0;
					int count = 0;
					for ( int i = 0; i < histogram.length; i++ )
					{
						pixelMean += histogram[ i ] * histogramSettings.getBinValue( i );
						count += histogram[ i ];
					}
					pixelMean /= count;
					return pixelMean;
				}
			)
			.mapToPair( pair -> pair.swap() )
			.sortByKey()
			.zipWithIndex()
			.filter( tuple -> tuple._2() >= mStart && tuple._2() < mEnd )
			.mapToPair( tuple -> tuple._1().swap() )
			.join( rddHistograms )
			.map( item -> item._2()._2() )
			.treeAggregate(
				new long[ N ],
				( ret, histogram ) ->
				{
					int counter = 0;
					for ( int i = 0; i < histogram.length; i++ )
						for ( int j = 0; j < histogram[ i ]; j++ )
							ret[ counter++ ] += histogramSettings.getBinValue( i );
					return ret;
				},
				( ret, other ) ->
				{
					for ( int i = 0; i < N; i++ )
						ret[ i ] += other[ i ];
					return ret;
				},
				getAggregationTreeDepth()
			);

		referenceHistogram = new long[ histogramSettings.bins ];
		for ( final long val : referenceVector )
			referenceHistogram[ histogramSettings.getBinIndex( ( short ) Math.round( ( double ) val / numWindowPoints ) ) ]++;
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

	private < V extends TreeMap< Short, Integer > > void saveSliceHistogramsToDisk( final int scale, final int slice, final V[] hist ) throws FileNotFoundException
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
		serializer.setKeyClass( Short.class, kryo.getSerializer( Short.class ) );
		serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
		kryo.register( TreeMap.class, serializer );

		//try ( final Output output = kryoSerializer.newKryoOutput() )
		//{
		//	output.setOutputStream( os );
		try ( final Output output = new Output( os ) )
		{
			kryo.writeClassAndObject( output, hist );
		}
	}

	private < V extends TreeMap< Short, Integer > > ListImg< V > readSliceHistogramsFromDisk( final int slice )
	{
		return new ListImg<>( Arrays.asList( readSliceHistogramsArrayFromDisk( 0, slice ) ), new long[] { fullTileSize[ 0 ], fullTileSize[ 1 ] } );
	}
	@SuppressWarnings("unchecked")
	private < V extends TreeMap< Short, Integer > > V[] readSliceHistogramsArrayFromDisk( final int scale, final int slice )
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
			serializer.setKeyClass( Short.class, kryo.getSerializer( Short.class ) );
			serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
			kryo.register( TreeMap.class, serializer );

			try ( final Input input = new Input( is ) )
			{
				return ( V[] ) kryo.readClassAndObject( input );
			}
		}
		catch ( final IOException e )
		{
			e.printStackTrace();
			return null;
		}
	}


	private int getAggregationTreeDepth()
	{
		return ( int ) Math.ceil( Math.log( sparkContext.defaultParallelism() ) / Math.log( 2 ) );
	}

	private static int shortToUnsigned( final short value )
	{
		return value + ( value >= 0 ? 0 : ( ( 1 << Short.SIZE ) ) );
	}




	// Arrays, slow??
	/*private < T extends NativeType< T > & RealType< T > > void populateHistograms() throws Exception
	{
		final JavaRDD< TileInfo > rddTiles = sparkContext.parallelize( Arrays.asList( tiles ) );

		// Check for existing histograms
		final Set< Integer > remainingSlices = new HashSet<>();
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
			if ( !Files.exists( Paths.get( generateSliceHistogramsPath( 0, slice ) ) ) )
				remainingSlices.add( slice );

		final int slicePixelsCount = ( int ) ( workingInterval.dimension( 0 ) * workingInterval.dimension( 1 ) );

		for ( final int currentSlice : remainingSlices )
		{
			System.out.println( "  Processing slice " + currentSlice );

			final short[][] histograms = rddTiles.treeAggregate(
				new short[ slicePixelsCount ][ bins ],	// zero value

				// generator
				( intermediateHist, tile ) ->
				{
					final ImagePlus imp = TiffSliceLoader.loadSlice( tile, currentSlice );
					Utils.workaroundImagePlusNSlices( imp );

					final Img< T > img = ImagePlusImgs.from( imp );
					final Cursor< T > cursor = Views.iterable( img ).localizingCursor();
					final int[] dimensions = Intervals.dimensionsAsIntArray( img );
					final int[] position = new int[ dimensions.length ];

					while ( cursor.hasNext() )
					{
						cursor.fwd();
						cursor.localize( position );
						final int pixel = IntervalIndexer.positionToIndex( position, dimensions );
						final short val = ( short ) cursor.get().getRealDouble();
						intermediateHist[ pixel ][ getBinIndex( val ) ]++;
					}

					imp.close();
					return intermediateHist;
				},

				// reducer
				( a, b ) ->
				{
					for ( int pixel = 0; pixel < a.length; pixel++ )
						for ( int i = 0; i < bins; i++ )
						a[ pixel ][ i ] += b[ pixel ][ i ];
					return a;
				},

				getAggregationTreeDepth() );

			System.out.println( "Obtained result for slice " + currentSlice + ", saving..");

			saveSliceArrayHistogramsToDisk( 0, currentSlice, histograms );
		}
	}*/
}
