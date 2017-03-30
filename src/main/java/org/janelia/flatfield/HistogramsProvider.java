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
import java.util.TreeSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.janelia.histogram.Histogram;
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

	private Double histMinValue, histMaxValue;
	private final int bins;

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

		if ( !allHistogramsReady() )
			populateHistograms();
	}

	// TreeMap
	@SuppressWarnings("unchecked")
	private <
		T extends NativeType< T > & RealType< T >,
		V extends TreeMap< Integer, Integer > >
	void populateHistograms() throws FileNotFoundException
	{
		final JavaRDD< TileInfo > rddTiles = sparkContext.parallelize( Arrays.asList( tiles ) );

		// Check for existing histograms
		final Set< Integer > remainingSlices = new HashSet<>();
		for ( int slice = 1; slice <= getNumSlices(); slice++ )
			if ( !Files.exists( Paths.get( generateSliceHistogramsPath( 0, slice ) ) ) )
				remainingSlices.add( slice );

		System.out.println( " Remaining slices count = " + remainingSlices.size() );

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
						ret = ( V[] ) new TreeMap[ ( int ) img.size() ];
						for ( int i = 0; i < ret.length; i++ )
							ret[ i ] = ( V ) new TreeMap< Integer, Integer >();
					}

					while ( cursor.hasNext() )
					{
						cursor.fwd();
						cursor.localize( position );
						final int pixel = IntervalIndexer.positionToIndex( position, dimensions );
						final int key = ( int ) cursor.get().getRealDouble();
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
						for ( final Entry< Integer, Integer > entry : b[ pixel ].entrySet() )
							a[ pixel ].put( entry.getKey(), a[ pixel ].getOrDefault( entry.getKey(), 0 ) + entry.getValue() );
					return a;
				},

				Integer.MAX_VALUE ); // max possible aggregation depth

			System.out.println( "Obtained result for slice " + currentSlice + ", saving..");

			saveSliceHistogramsToDisk( 0, currentSlice, histograms );
		}
	}

	public JavaPairRDD< Long, Histogram > getHistograms()
	{
		if ( rddHistograms == null )
			loadHistograms();
		return rddHistograms;
	}
	private < V extends TreeMap< Integer, Integer > > void loadHistograms()
	{
		final List< Integer > slices = new ArrayList<>();
		for ( int slice = ( int ) workingInterval.min( 2 ) + 1; slice <= ( int ) workingInterval.max( 2 ) + 1; slice++ )
			slices.add( slice );

		System.out.println( "Opening " + slices.size() + " slice histogram files" );
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

		if ( histMinValue == null || histMaxValue == null )
		{
			rddTreeMaps.persist( StorageLevel.MEMORY_ONLY_SER() );

			// extract value statistics accumulating all histograms together
			final TreeMap< Integer, Long > valueStats = rddTreeMaps.map( tuple ->
				{
					final TreeMap< Integer, Long > ret = new TreeMap<>();
					for ( final Entry< Integer, Integer > entry : tuple._2().entrySet() )
						ret.put( entry.getKey(), entry.getValue().longValue() );
					return ret;
				} )
			.treeReduce( ( ret, other ) ->
				{
					for ( final Entry< Integer, Long > entry : other.entrySet() )
						ret.put( entry.getKey(), ret.getOrDefault( entry.getKey(), 0l ) + entry.getValue() );
					return ret;
				} );
			System.out.println();
			System.out.println( "Value stats:" );
			for ( final Entry< Integer, Long > entry : valueStats.entrySet() )
				System.out.println( entry.getKey() + ": " + entry.getValue() );
			System.out.println();

			// extract 10 min/max values from the histograms
			final int statsCount = 10;
			final Tuple2< TreeSet< Integer >, TreeSet< Integer > > histMinsMaxs = rddTreeMaps
					.map( tuple -> tuple._2() )
					.treeAggregate(
						new Tuple2<>( new TreeSet<>(), new TreeSet<>() ),
						( ret, map ) ->
							{
								final TreeSet< Integer > mins = ret._1(), maxs = ret._2();
								for ( final Integer key : map.keySet() )
								{
									mins.add( key );
									while ( mins.size() > statsCount )
										mins.remove( mins.last() );

									maxs.add( key );
									while ( maxs.size() > statsCount )
										maxs.remove( maxs.first() );
								}
								return new Tuple2<>( mins, maxs );
							},
						( ret, val ) ->
							{
								final TreeSet< Integer > mins = ret._1(), maxs = ret._2();

								mins.addAll( val._1() );
								while ( mins.size() > statsCount )
									mins.remove( mins.last() );

								maxs.addAll( val._2() );
								while ( maxs.size() > statsCount )
									maxs.remove( maxs.first() );

								return ret;
							},
							Integer.MAX_VALUE ); // max possible aggregation depth
			System.out.println();
			System.out.println( "Min " + statsCount + " values: " + histMinsMaxs._1() );
			System.out.println( "Max " + statsCount + " values: " + histMinsMaxs._2().descendingSet() );
			System.out.println();
			final Tuple2< Integer, Integer > histMinMax = new Tuple2<>( histMinsMaxs._1().first(), histMinsMaxs._2().last() );


			histMinValue = new Double( histMinMax._1() );
			histMaxValue = new Double( histMinMax._2() );
		}

		System.out.println( "Histograms min=" + histMinValue + ", max=" + histMaxValue );

		rddHistograms = rddTreeMaps
				.mapValues( map ->
					{
						final Histogram histogram = new Histogram( histMinValue, histMaxValue, bins );
						for ( final Entry< Integer, Integer > entry : map.entrySet() )
							histogram.put( entry.getKey(), entry.getValue() );
						return histogram;
					} );

		// enforce the computation so we can unpersist the parent RDD after that
		System.out.println( "Total histograms (pixels) count = " + rddHistograms.persist( StorageLevel.MEMORY_ONLY_SER() ).count() );

		rddTreeMaps.unpersist();
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
					for ( int i = 0; i < histogram.getNumBins(); ++i )
						ret.set( i, ret.get( i ) + histogram.get( i ) );
					return ret;
				},
				Integer.MAX_VALUE // max possible aggregation depth
			);

		for ( int i = 0; i < accumulatedHistograms.getNumBins(); ++i )
			accumulatedHistograms.set( i, accumulatedHistograms.get( i ) / numMedianPoints );

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

	private < V extends TreeMap< Integer, Integer > > void saveSliceHistogramsToDisk( final int scale, final int slice, final V[] hist ) throws FileNotFoundException
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
		kryo.register( TreeMap.class, serializer );

		//try ( final Output output = kryoSerializer.newKryoOutput() )
		//{
		//	output.setOutputStream( os );
		try ( final Output output = new Output( os ) )
		{
			kryo.writeClassAndObject( output, hist );
		}
	}

	private < V extends TreeMap< Integer, Integer > > ListImg< V > readSliceHistogramsFromDisk( final int slice )
	{
		return new ListImg<>( Arrays.asList( readSliceHistogramsArrayFromDisk( 0, slice ) ), new long[] { fullTileSize[ 0 ], fullTileSize[ 1 ] } );
	}
	@SuppressWarnings("unchecked")
	private < V extends TreeMap< Integer, Integer > > V[] readSliceHistogramsArrayFromDisk( final int scale, final int slice )
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
