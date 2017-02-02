package org.janelia.stitching;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.Intervals;
import scala.Tuple2;

public class SparkTest
{
	private transient Interval test = Intervals.createMinSize(0,0,0,3,4,5);

	@Test
	public void test()
	{
		final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "TestImmutability" )
				.setMaster( "local[1]" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.registerKryoClasses( new Class[] { Short.class, Integer.class, Long.class, Double.class, TreeMap.class, TreeMap[].class, double[].class, List.class, Tuple2.class, Interval.class, FinalInterval.class, ArrayImg.class, DoubleType.class, DoubleArray.class, long[].class } )
				.set( "spark.rdd.compress", "true" )
			);

//		final ArrayImg< DoubleType, DoubleArray > test = ArrayImgs.doubles( 3,4,5 );
//		final Broadcast< ArrayImg< DoubleType, DoubleArray > > broadcastTest = sparkContext.broadcast( test );

		final Broadcast< Interval > broadcastTest = sparkContext.broadcast( test );

		final Random rnd = new Random();
		final List< Tuple2< Integer, Integer > > data = new ArrayList<>();
		for ( int i = 0; i < 1000; i++ )
			data.add( new Tuple2<>( rnd.nextInt( 10000 ), rnd.nextInt( 200 ) ) );
		final JavaPairRDD< Integer, long[] > rdd = sparkContext.parallelizePairs( data ).mapToPair( tuple ->
			{
				final long[] arr = new long[ tuple._2() ];
				for ( int i = 0; i < arr.length; i++ )
					arr[ i ] = tuple._1() * i;
				return new Tuple2<>( tuple._1(), arr );
			} );
		rdd.cache();

		final Map< Integer, long[] > rddInitialMap = rdd.collectAsMap();

		final long count = rdd.map( tuple ->
			{
				final long[] arr = tuple._2().clone();
				int counter = 0;
				for ( int i = 0; i < arr.length; i++, counter++ )
					arr[ i ] = 0;
				return tuple._1() * counter;
			} ).distinct().count();

		System.out.println( "Original count = " + data.size() + ", distinct count = " + count );

		final Map< Integer, long[] > rddNowMap = rdd.collectAsMap();

		Assert.assertEquals( rddInitialMap.size(), rddNowMap.size() );
		Assert.assertArrayEquals( new TreeMap<>( rddInitialMap ).keySet().toArray(), new TreeMap<>( rddNowMap ).keySet().toArray() );
//		for ( final Integer key : rddInitialMap.keySet() )
//		{
//			System.out.println( "For key="+key+":" );
//			System.out.println( "was="+Arrays.toString( rddInitialMap.get( key ) ));
//			System.out.println( "now="+Arrays.toString( rddNowMap.get( key ) ));
//			System.out.println( "----------------" );
//			Assert.assertArrayEquals( rddInitialMap.get( key ), rddNowMap.get( key ) );
//		}

		sparkContext.close();
	}
}
