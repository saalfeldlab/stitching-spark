package org.janelia.flatfield;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.Cursor;
import net.imglib2.FinalDimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class KryoSerializationTest
{
	@Test
	public void testRAI()
	{
		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local" )
				.setAppName( "TestRAISerializationWithKryo" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.registerKryoClasses( new Class[] { Integer.class, RandomAccessibleInterval.class, int[].class } )
			) )
		{
			final Random rnd = new Random();
			final long[] dimensions = new long[ rnd.nextInt( 3 ) + 1 ];
			for ( int d = 0; d < dimensions.length; ++d )
				dimensions[ d ] = rnd.nextInt( 10 ) + 1;
			final int[] image = new int[ ( int ) Intervals.numElements( new FinalDimensions( dimensions ) ) ];
			for ( int i = 0; i < image.length; ++i )
				image[ i ] = rnd.nextInt( 1000 );
			final RandomAccessibleInterval< IntType > rai = ArrayImgs.ints( image, dimensions );

			final Broadcast< RandomAccessibleInterval< IntType > > raiBroadcast = sparkContext.broadcast( rai );
			final List< int[] > result = sparkContext.parallelize( Collections.singletonList( 0 ) ).map( i ->
				{
					final RandomAccessibleInterval< IntType > raiLocal = raiBroadcast.value();
					final int[] arr = new int[ ( int ) Intervals.numElements( raiLocal ) ];
					final Cursor< IntType > cursor = Views.iterable( raiLocal ).localizingCursor();
					final int[] pos = new int[ raiLocal.numDimensions() ], dim = Intervals.dimensionsAsIntArray( raiLocal );
					while ( cursor.hasNext() )
					{
						cursor.fwd();
						cursor.localize( pos );
						arr[ IntervalIndexer.positionToIndex( pos, dim ) ] = cursor.get().get();
					}
					return arr;
				} )
			.collect();

			Assert.assertEquals( 1, result.size() );
			Assert.assertArrayEquals( image, result.get( 0 ) );
		}
	}
}
