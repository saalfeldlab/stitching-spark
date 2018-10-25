package net.imglib2.neighborsearch;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.KDIntervalTree;
import net.imglib2.KDTree;
import net.imglib2.RealInterval;
import net.imglib2.util.Intervals;

public class KDIntervalTreeSparkBroadcastingTest
{
	@Test
	public void test()
	{
		final List< Integer > values = new ArrayList<>();
		values.add( 4 );
		values.add( 2 );
		values.add( 7 );
		values.add( 4 );
		values.add( 6 );
		values.add( 5 );

		final List< RealInterval > intervals = new ArrayList<>();
		intervals.add( Intervals.createMinMaxReal( 0, 0, 10, 10 ) );
		intervals.add( Intervals.createMinMaxReal( 5.5, -2.5, 7.5, 11.5 ) );
		intervals.add( Intervals.createMinMaxReal( 12.5, 3, 12.5, 10 ) );
		intervals.add( Intervals.createMinMaxReal( 15, 15, 25, 25 ) );
		intervals.add( Intervals.createMinMaxReal( 2, 3, 6, 6 ) );
		intervals.add( Intervals.createMinMaxReal( -5, 9, 15, 15 ) );

		final KDIntervalTree< Integer > intervalTree = new KDIntervalTree<>( values, intervals );
		Assert.assertEquals( 6, intervalTree.size() );
		Assert.assertEquals( 24, intervalTree.getIntervalsCornerPointsKDTree().size() );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local" )
				.setAppName( "TestBroadcasting" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final Broadcast< KDIntervalTree< Integer > > broadcastedIntervalTree = sparkContext.broadcast( intervalTree );

			Assert.assertEquals( 6, broadcastedIntervalTree.value().size() );
			Assert.assertEquals( 24, broadcastedIntervalTree.value().getIntervalsCornerPointsKDTree().size() );

			final KDTree< KDIntervalTree< Integer >.ValueWithId >.KDTreeCursor treeCursor = intervalTree.getIntervalsCornerPointsKDTree().cursor();
			final KDTree< KDIntervalTree< Integer >.ValueWithId >.KDTreeCursor broadcastedTreeCursor = broadcastedIntervalTree.value().getIntervalsCornerPointsKDTree().cursor();

			while ( treeCursor.hasNext() || broadcastedTreeCursor.hasNext() )
			{
				treeCursor.fwd();
				broadcastedTreeCursor.fwd();

				Assert.assertEquals( treeCursor.get().id, broadcastedTreeCursor.get().id );
				Assert.assertEquals( treeCursor.get().value, broadcastedTreeCursor.get().value );
			}
		}
	}
}
