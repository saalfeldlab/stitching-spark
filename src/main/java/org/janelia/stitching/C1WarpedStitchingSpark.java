package org.janelia.stitching;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.util.Conversions;

public class C1WarpedStitchingSpark implements Serializable, AutoCloseable
{
	public static void main( final String[] args ) throws Exception
	{
		final String filteredTilesStr = args.length > 0 ? args[ 0 ] : null;
		final Set< Integer > filteredTiles;
		if ( filteredTilesStr != null )
			filteredTiles = new TreeSet<>( Arrays.asList( Conversions.toBoxedArray( Conversions.parseIntArray( filteredTilesStr.split( "," ) ) ) ) );
		else
			filteredTiles = null;

		try ( final C1WarpedStitchingSpark driver = new C1WarpedStitchingSpark() )
		{
			driver.run( filteredTiles );
		}
	}

	private static final long serialVersionUID = 6006962943789087537L;

	private C1WarpedStitchingJob job;
	private transient JavaSparkContext sparkContext;

	public void run( final Set< Integer > filteredTiles ) throws Exception
	{
		job = new C1WarpedStitchingJob( C1WarpedMetadata.getTileSlabMapping(), filteredTiles );

		final SerializableStitchingParameters params = new SerializableStitchingParameters();
		params.channel1 = 1;
		params.channel2 = 1;
		params.checkPeaks = 100;
		params.computeOverlap = true;
		params.subpixelAccuracy = true;
		params.dimensionality = C1WarpedMetadata.NUM_DIMENSIONS;
		job.setParams( params );

		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "C1WarpedStitching" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);

		final WarpedStitchingExecutor warpedStitchingExecutor = new WarpedStitchingExecutor( job, sparkContext );
		warpedStitchingExecutor.run();

		System.out.println( "Done" );
	}

	@Override
	public void close()
	{
		if ( sparkContext != null )
		{
			sparkContext.close();
			sparkContext = null;
		}
	}
}
