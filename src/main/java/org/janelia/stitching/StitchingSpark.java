package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.stitching.StitchingJob.PipelineStep;

import mpicbg.stitching.StitchingParameters;

/**
 * Driver class for running stitching jobs on a Spark cluster.
 *
 * @author Igor Pisarev
 */

public class StitchingSpark implements Serializable, AutoCloseable
{
	public static void main( final String[] args ) throws IllegalArgumentException, IOException
	{
		final StitchingArguments stitchingArgs = new StitchingArguments( args );
		if ( !stitchingArgs.parsedSuccessfully() )
			throw new IllegalArgumentException( "argument format mismatch" );

		// check that there are no duplicated tile configurations
		if ( stitchingArgs.inputTileConfigurations().size() != new HashSet<>( stitchingArgs.inputTileConfigurations() ).size() )
			throw new RuntimeException( "some tile configurations are duplicated, please check your input arguments" );

		try ( final StitchingSpark driver = new StitchingSpark( stitchingArgs ) )
		{
			driver.run();
		}
	}

	private static final long serialVersionUID = 6006962943789087537L;

	private final StitchingArguments args;
	private StitchingJob job;
	private transient JavaSparkContext sparkContext;

	public StitchingSpark( final StitchingArguments args )
	{
		this.args = args;
	}

	public void run()
	{
		job = new StitchingJob( args );
		try {
			final List< TileInfo[] > tilesMultichannel = new ArrayList<>();
			for ( int channel = 0; channel < job.getChannels(); channel++ ) {
				final String tileConfigLink = args.inputTileConfigurations().get( channel );
				tilesMultichannel.add( job.getDataProvider().loadTiles( tileConfigLink ) );
			}

			job.setTilesMultichannel( tilesMultichannel );
		} catch ( final Exception e ) {
			System.out.println( "Aborted: " + e.getMessage() );
			e.printStackTrace();
			System.exit( 2 );
		}

		final StitchingParameters params = new StitchingParameters();
		params.channel1 = 1;
		params.channel2 = 1;
		params.checkPeaks = 50;
		params.computeOverlap = true;
		params.subpixelAccuracy = true;
		params.virtual = false;
		params.absoluteThreshold = 5;
		params.relativeThreshold = 3;
		job.setParams( params );

		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "Stitching" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);

		final PipelineStepExecutorFactory pipelineExecutorFactory = new PipelineStepExecutorFactory( job, sparkContext );
		for ( final PipelineStep step : job.getPipeline() )
		{
			try
			{
				pipelineExecutorFactory.getPipelineStepExecutor( step ).run();
			}
			catch ( final PipelineExecutionException e )
			{
				e.printStackTrace();
				System.out.println( "Pipeline execution exception: " + e.getMessage() );
				return;
			}
		}

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
