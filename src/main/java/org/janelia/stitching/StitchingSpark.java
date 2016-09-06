package org.janelia.stitching;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.stitching.StitchingJob.PipelineStep;

import mpicbg.stitching.StitchingParameters;

/**
 * Driver class for running stitching jobs on a Spark cluster.
 *
 * @author Igor Pisarev
 */

public class StitchingSpark implements Runnable, Serializable {

	public static void main( final String[] args ) {
		final StitchingArguments stitchingArgs = new StitchingArguments( args );
		if ( !stitchingArgs.parsedSuccessfully() )
			System.exit( 1 );

		final StitchingSpark driver = new StitchingSpark( stitchingArgs );
		driver.run();
	}

	private static final long serialVersionUID = 6006962943789087537L;

	private final StitchingArguments args;
	private StitchingJob job;
	private transient JavaSparkContext sparkContext;

	public StitchingSpark( final StitchingArguments args ) {
		this.args = args;
	}

	@Override
	public void run()
	{
		job = new StitchingJob( args );
		try {
			job.setTiles( TileInfoJSONProvider.loadTilesConfiguration( args.inputFilePath() ) );
		} catch ( final Exception e ) {
			System.out.println( "Aborted: " + e.getMessage() );
			e.printStackTrace();
			System.exit( 2 );
		}

		final StitchingParameters params = new StitchingParameters();
		params.channel1 = 0;
		params.channel2 = 0;
		params.timeSelect = 0;
		params.checkPeaks = 100;
		params.computeOverlap = true;
		params.subpixelAccuracy = false;
		params.virtual = true;
		params.absoluteThreshold = 7;
		params.relativeThreshold = 5;
		job.setParams( params );

		sparkContext = new JavaSparkContext( new SparkConf().setAppName( "Stitching" ) );

		final PipelineStepExecutorFactory pipelineExecutorFactory = new PipelineStepExecutorFactory( job, sparkContext );
		for ( final PipelineStep step : job.getPipeline() )
			pipelineExecutorFactory.getPipelineStepExecutor( step ).run();

		sparkContext.close();
		System.out.println( "done" );
	}
}
