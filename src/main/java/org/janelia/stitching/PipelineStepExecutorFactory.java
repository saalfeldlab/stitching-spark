package org.janelia.stitching;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.stitching.StitchingJob.PipelineStep;

/**
 * Simplifies obtaining a concrete pipeline step executor.
 *
 * @author Igor Pisarev
 */

public final class PipelineStepExecutorFactory
{
	final Map< PipelineStep, PipelineStepExecutor > executors;

	public PipelineStepExecutorFactory( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		executors = new HashMap<>();
		executors.put( PipelineStep.Metadata, new PipelineMetadataStepExecutor( job, sparkContext ) );
		executors.put( PipelineStep.Stitching, new PipelineStitchingStepExecutor( job, sparkContext ) );

		// TODO: Disabled for now since we have switched to illumination correction and it is applied on the Shift & Fuse phases
		//executors.put( PipelineStep.IntensityCorrection, new PipelineIntensityCorrectionStepExecutor( job, sparkContext ) );

		executors.put( PipelineStep.Fusion, new PipelineFusionStepExecutor( job, sparkContext ) );
		executors.put( PipelineStep.Export, new PipelineExportStepExecutor( job, sparkContext ) );
	}

	public PipelineStepExecutor getPipelineStepExecutor( final PipelineStep step )
	{
		return executors.get( step );
	}
}
