package org.janelia.stitching;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.stitching.StitchingJob.PipelineStep;

public final class PipelineStepExecutorFactory
{
	final Map< PipelineStep, PipelineStepExecutor > executors;

	public PipelineStepExecutorFactory( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		executors = new HashMap<>();
		executors.put( PipelineStep.Metadata, new PipelineMetadataStepExecutor( job, sparkContext ) );
	}

	public PipelineStepExecutor getPipelineStepExecutor( final PipelineStep step )
	{
		return executors.get( step );
	}
}
