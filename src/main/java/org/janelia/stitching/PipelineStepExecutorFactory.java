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
		executors.put( PipelineStep.Blur, new PipelineBlurStepExecutor( job, sparkContext ) );
		executors.put( PipelineStep.Shift, new PipelineShiftStepExecutor( job, sparkContext ) );
		executors.put( PipelineStep.IntensityCorrection, new PipelineIntensityCorrectionStepExecutor( job, sparkContext ) );
		executors.put( PipelineStep.Fusion, new PipelineFusionStepExecutor( job, sparkContext ) );
		executors.put( PipelineStep.Export, new PipelineExportStepExecutor( job, sparkContext ) );
	}

	public PipelineStepExecutor getPipelineStepExecutor( final PipelineStep step )
	{
		return executors.get( step );
	}
}