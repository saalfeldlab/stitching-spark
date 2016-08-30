package org.janelia.stitching;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;

public abstract class PipelineStepExecutor implements Runnable, Serializable
{
	private static final long serialVersionUID = 3546355803511705943L;

	protected final StitchingJob job;
	protected final JavaSparkContext sparkContext;

	public PipelineStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		this.job = job;
		this.sparkContext = sparkContext;
	}

	/*protected PipelineStep()
	{
		this.job = null;
		this.sparkContext = null;
	}*/
}
