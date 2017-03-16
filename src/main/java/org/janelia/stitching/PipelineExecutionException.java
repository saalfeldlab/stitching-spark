package org.janelia.stitching;

public class PipelineExecutionException extends Exception
{
	private static final long serialVersionUID = -2015347403889233169L;

	public PipelineExecutionException()
	{
        super();
    }

    public PipelineExecutionException( final String message )
    {
        super( message );
    }

    public PipelineExecutionException( final String message, final Throwable cause )
    {
        super( message, cause );
    }

    public PipelineExecutionException( final Throwable cause )
    {
        super( cause );
    }
}
