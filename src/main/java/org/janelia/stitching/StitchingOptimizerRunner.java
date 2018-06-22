package org.janelia.stitching;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;
import org.janelia.stitching.StitchingOptimizer.OptimizerMode;

public class StitchingOptimizerRunner implements Serializable, AutoCloseable
{
	public static void main( final String[] args ) throws IOException
	{
		final StitchingArguments stitchingArgs = new StitchingArguments( args );
		if ( !stitchingArgs.parsedSuccessfully() )
			System.exit( 1 );

		// check that there are no duplicated tile configurations
		if ( stitchingArgs.inputTileConfigurations().size() != new HashSet<>( stitchingArgs.inputTileConfigurations() ).size() )
			throw new RuntimeException( "some tile configurations are duplicated, please check your input arguments" );

		try ( final StitchingOptimizerRunner driver = new StitchingOptimizerRunner( stitchingArgs ) )
		{
			driver.run();
		}
	}

	private static final long serialVersionUID = 6006962943789087537L;

	private static final int iteration = 1;

	private final StitchingArguments args;
	private StitchingJob job;
	private transient JavaSparkContext sparkContext;

	public StitchingOptimizerRunner( final StitchingArguments args )
	{
		this.args = args;
	}

	public void run() throws IOException
	{
		job = new StitchingJob( args );
		try {
			final List< TileInfo[] > tilesMultichannel = new ArrayList<>();
			for ( int channel = 0; channel < job.getChannels(); channel++ ) {
				final URI tileConfigUri = URI.create( args.inputTileConfigurations().get( channel ) );
				tilesMultichannel.add( TileInfoJSONProvider.loadTilesConfiguration( job.getDataProvider().getJsonReader( tileConfigUri ) ) );
			}

			job.setTilesMultichannel( tilesMultichannel );
		} catch ( final Exception e ) {
			System.out.println( "Aborted: " + e.getMessage() );
			e.printStackTrace();
			System.exit( 2 );
		}

		final SerializableStitchingParameters params = new SerializableStitchingParameters();
		params.channel1 = 1;
		params.channel2 = 1;
		params.checkPeaks = 100;
		params.computeOverlap = true;
		params.subpixelAccuracy = true;
		job.setParams( params );

		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "StitchingOptimizer" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);

		final DataProvider dataProvider = job.getDataProvider();
		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String iterationDirname = "iter" + iteration;
		try ( final OutputStream logOut = dataProvider.getOutputStream( URI.create( PathResolver.get( basePath, iterationDirname, "optimizer.txt" ) ) ) )
		{
			try ( final PrintWriter logWriter = new PrintWriter( logOut ) )
			{
				final StitchingOptimizer optimizer = new StitchingOptimizer( job, sparkContext );
				optimizer.optimize( iteration, OptimizerMode.Affine, logWriter );
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
