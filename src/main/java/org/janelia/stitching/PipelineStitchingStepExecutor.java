package org.janelia.stitching;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;
import org.janelia.flatfield.FlatfieldCorrection;
import org.janelia.stitching.StitchingOptimizer.OptimizerMode;

import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.RandomAccessiblePairNullable;

/**
 * Computes updated tile positions using phase correlation for pairwise matches and then global optimization for fitting all of them together.
 * Saves updated tile configuration on the disk.
 *
 * @author Igor Pisarev
 */

public class PipelineStitchingStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -7152174064553332061L;

	public PipelineStitchingStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run() throws PipelineExecutionException
	{
		try
		{
			runStitchingIterations();
		}
		catch ( final IOException e )
		{
			System.out.println( "Something went wrong during stitching:" );
			e.printStackTrace();
			throw new PipelineExecutionException( e );
		}
	}

	private < U extends NativeType< U > & RealType< U > > void runStitchingIterations() throws PipelineExecutionException, IOException
	{
		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String filename = PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( 0 ) );

		final DataProvider dataProvider = job.getDataProvider();
		final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedFlatfieldsForChannels = sparkContext.broadcast( loadFlatfieldsForChannels() );
		final Broadcast< List< Map< Integer, TileInfo > > > broadcastedTileMapsForChannels = sparkContext.broadcast( createTileMapsForChannels() );

		for ( int iteration = 0; ; ++iteration )
		{
			final String iterationDirname = "iter" + iteration;
			final String stitchedTilesFilepath = PathResolver.get( basePath, iterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) );

			if ( !dataProvider.fileExists( URI.create( stitchedTilesFilepath ) ) )
			{
				final StitchingIterationPerformer< U > stitchingIterationPerformer = new StitchingIterationPerformer<>(
						job,
						sparkContext,
						iteration,
						broadcastedFlatfieldsForChannels,
						broadcastedTileMapsForChannels
					);
				stitchingIterationPerformer.run( job.getArgs().translationOnlyStitching() ? OptimizerMode.Translation : OptimizerMode.Affine );
			}
			else
			{
				System.out.println( "Stitched tiles file already exists for iteration " + iteration + ", continue..." );
			}

			// check if number of stitched tiles has increased compared to the previous iteration
			final TileInfo[] stageTiles = job.getTiles( 0 );
			final TileInfo[] stitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( stitchedTilesFilepath ) ) );

			// stop if all input tiles are included in the stitched set
			if ( stageTiles.length == stitchedTiles.length )
			{
				System.out.println( "Stopping on iteration " + iteration + ": all input tiles (n=" + stageTiles.length + ") are included in the stitched set" );
				copyFinalSolution( iteration );
				break;
			}
			else if ( iteration > 0 )
			{
				final String previousIterationDirname = "iter" + ( iteration - 1 );
				final String previousStitchedTilesFilepath = PathResolver.get( basePath, previousIterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) );
				final TileInfo[] previousStitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( previousStitchedTilesFilepath ) ) );

				//final String usedPairsFilepath = PathResolver.get( basePath, iterationDirname, "pairwise-used.json" );
				//final String previousUsedPairsFilepath = PathResolver.get( basePath, previousIterationDirname, "pairwise-used.json" );
				//final List< SerializablePairWiseStitchingResult > usedPairs = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( usedPairsFilepath ) ) );
				//final List< SerializablePairWiseStitchingResult > previousUsedPairs = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( previousUsedPairsFilepath ) ) );

				if ( stitchedTiles.length <= previousStitchedTiles.length )
				{
//					if ( stitchedTiles.length < previousStitchedTiles.length || ( stitchedTiles.length == previousStitchedTiles.length && usedPairs.size() <= previousUsedPairs.size() ) )
//					{
					/*if ( !higherOrderStitching )
					{
						higherOrderStitching = true;
					}
					else*/
					{
						// mark the last solution as not used because it is worse than from the previous iteration
						dataProvider.moveFolder(
								URI.create( PathResolver.get( basePath, iterationDirname ) ),
								URI.create( PathResolver.get( basePath, Utils.addFilenameSuffix( iterationDirname, "-notused" ) ) )
							);
						copyFinalSolution( iteration - 1 );
						System.out.println( "Stopping on iteration " + iteration + ": the new solution (n=" + stitchedTiles.length + ") is not greater than the previous solution (n=" + previousStitchedTiles.length + "). Input tiles n=" + stageTiles.length );
						break;
					}
				}
			}
		}

		broadcastedFlatfieldsForChannels.destroy();
	}

	/**
	 * Stores the final solution in the main folder when further iterations don't yield better results
	 *
	 * @param fromIteration
	 * @throws IOException
	 */
	private void copyFinalSolution( final int fromIteration ) throws IOException
	{
		final DataProvider dataProvider = job.getDataProvider();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( channel ) );
			final String filename = PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( channel ) );
			final String iterationDirname = "iter" + fromIteration;
			final String stitchedTilesFilepath = PathResolver.get( basePath, iterationDirname, Utils.addFilenameSuffix( filename, "-stitched" ) );
			final String finalTilesFilepath = PathResolver.get( basePath, Utils.addFilenameSuffix( filename, "-final" ) );
			dataProvider.copyFile( URI.create( stitchedTilesFilepath ), URI.create( finalTilesFilepath ) );

			if ( channel == 0 )
				dataProvider.copyFile(
						URI.create( PathResolver.get( basePath, iterationDirname, "optimizer.txt" ) ),
						URI.create( PathResolver.get( basePath, "optimizer-final.txt" ) )
					);
		}
	}

	/**
	 * Tries to load flatfield components for all channels.
	 *
	 * @return
	 * @throws IOException
	 */
	private < U extends NativeType< U > & RealType< U > > List< RandomAccessiblePairNullable< U, U > > loadFlatfieldsForChannels() throws PipelineExecutionException
	{
		final DataProvider dataProvider = job.getDataProvider();
		System.out.println( "Broadcasting flatfield correction images" );
		final List< RandomAccessiblePairNullable< U, U > > flatfieldsForChannels = new ArrayList<>();
		for ( final String channelPath : job.getArgs().inputTileConfigurations() )
		{
			final String channelPathNoExt = channelPath.lastIndexOf( '.' ) != -1 ? channelPath.substring( 0, channelPath.lastIndexOf( '.' ) ) : channelPath;
			// use it as a folder with the input file's name
			try
			{
				flatfieldsForChannels.add( FlatfieldCorrection.loadFlatfields( dataProvider, channelPathNoExt,  job.getDimensionality() ) );
			}
			catch ( final IOException e)
			{
				e.printStackTrace();
				throw new PipelineExecutionException( "Cannot load flatfields", e );
			}
		}
		return flatfieldsForChannels;
	}

	private List< Map< Integer, TileInfo > > createTileMapsForChannels()
	{
		final List< Map< Integer, TileInfo > > tileMapsForChannels = new ArrayList<>();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
			tileMapsForChannels.add( Utils.createTilesMap( job.getTiles( channel ) ) );
		return tileMapsForChannels;
	}
}
