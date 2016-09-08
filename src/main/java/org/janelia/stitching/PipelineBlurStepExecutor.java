package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import ij.IJ;
import ij.ImagePlus;
import ij.Prefs;
import ij.plugin.filter.GaussianBlur;

/**
 * Applies gaussian blur to the input tile images.
 * Stores them separately on the disk along with new tile configuration.
 *
 * @author Igor Pisarev
 */

public class PipelineBlurStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = 9192968542898051534L;

	public PipelineBlurStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run()
	{
		final String blurredFolder = job.getBaseFolder() + "/blurred";
		new File( blurredFolder ).mkdirs();

		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( job.getTiles() ) );
		final JavaRDD< TileInfo > task = rdd.map(
				new Function< TileInfo, TileInfo >()
				{
					private static final long serialVersionUID = 772321548054114279L;

					@Override
					public TileInfo call( final TileInfo tile ) throws Exception
					{
						System.out.println( "Blurring tile " + tile.getIndex() );
						final ImagePlus imp = IJ.openImage( tile.getFilePath() );
						Utils.workaroundImagePlusNSlices( imp );

						Prefs.setThreads( 1 );
						final GaussianBlur blur = new GaussianBlur();
						blur.setup( "", imp );
						for ( int slice = 1; slice <= imp.getNSlices(); slice++ )
						{
							imp.setSlice( slice );
							blur.blurGaussian( imp.getProcessor(), job.getArgs().blurStrength() );
						}

						System.out.println( "Saving blurred tile " + tile.getIndex() );
						final TileInfo blurredTile = tile.clone();
						blurredTile.setFilePath( blurredFolder + "/" + Utils.addFilenameSuffix( new File( tile.getFilePath() ).getName(), "_blurred" ) );
						IJ.saveAsTiff( imp, blurredTile.getFilePath() );
						return blurredTile;
					}
				});

		final List< TileInfo > blurredTiles = task.collect();

		System.out.println( "Obtained blurred tiles" );

		final Map< Integer, TileInfo > tilesMap = job.getTilesMap();
		for ( final TileInfo blurredTile : blurredTiles )
			tilesMap.get( blurredTile.getIndex() ).setFilePath( blurredTile.getFilePath() );

		try {
			TileInfoJSONProvider.saveTilesConfiguration( job.getTiles(), Utils.addFilenameSuffix( Utils.removeFilenameSuffix( job.getArgs().inputFilePath(), "_full" ), "_blurred" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
}
