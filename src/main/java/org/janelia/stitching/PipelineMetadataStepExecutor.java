package org.janelia.stitching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.janelia.util.Conversions;

import ij.ImagePlus;
import mpicbg.stitching.ImageCollectionElement;

/**
 * Queries metadata (image type and dimensions) for each tile using Spark cluster.
 * Saves updated tile configuration on the disk.
 */

public class PipelineMetadataStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -4817219922945295127L;

	public PipelineMetadataStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run()
	{
		final ArrayList< TileInfo > tilesWithoutMetadata = new ArrayList<>();
		for ( final TileInfo tile : job.getTiles() )
			if ( tile.getSize() == null || tile.getType() == null )
				tilesWithoutMetadata.add( tile );

		if ( !tilesWithoutMetadata.isEmpty() )
			queryMetadata( tilesWithoutMetadata );

		job.validateTiles();
		TileOperations.translateTilesToOrigin( job.getTiles() );
	}

	private void queryMetadata( final ArrayList< TileInfo > tilesWithoutMetadata )
	{
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( tilesWithoutMetadata );
		final JavaRDD< TileInfo > task = rdd.map(
				new Function< TileInfo, TileInfo >()
				{
					private static final long serialVersionUID = -4991255417353136684L;

					@Override
					public TileInfo call( final TileInfo tile ) throws Exception
					{
						final ImageCollectionElement el = Utils.createElement( job, tile );
						final ImagePlus imp = el.open( true );

						// FIXME: workaround for misinterpreting slices as timepoints when no metadata is present
						long[] size = Conversions.toLongArray( el.getDimensions() );
						if ( size.length == 2 && imp.getNFrames() > 1 )
							size = new long[] { size[ 0 ], size[ 1 ], imp.getNFrames() };

						tile.setType( ImageType.valueOf( imp.getType() ) );
						tile.setSize( size );

						el.close();
						return tile;
					}
				});

		final List< TileInfo > tilesMetadata = task.collect();

		System.out.println( "Obtained metadata for all tiles" );

		final Map< Integer, TileInfo > tilesMap = job.getTilesMap();
		for ( final TileInfo tileMetadata : tilesMetadata ) {
			final TileInfo tile = tilesMap.get( tileMetadata.getIndex() );
			tile.setType( tileMetadata.getType() );
			tile.setSize( tileMetadata.getSize() );
		}

		try {
			TileInfoJSONProvider.saveTilesConfiguration( job.getTiles(), job.getBaseFolder() + "/" + Utils.addFilenameSuffix( job.getDatasetName() + ".json", "_full" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
}
