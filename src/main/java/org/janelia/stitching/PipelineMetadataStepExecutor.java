package org.janelia.stitching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ij.ImagePlus;
import mpicbg.stitching.ImageCollectionElement;

/**
 * Queries metadata (image type and dimensions) for each tile using Spark cluster.
 * Saves updated tile configuration on the disk.
 *
 * @author Igor Pisarev
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
		for ( int channel = 0; channel < job.getChannels(); channel++ )
		{
			final List< TileInfo > tilesWithoutMetadata = new ArrayList<>();
			for ( final TileInfo tile : job.getTiles( channel ) )
				if ( tile.getSize() == null || tile.getType() == null )
					tilesWithoutMetadata.add( tile );

			if ( !tilesWithoutMetadata.isEmpty() )
			{
				final List< TileInfo > tilesMetadata = queryMetadata( tilesWithoutMetadata );

				System.out.println( "Obtained metadata for all tiles of channel " + channel );

				final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( job.getTiles( channel ) );
				for ( final TileInfo tileMetadata : tilesMetadata )
				{
					final TileInfo tile = tilesMap.get( tileMetadata.getIndex() );
					tile.setType( tileMetadata.getType() );
					tile.setSize( tileMetadata.getSize() );
				}

				try {
					TileInfoJSONProvider.saveTilesConfiguration( job.getTiles( channel ), Utils.addFilenameSuffix( job.getArgs().inputTileConfigurations().get( channel ), "_full" ) );
				} catch ( final IOException e ) {
					e.printStackTrace();
				}
			}
		}

		job.validateTiles();

		//TileOperations.translateTilesToOriginReal( job.getTiles() );
	}

	private List< TileInfo > queryMetadata( final List< TileInfo > tilesWithoutMetadata )
	{
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( tilesWithoutMetadata );
		final JavaRDD< TileInfo > task = rdd.map( tile ->
			{
				final ImageCollectionElement el = Utils.createElement( job, tile );
				final ImagePlus imp = el.open( true );

				final int[] fullDimensions = imp.getDimensions();
				final long[] size = new long[ imp.getNDimensions() ];
				size[ 0 ] = fullDimensions[ 0 ];
				size[ 1 ] = fullDimensions[ 1 ];
				int actualDimIndex = 2;
				for ( int d = 2; d < fullDimensions.length; d++ )
					if ( fullDimensions[ d ] > 1)
						size[ actualDimIndex++ ] = fullDimensions[ d ];

				tile.setType( ImageType.valueOf( imp.getType() ) );
				tile.setSize( size );

				el.close();
				return tile;
			} );
		return task.collect();
	}
}
