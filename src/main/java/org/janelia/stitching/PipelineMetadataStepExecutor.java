package org.janelia.stitching;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.util.Conversions;
import org.janelia.util.ImageImporter;

import ij.ImagePlus;

/**
 * Modifies tile configurations in the following ways:
 * 1) Removes duplicated tiles (picks the one with later timestamp)
 * 2) Removes tiles that are not present on disk
 * 3) Adds missing tiles back to the configuration (tiles that are present on disk but missing from the initial configuration)
 * 4) Queries dimensions and image type of a tile and sets them for all tiles.
 * Saves updated tile configurations on the disk.
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
	public void run() throws PipelineExecutionException
	{
		try
		{
			final int[] duplicateTilesRemoved = removeDuplicateTiles();
			final int[] nonExistentTilesRemoved = removeNonExistentTiles();
			final int[] missingTilesAdded = addMissingTiles();
			fillSizeAndImageType();

			//TileOperations.translateTilesToOriginReal( job.getTiles() );

			for ( int channel = 0; channel < job.getChannels(); ++channel )
			{
				System.out.println( "Channel " + channel + ":" );
				System.out.println( "  duplicated tiles removed = " + duplicateTilesRemoved[ channel ] );
				System.out.println( "  non-existent tiles removed = " + nonExistentTilesRemoved[ channel ] );
				System.out.println( "  missing tiles added = " + missingTilesAdded[ channel ] );

				TileInfoJSONProvider.saveTilesConfiguration( job.getTiles( channel ), Utils.addFilenameSuffix( job.getArgs().inputTileConfigurations().get( channel ), "_full" ) );
			}
		}
		catch ( final Exception e )
		{
			throw new PipelineExecutionException( e.getMessage(), e.getCause() );
		}

		job.validateTiles();
	}

	private int[] removeDuplicateTiles() throws Exception
	{
		final int[] duplicates = new int[ job.getChannels() ];
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final Map< String, TileInfo > coordinatesToTiles = new LinkedHashMap<>();
			for ( final TileInfo tile : job.getTiles( channel ) )
			{
				final String coordinates = Utils.getTileCoordinatesString( tile );
				if ( !coordinatesToTiles.containsKey( coordinates ) || Utils.getTileTimestamp( tile ) > Utils.getTileTimestamp( coordinatesToTiles.get( coordinates ) ) )
					coordinatesToTiles.put( coordinates, tile );
			}
			duplicates[ channel ] = job.getTiles( channel ).length - coordinatesToTiles.size();
			job.setTiles( coordinatesToTiles.values().toArray( new TileInfo[ 0 ] ), channel );
		}
		return duplicates;
	}

	private int[] removeNonExistentTiles() throws Exception
	{
		final int[] nonExistentTiles = new int[ job.getChannels() ];
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final List< TileInfo > existentTiles = new ArrayList<>();
			for ( final TileInfo tile : job.getTiles( channel ) )
				if ( Files.exists( Paths.get( tile.getFilePath() ) ) )
					existentTiles.add( tile );
			nonExistentTiles[ channel ] = job.getTiles( channel ).length - existentTiles.size();
			job.setTiles( existentTiles.toArray( new TileInfo[ 0 ] ), channel );
		}
		return nonExistentTiles;
	}

	private int[] addMissingTiles() throws Exception
	{
		final int[] missingTiles = new int[ job.getChannels() ];

		final Map< String, TileInfo >[] coordinatesToTiles = new Map[ job.getChannels() ];
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			coordinatesToTiles[ channel ] = new HashMap<>();
			for ( final TileInfo tile : job.getTiles( channel ) )
				coordinatesToTiles[ channel ].put( Utils.getTileCoordinatesString( tile ), tile );
		}

		final Map< String, double[] > coordinatesToPosition = new HashMap<>();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
			for ( final Entry< String, TileInfo > entry : coordinatesToTiles[ channel ].entrySet() )
				coordinatesToPosition.put( entry.getKey(), entry.getValue().getPosition() );

		final TreeMap< Long, List< TileInfo > >[] timestampToTiles = new TreeMap[ job.getChannels() ];
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			timestampToTiles[ channel ] = new TreeMap<>();
			for ( final TileInfo tile : job.getTiles( channel ) )
			{
				final long timestamp = Utils.getTileTimestamp( tile );
				if ( !timestampToTiles[ channel ].containsKey( timestamp ) )
					timestampToTiles[ channel ].put( timestamp, new ArrayList<>() );
				timestampToTiles[ channel ].get( timestamp ).add( tile );
			}
		}

		final Integer[] maxTileIndex = new Integer[ job.getChannels() ];
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			for ( final TileInfo tile : job.getTiles( channel ) )
			{
				if ( maxTileIndex[ channel ] != null )
				{
					if ( tile.getIndex() != null )
						maxTileIndex[ channel ] = Math.max( tile.getIndex(), maxTileIndex[ channel ] );
				}
				else
				{
					maxTileIndex[ channel ] = tile.getIndex();
				}
			}
		}

		final String fileNameChannelPattern = "^.*?_(ch\\d)_.*?\\.tif$";
		final FilenameFilter fileNameChannelFilter = new FilenameFilter()
		{
			@Override
			public boolean accept( final File dir, final String name )
			{
				return name.matches( fileNameChannelPattern );
			}
		};

		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final File imagesBaseDir = Paths.get( job.getTiles( channel )[ 0 ].getFilePath() ).getParent().toFile();
			final String[] fileList = imagesBaseDir.list( fileNameChannelFilter );
			for ( final String fileName : fileList )
			{
				final String coordinates = Utils.getTileCoordinatesString( fileName );
				if ( !coordinatesToTiles[ channel ].containsKey( coordinates ) )
				{
					final TileInfo newTile = new TileInfo();
					newTile.setPosition( coordinatesToPosition.get( coordinates ).clone() );
					newTile.setFilePath( imagesBaseDir.getAbsolutePath() + "/" + fileName );

					if ( maxTileIndex[ channel ] != null )
						newTile.setIndex( ++maxTileIndex[ channel ] );

					final long timestamp = Utils.getTileTimestamp( fileName );
					if ( !timestampToTiles[ channel ].containsKey( timestamp ) )
						timestampToTiles[ channel ].put( timestamp, new ArrayList<>() );
					timestampToTiles[ channel ].get( timestamp ).add( newTile );

					++missingTiles[ channel ];
				}
			}
		}

		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final List< TileInfo > tiles = new ArrayList<>();
			for ( final List< TileInfo > tilesTimestampGroup : timestampToTiles[ channel ].values() )
				tiles.addAll( tilesTimestampGroup );
			job.setTiles( tiles.toArray( new TileInfo[ 0 ] ), channel );
		}

		return missingTiles;
	}

	private void fillSizeAndImageType()
	{
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final List< TileInfo > tilesWithoutMetadata = new ArrayList<>();
			for ( final TileInfo tile : job.getTiles( channel ) )
				if ( tile.getSize() == null || tile.getType() == null )
					tilesWithoutMetadata.add( tile );

			if ( tilesWithoutMetadata.isEmpty() )
				continue;

			// Determine tile dimensions and image type by opening the first tile image
			final ImagePlus impTest = ImageImporter.openImage( job.getTiles( channel )[ 0 ].getFilePath() );
			final long[] size = Conversions.toLongArray( Utils.getImagePlusDimensions( impTest ) );
			final ImageType imageType = ImageType.valueOf( impTest.getType() );
			impTest.close();

			for ( final TileInfo tile : tilesWithoutMetadata )
			{
				tile.setSize( size );
				tile.setType( imageType );
			}
		}
	}
}
