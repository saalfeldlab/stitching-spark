package org.janelia.stitching;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.util.ComparableTuple;
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

	public static void process( final TreeMap< Integer, List< TileInfo > > tileChannels ) throws Exception
	{
		System.out.println( "Searching for missing tiles..." );
		final Map< Integer, Integer > missingTilesAdded = addMissingTiles( tileChannels );
		{
			final StringBuilder sb = new StringBuilder( "  tiles added:" );
			for ( final int channel : tileChannels.keySet() )
				sb.append( channel != tileChannels.firstKey() ? ", " : " " ).append( "ch" + channel ).append( "=" ).append( missingTilesAdded.get( channel ) );
			System.out.println( sb.toString() );
		}

		System.out.println( "Searching for duplicate tiles to remove..." );
		final Map< Integer, Integer > duplicateTilesRemoved = removeDuplicateTiles( tileChannels );
		{
			final StringBuilder sb = new StringBuilder( "  tiles removed:" );
			for ( final int channel : tileChannels.keySet() )
				sb.append( channel != tileChannels.firstKey() ? ", " : " " ).append( "ch" + channel ).append( "=" ).append( duplicateTilesRemoved.get( channel ) );
			System.out.println( sb.toString() );
		}

		System.out.println( "Searching for lost tiles to remove (that don't exist on the hard drive)..." );
		final Map< Integer, Integer > nonExistingTilesRemoved = removeNonExistingTiles( tileChannels );
		{
			final StringBuilder sb = new StringBuilder( "  tiles removed:" );
			for ( final int channel : tileChannels.keySet() )
				sb.append( channel != tileChannels.firstKey() ? ", " : " " ).append( "ch" + channel ).append( "=" ).append( nonExistingTilesRemoved.get( channel ) );
			System.out.println( sb.toString() );
		}

		System.out.println( "Filling metadata..." );
		final Map< Integer, Integer > noMetadataTiles = fillSizeAndImageType( tileChannels );

		boolean somethingChanged = false;
		for ( final int channel : tileChannels.keySet() )
			if ( duplicateTilesRemoved.get( channel ) + nonExistingTilesRemoved.get( channel ) + missingTilesAdded.get( channel ) + noMetadataTiles.get( channel ) > 0 )
				somethingChanged = true;
		final Map< Integer, Integer > nonIntersectingTilesRemoved;
		if ( somethingChanged )
		{
			System.out.println( "Tile configuration has changed, intersecting tile sets across channels..." );
			nonIntersectingTilesRemoved = makeIndexesConsistentAcrossChannels( tileChannels );
			{
				final StringBuilder sb = new StringBuilder( "  tiles removed:" );
				for ( final int channel : tileChannels.keySet() )
					sb.append( channel != tileChannels.firstKey() ? ", " : " " ).append( "ch" + channel ).append( "=" ).append( nonIntersectingTilesRemoved.get( channel ) );
				System.out.println( sb.toString() );
			}
		}
		else
		{
			nonIntersectingTilesRemoved = new TreeMap<>();
		}

		if ( !checkSortedTimestampOrder( tileChannels ) )
			throw new PipelineExecutionException( "Some tiles are not sorted by their timestamp" );

		if ( !checkIndexesConsistency( tileChannels ) )
			throw new PipelineExecutionException( "Some tiles have different indexes in the same iteration order, cannot do index-based matching" );

		if ( !checkCoordinatesConsistency( tileChannels ) )
			throw new PipelineExecutionException( "Some tiles with the same index have different stage coordinates, cannot do index-based matching" );

		// test that all tiles have the same size
		for ( final int channel : tileChannels.keySet() )
		{
			ComparableTuple< Long > tileSize = null;
			for ( final TileInfo tile : tileChannels.get( channel ) )
				if ( tileSize == null )
					tileSize = new ComparableTuple<>( Conversions.toBoxedArray( tile.getSize() ) );
				else if ( tileSize.compareTo( new ComparableTuple<>( Conversions.toBoxedArray( tile.getSize() ) ) ) != 0 )
					System.out.println( "----- Different tile size:  channel=" + channel + ", tile=" + tile.getIndex() + ", size=" + Arrays.toString( tile.getSize() ) + ",  should be " + tileSize + " -----" );
		}
	}

	@Override
	public void run() throws PipelineExecutionException
	{
		// don't execute smart logic since this step has already been executed at the ImageList.csv -> JSON parsing step
		job.validateTiles();
	}

	private static Map< Integer, Integer > removeDuplicateTiles( final TreeMap< Integer, List< TileInfo > > tileChannels ) throws Exception
	{
		final Map< Integer, Integer > duplicates = new TreeMap<>();
		for ( final int channel : tileChannels.keySet() )
		{
			final Map< String, TileInfo > coordinatesToTiles = new LinkedHashMap<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
			{
				final String coordinates = Utils.getTileCoordinatesString( tile );
				if ( !coordinatesToTiles.containsKey( coordinates ) || Utils.getTileTimestamp( tile ) > Utils.getTileTimestamp( coordinatesToTiles.get( coordinates ) ) )
					coordinatesToTiles.put( coordinates, tile );
			}
			duplicates.put( channel, tileChannels.get( channel ).size() - coordinatesToTiles.size() );
			tileChannels.put( channel, new ArrayList<>( coordinatesToTiles.values() ) );
		}
		return duplicates;
	}

	private static Map< Integer, Integer > removeNonExistingTiles( final TreeMap< Integer, List< TileInfo > > tileChannels ) throws Exception
	{
		final Map< Integer, Integer > nonExistingTiles = new TreeMap<>();
		for ( final int channel : tileChannels.keySet() )
		{
			final List< TileInfo > existingTiles = new ArrayList<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
				if ( Files.exists( Paths.get( tile.getFilePath() ) ) )
					existingTiles.add( tile );
			nonExistingTiles.put( channel, tileChannels.get( channel ).size() - existingTiles.size() );
			tileChannels.put( channel, existingTiles );
		}
		return nonExistingTiles;
	}

	private static Map< Integer, Integer > addMissingTiles( final TreeMap< Integer, List< TileInfo > > tileChannels ) throws Exception
	{
		final Map< Integer, Integer > missingTiles = new TreeMap<>();

		final Map< Integer, Map< String, TileInfo > > channelCoordinatesToTiles = new TreeMap<>();
		for ( final int channel : tileChannels.keySet() )
		{
			channelCoordinatesToTiles.put( channel, new HashMap<>() );
			for ( final TileInfo tile : tileChannels.get( channel ) )
				channelCoordinatesToTiles.get( channel ).put( Utils.getTileCoordinatesString( tile ), tile );
		}

		final Map< String, double[] > coordinatesToPosition = new HashMap<>();
		for ( final int channel : tileChannels.keySet() )
			for ( final Entry< String, TileInfo > entry : channelCoordinatesToTiles.get( channel ).entrySet() )
				coordinatesToPosition.put( entry.getKey(), entry.getValue().getPosition() );

		final Map< Integer, TreeMap< Long, List< TileInfo > > > channelTimestampToTiles = new TreeMap<>();
		for ( final int channel : tileChannels.keySet() )
		{
			channelTimestampToTiles.put( channel, new TreeMap<>() );
			for ( final TileInfo tile : tileChannels.get( channel ) )
			{
				final long timestamp = Utils.getTileTimestamp( tile );
				if ( !channelTimestampToTiles.get( channel ).containsKey( timestamp ) )
					channelTimestampToTiles.get( channel ).put( timestamp, new ArrayList<>() );
				channelTimestampToTiles.get( channel ).get( timestamp ).add( tile );
			}
		}

		final Map< Integer, Integer > channelMaxTileIndex = new TreeMap<>();
		for ( final int channel : tileChannels.keySet() )
		{
			channelMaxTileIndex.put( channel, null );
			for ( final TileInfo tile : tileChannels.get( channel ) )
			{
				if ( channelMaxTileIndex.get( channel ) != null )
				{
					if ( tile.getIndex() != null )
						channelMaxTileIndex.put( channel, Math.max( tile.getIndex(), channelMaxTileIndex.get( channel ) ) );
				}
				else
				{
					channelMaxTileIndex.put( channel, tile.getIndex() );
				}
			}
		}

		for ( final int channel : tileChannels.keySet() )
		{
			final File imagesBaseDir = Paths.get( tileChannels.get( channel ).get( 0 ).getFilePath() ).getParent().toFile();

			final String fileNameChannelPattern = String.format( "^.*?_%dnm_.*?\\.tif$", channel );
			final FilenameFilter fileNameChannelFilter = new FilenameFilter()
			{
				@Override
				public boolean accept( final File dir, final String name )
				{
					return name.matches( fileNameChannelPattern );
				}
			};

			final String[] fileList = imagesBaseDir.list( fileNameChannelFilter );
			for ( final String fileName : fileList )
			{
				final String coordinates;
				try
				{
					coordinates = Utils.getTileCoordinatesString( fileName );
				}
				catch ( final Exception e )
				{
					continue;
				}

				if ( !channelCoordinatesToTiles.get( channel ).containsKey( coordinates ) && coordinatesToPosition.containsKey( coordinates ) )
				{
					final TileInfo newTile = new TileInfo();
					newTile.setPosition( coordinatesToPosition.get( coordinates ).clone() );
					newTile.setFilePath( imagesBaseDir.getAbsolutePath() + "/" + fileName );
					newTile.setPixelResolution( tileChannels.get( channel ).get( 0 ).getPixelResolution().clone() );

					channelMaxTileIndex.put( channel, channelMaxTileIndex.getOrDefault( channel, -1 ) + 1 );
					newTile.setIndex( channelMaxTileIndex.get( channel ).intValue() );

					final long timestamp = Utils.getTileTimestamp( fileName );
					if ( !channelTimestampToTiles.get( channel ).containsKey( timestamp ) )
						channelTimestampToTiles.get( channel ).put( timestamp, new ArrayList<>() );
					channelTimestampToTiles.get( channel ).get( timestamp ).add( newTile );

					missingTiles.put( channel, missingTiles.getOrDefault( channel, 0 ) + 1 );
				}
			}
		}

		for ( final int channel : tileChannels.keySet() )
		{
			final List< TileInfo > tiles = new ArrayList<>();
			for ( final List< TileInfo > tilesTimestampGroup : channelTimestampToTiles.get( channel ).values() )
				tiles.addAll( tilesTimestampGroup );
			tileChannels.put( channel, tiles );

			if ( !missingTiles.containsKey( channel ) )
				missingTiles.put( channel, 0 );
		}

		return missingTiles;
	}

	private static Map< Integer, Integer > fillSizeAndImageType( final TreeMap< Integer, List< TileInfo > > tileChannels )
	{
		final Map< Integer, Integer > noMetadataTiles = new TreeMap<>();
		for ( final int channel : tileChannels.keySet() )
		{
			final List< TileInfo > tilesWithoutMetadata = new ArrayList<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
				if ( tile.getSize() == null || tile.getType() == null )
					tilesWithoutMetadata.add( tile );

			noMetadataTiles.put( channel, tilesWithoutMetadata.size() );
			if ( tilesWithoutMetadata.isEmpty() )
				continue;

			// Determine tile dimensions and image type by opening the first tile image
			final ImagePlus impTest = ImageImporter.openImage( tileChannels.get( channel ).get( 0 ).getFilePath() );
			final long[] size = Conversions.toLongArray( Utils.getImagePlusDimensions( impTest ) );
			final ImageType imageType = ImageType.valueOf( impTest.getType() );
			impTest.close();

			for ( final TileInfo tile : tilesWithoutMetadata )
			{
				tile.setSize( size );
				tile.setType( imageType );
			}
		}
		return noMetadataTiles;
	}

	private static Map< Integer, Integer > makeIndexesConsistentAcrossChannels( final TreeMap< Integer, List< TileInfo > > tileChannels ) throws Exception
	{
		// Match the smallest channel by removing non-intersecting tiles from the other channel sets.
		// Then index them to ensure that tiles at the same stage position have the same index.
		final Set< String > coordsIntersection = new HashSet<>();
		for ( final int channel : tileChannels.keySet() )
		{
			final Set< String > channelCoords = new HashSet<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
				channelCoords.add( Utils.getTileCoordinatesString( tile ) );

			if ( channel == tileChannels.firstKey() )
				coordsIntersection.addAll( channelCoords );
			else
				coordsIntersection.retainAll( channelCoords );
		}

		final Map< Integer, Integer > tilesRemoved = new TreeMap<>();
		for ( final int channel : tileChannels.keySet() )
		{
			final List< TileInfo > retained = new ArrayList<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
				if ( coordsIntersection.contains( Utils.getTileCoordinatesString( tile ) ) )
					retained.add( tile );

			for ( int i = 0; i < retained.size(); ++i )
				retained.get( i ).setIndex( i );

			tilesRemoved.put( channel, tileChannels.get( channel ).size() - retained.size() );
			tileChannels.put( channel, retained );
		}
		return tilesRemoved;
	}

	private static boolean checkIndexesConsistency( final TreeMap< Integer, List< TileInfo > > tileChannels ) throws Exception
	{
		Integer tilesCount = null;
		for ( final int channel : tileChannels.keySet() )
			if ( tilesCount == null )
				tilesCount = tileChannels.get( channel ).size();
			else if ( tilesCount != tileChannels.get( channel ).size() )
				return false;

		for ( int i = 0; i < tilesCount; ++i )
		{
			Integer index = null;
			for ( final int channel : tileChannels.keySet() )
				if ( index == null )
					index = tileChannels.get( channel ).get( i ).getIndex();
				else if ( !index.equals( tileChannels.get( channel ).get( i ).getIndex() ) )
					return false;
		}

		return true;
	}

	private static boolean checkCoordinatesConsistency( final TreeMap< Integer, List< TileInfo > > tileChannels ) throws Exception
	{
		Integer tilesCount = null;
		for ( final int channel : tileChannels.keySet() )
			if ( tilesCount == null )
				tilesCount = tileChannels.get( channel ).size();
			else if ( tilesCount != tileChannels.get( channel ).size() )
				return false;

		for ( int i = 0; i < tilesCount; ++i )
		{
			String coordinates = null;
			for ( final int channel : tileChannels.keySet() )
				if ( coordinates == null )
					coordinates = Utils.getTileCoordinatesString( tileChannels.get( channel ).get( i ) );
				else if ( !coordinates.equals( Utils.getTileCoordinatesString( tileChannels.get( channel ).get( i ) ) ) )
					return false;
		}

		return true;
	}

	private static boolean checkSortedTimestampOrder( final TreeMap< Integer, List< TileInfo > > tileChannels ) throws Exception
	{
		for ( final int channel : tileChannels.keySet() )
		{
			long lastTimestamp = Long.MIN_VALUE;
			for ( final TileInfo tile : tileChannels.get( channel ) )
			{
				final long timestamp = Utils.getTileTimestamp( tile );
				if ( timestamp < lastTimestamp )
					return false;
				lastTimestamp = timestamp;
			}
		}
		return true;
	}
}
