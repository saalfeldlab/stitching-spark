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

	public static void process( final List< TileInfo[] > tileChannels ) throws Exception
	{
		System.out.println( "Searching for missing tiles..." );
		final int[] missingTilesAdded = addMissingTiles( tileChannels );
		{
			final StringBuilder sb = new StringBuilder( "  tiles added:" );
			for ( int channel = 0; channel < tileChannels.size(); ++channel )
				sb.append( channel > 0 ? ", " : " " ).append( "ch" + channel ).append( "=" ).append( missingTilesAdded[ channel ] );
			System.out.println( sb.toString() );
		}

		System.out.println( "Searching for duplicate tiles to remove..." );
		final int[] duplicateTilesRemoved = removeDuplicateTiles( tileChannels );
		{
			final StringBuilder sb = new StringBuilder( "  tiles removed:" );
			for ( int channel = 0; channel < tileChannels.size(); ++channel )
				sb.append( channel > 0 ? ", " : " " ).append( "ch" + channel ).append( "=" ).append( duplicateTilesRemoved[ channel ] );
			System.out.println( sb.toString() );
		}

		System.out.println( "Searching for lost tiles to remove (that don't exist on the hard drive)..." );
		final int[] nonExistentTilesRemoved = removeNonExistentTiles( tileChannels );
		{
			final StringBuilder sb = new StringBuilder( "  tiles removed:" );
			for ( int channel = 0; channel < tileChannels.size(); ++channel )
				sb.append( channel > 0 ? ", " : " " ).append( "ch" + channel ).append( "=" ).append( nonExistentTilesRemoved[ channel ] );
			System.out.println( sb.toString() );
		}

		System.out.println( "Filling metadata..." );
		final int[] noMetadataTiles = fillSizeAndImageType( tileChannels );

		boolean somethingChanged = false;
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
			if ( duplicateTilesRemoved[ channel ] + nonExistentTilesRemoved[ channel ] + missingTilesAdded[ channel ] + noMetadataTiles[ channel ] > 0 )
				somethingChanged = true;
		final int[] nonIntersectingTilesRemoved;
		if ( somethingChanged )
		{
			System.out.println( "Tile configuration has changed, intersecting tile sets across channels..." );
			nonIntersectingTilesRemoved = makeIndexesConsistentAcrossChannels( tileChannels );
			{
				final StringBuilder sb = new StringBuilder( "  tiles removed:" );
				for ( int channel = 0; channel < tileChannels.size(); ++channel )
					sb.append( channel > 0 ? ", " : " " ).append( "ch" + channel ).append( "=" ).append( nonIntersectingTilesRemoved[ channel ] );
				System.out.println( sb.toString() );
			}
		}
		else
		{
			nonIntersectingTilesRemoved = new int[ tileChannels.size() ];
		}

		if ( !checkSortedTimestampOrder( tileChannels ) )
			throw new PipelineExecutionException( "Some tiles are not sorted by their timestamp" );

		if ( !checkIndexesConsistency( tileChannels ) )
			throw new PipelineExecutionException( "Some tiles have different indexes in the same iteration order, cannot do index-based matching" );

		if ( !checkCoordinatesConsistency( tileChannels ) )
			throw new PipelineExecutionException( "Some tiles with the same index have different stage coordinates, cannot do index-based matching" );

		// test that all tiles have the same size
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
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

	private static int[] removeDuplicateTiles( final List< TileInfo[] > tileChannels ) throws Exception
	{
		final int[] duplicates = new int[ tileChannels.size() ];
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			final Map< String, TileInfo > coordinatesToTiles = new LinkedHashMap<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
			{
				final String coordinates = Utils.getTileCoordinatesString( tile );
				if ( !coordinatesToTiles.containsKey( coordinates ) || Utils.getTileTimestamp( tile ) > Utils.getTileTimestamp( coordinatesToTiles.get( coordinates ) ) )
					coordinatesToTiles.put( coordinates, tile );
			}
			duplicates[ channel ] = tileChannels.get( channel ).length - coordinatesToTiles.size();
			tileChannels.set( channel, coordinatesToTiles.values().toArray( new TileInfo[ 0 ] ) );
		}
		return duplicates;
	}

	private static int[] removeNonExistentTiles( final List< TileInfo[] > tileChannels ) throws Exception
	{
		final int[] nonExistentTiles = new int[ tileChannels.size() ];
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			final List< TileInfo > existentTiles = new ArrayList<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
				if ( Files.exists( Paths.get( tile.getFilePath() ) ) )
					existentTiles.add( tile );
			nonExistentTiles[ channel ] = tileChannels.get( channel ).length - existentTiles.size();
			tileChannels.set( channel, existentTiles.toArray( new TileInfo[ 0 ] ) );
		}
		return nonExistentTiles;
	}

	private static int[] addMissingTiles( final List< TileInfo[] > tileChannels ) throws Exception
	{
		final int[] missingTiles = new int[ tileChannels.size() ];

		final Map< String, TileInfo >[] coordinatesToTiles = new Map[ tileChannels.size() ];
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			coordinatesToTiles[ channel ] = new HashMap<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
				coordinatesToTiles[ channel ].put( Utils.getTileCoordinatesString( tile ), tile );
		}

		final Map< String, double[] > coordinatesToPosition = new HashMap<>();
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
			for ( final Entry< String, TileInfo > entry : coordinatesToTiles[ channel ].entrySet() )
				coordinatesToPosition.put( entry.getKey(), entry.getValue().getPosition() );

		final TreeMap< Long, List< TileInfo > >[] timestampToTiles = new TreeMap[ tileChannels.size() ];
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			timestampToTiles[ channel ] = new TreeMap<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
			{
				final long timestamp = Utils.getTileTimestamp( tile );
				if ( !timestampToTiles[ channel ].containsKey( timestamp ) )
					timestampToTiles[ channel ].put( timestamp, new ArrayList<>() );
				timestampToTiles[ channel ].get( timestamp ).add( tile );
			}
		}

		final Integer[] maxTileIndex = new Integer[ tileChannels.size() ];
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			for ( final TileInfo tile : tileChannels.get( channel ) )
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


		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			final File imagesBaseDir = Paths.get( tileChannels.get( channel )[ 0 ].getFilePath() ).getParent().toFile();

			final String fileNameChannelPattern = String.format( "^.*?_ch%d_.*?\\.tif$", channel );
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
				final String coordinates = Utils.getTileCoordinatesString( fileName );
				if ( !coordinatesToTiles[ channel ].containsKey( coordinates ) && coordinatesToPosition.containsKey( coordinates ) )
				{
					final TileInfo newTile = new TileInfo();
					newTile.setPosition( coordinatesToPosition.get( coordinates ).clone() );
					newTile.setFilePath( imagesBaseDir.getAbsolutePath() + "/" + fileName );
					newTile.setPixelResolution( tileChannels.get( channel )[ 0 ].getPixelResolution().clone() );

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

		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			final List< TileInfo > tiles = new ArrayList<>();
			for ( final List< TileInfo > tilesTimestampGroup : timestampToTiles[ channel ].values() )
				tiles.addAll( tilesTimestampGroup );
			tileChannels.set( channel, tiles.toArray( new TileInfo[ 0 ] ) );
		}

		return missingTiles;
	}

	private static int[] fillSizeAndImageType( final List< TileInfo[] > tileChannels )
	{
		final int[] noMetadataTiles = new int[ tileChannels.size() ];
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			final List< TileInfo > tilesWithoutMetadata = new ArrayList<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
				if ( tile.getSize() == null || tile.getType() == null )
					tilesWithoutMetadata.add( tile );

			noMetadataTiles[ channel ] = tilesWithoutMetadata.size();
			if ( tilesWithoutMetadata.isEmpty() )
				continue;

			// Determine tile dimensions and image type by opening the first tile image
			final ImagePlus impTest = ImageImporter.openImage( tileChannels.get( channel )[ 0 ].getFilePath() );
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

	private static int[] makeIndexesConsistentAcrossChannels( final List< TileInfo[] > tileChannels ) throws Exception
	{
		// Match the smallest channel by removing non-intersecting tiles from the other channel sets.
		// Then index them to ensure that tiles at the same stage position have the same index.
		final Set< String > coordsIntersection = new HashSet<>();
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			final Set< String > channelCoords = new HashSet<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
				channelCoords.add( Utils.getTileCoordinatesString( tile ) );

			if ( channel == 0)
				coordsIntersection.addAll( channelCoords );
			else
				coordsIntersection.retainAll( channelCoords );
		}

		final int[] tilesRemoved = new int[ tileChannels.size() ];
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
		{
			final List< TileInfo > retained = new ArrayList<>();
			for ( final TileInfo tile : tileChannels.get( channel ) )
				if ( coordsIntersection.contains( Utils.getTileCoordinatesString( tile ) ) )
					retained.add( tile );

			for ( int i = 0; i < retained.size(); ++i )
				retained.get( i ).setIndex( i );

			tilesRemoved[ channel ] = tileChannels.get( channel ).length - retained.size();
			tileChannels.set( channel, retained.toArray( new TileInfo[ 0 ] ) );
		}
		return tilesRemoved;
	}

	private static boolean checkIndexesConsistency( final List< TileInfo[] > tileChannels ) throws Exception
	{
		Integer tilesCount = null;
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
			if ( tilesCount == null )
				tilesCount = tileChannels.get( channel ).length;
			else if ( tilesCount != tileChannels.get( channel ).length )
				return false;

		for ( int i = 0; i < tilesCount; ++i )
		{
			Integer index = null;
			for ( int channel = 0; channel < tileChannels.size(); ++channel )
				if ( index == null )
					index = tileChannels.get( channel )[ i ].getIndex();
				else if ( !index.equals( tileChannels.get( channel )[ i ].getIndex() ) )
					return false;
		}

		return true;
	}

	private static boolean checkCoordinatesConsistency( final List< TileInfo[] > tileChannels ) throws Exception
	{
		Integer tilesCount = null;
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
			if ( tilesCount == null )
				tilesCount = tileChannels.get( channel ).length;
			else if ( tilesCount != tileChannels.get( channel ).length )
				return false;

		for ( int i = 0; i < tilesCount; ++i )
		{
			String coordinates = null;
			for ( int channel = 0; channel < tileChannels.size(); ++channel )
				if ( coordinates == null )
					coordinates = Utils.getTileCoordinatesString( tileChannels.get( channel )[ i ] );
				else if ( !coordinates.equals( Utils.getTileCoordinatesString( tileChannels.get( channel )[ i ] ) ) )
					return false;
		}

		return true;
	}

	private static boolean checkSortedTimestampOrder( final List< TileInfo[] > tileChannels ) throws Exception
	{
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
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
