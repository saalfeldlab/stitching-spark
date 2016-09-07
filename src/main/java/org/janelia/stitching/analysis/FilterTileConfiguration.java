package org.janelia.stitching.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.janelia.util.Conversions;

/**
 * Chooses a subset of tiles from initial tile configuration based on specified criteria
 * and stores it to a separate tile configuration file.
 *
 * @author Igor Pisarev
 */

public class FilterTileConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final String filename = args[ 0 ];
		final String filterType = args[ 1 ];
		final String filter = args[ 2 ];

		switch ( filterType )
		{
		case "str":
			filterByFilename( filename, filter );
			break;
		case "in":
			filterByTileIndexes( filename, new TreeSet< >( Conversions.arrToList( Conversions.parseIntArray( filter.split( "," ) ) ) ) );
			break;
		case "out":
			filterByTileIndexesExcluded( filename, new TreeSet< >( Conversions.arrToList( Conversions.parseIntArray( filter.split( "," ) ) ) ) );
			break;

		default:
			throw new Exception( "Unknown filter type provided. Accepted values are: str,in,out" );
		}

	}

	public static void filterByFilename( final String inputFilename, final String filenameFilter ) throws IOException
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilename );
		final ArrayList< TileInfo > filteredTiles = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( tile.getFilePath().contains( filenameFilter ) )
				filteredTiles.add( tile );
		TileInfoJSONProvider.saveTilesConfiguration( filteredTiles.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix( inputFilename, "_filtered_" + filenameFilter ) );
	}

	public static void filterByTileIndexes( final String inputFilename, final Set< Integer > tileIndexes ) throws IOException
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilename );
		final ArrayList< TileInfo > filteredTiles = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( tileIndexes.contains( tile.getIndex() ) )
				filteredTiles.add( tile );
		TileInfoJSONProvider.saveTilesConfiguration( filteredTiles.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix( inputFilename, "_filtered_" + tileIndexes.toString().replaceAll( " ", "" ) ) );
	}

	public static void filterByTileIndexesExcluded( final String inputFilename, final Set< Integer > tileIndexesExcluded ) throws IOException
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilename );
		final ArrayList< TileInfo > filteredTiles = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( !tileIndexesExcluded.contains( tile.getIndex() ) )
				filteredTiles.add( tile );
		TileInfoJSONProvider.saveTilesConfiguration( filteredTiles.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix( inputFilename, "_excluded_" + tileIndexesExcluded.toString().replaceAll( " ", "" ) ) );
	}
}
