package org.janelia.stitching.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class FilterTileConfiguration
{
	public static void main( final String[] args ) throws IOException
	{
		filterByTileIndexes( args[ 0 ], new TreeSet< >( Arrays.asList( 145, 154 ) ) );
	}

	public static void filterByFilename( final String inputFilename, final String filenameFilter ) throws IOException
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilename );
		final ArrayList< TileInfo > filteredTiles = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( tile.getFile().contains( filenameFilter ) )
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
