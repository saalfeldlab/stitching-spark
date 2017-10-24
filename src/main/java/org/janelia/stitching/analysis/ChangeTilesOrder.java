package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.ImageType;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class ChangeTilesOrder
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );

		final List< TileInfo >[] channels = new ArrayList[] { new ArrayList<>(), new ArrayList<>() };
		for ( final TileInfo tile : tiles)
		{
			if ( tile.getFilePath().contains( "_ch0_" ) )
				channels[0].add( tile );
			else if ( tile.getFilePath().contains( "_ch1_" ) )
				channels[1].add( tile );
		}

		if ( channels[0].size() + channels[1].size() != tiles.length || channels[0].size() != tiles.length / 2 )
			throw new Exception("Tiles count doesn't match after filtering");

		final List< TileInfo > allTiles = new ArrayList<>();
		allTiles.addAll( channels[0] );
		allTiles.addAll( channels[1] );

		for ( int i = 0; i < allTiles.size(); i++ )
		{
			allTiles.get( i ).setIndex( i );
			allTiles.get( i ).setType( ImageType.GRAY16 );
		}

		TileInfoJSONProvider.saveTilesConfiguration( allTiles.toArray( new TileInfo[0] ), dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[0], "_out" ) ) ) );
	}
}
