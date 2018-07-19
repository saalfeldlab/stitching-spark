package org.janelia.util;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;

public class TileSymlinkCreator
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final String outputPath = args[ args.length - 1 ];
		for ( int channel = 0; channel < args.length - 1; ++channel )
		{
			final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ channel ] ) ) );
			for ( final TileInfo tile : tiles )
			{
				final String newTileName = String.format( "ch%d_tile%d.tif", channel, tile.getIndex() );
				Files.createSymbolicLink(
						Paths.get( outputPath, newTileName ),
						Paths.get( tile.getFilePath() )
					);
			}
		}
	}
}
