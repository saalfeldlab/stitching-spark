package org.janelia.stitching.analysis;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.Utils;

public class ExtractTilePosition
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		TileOperations.translateTilesToOriginReal( tiles );
		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( tiles );
		for ( final int i : new int[] { 18299, 18300 } )
			System.out.println( "Tile " + tilesMap.get(i).getIndex() + ":  position = " + Arrays.toString( tilesMap.get(i).getPosition() ) + ",  filename="+Paths.get(tilesMap.get(i).getFilePath()).getFileName());
	}
}
