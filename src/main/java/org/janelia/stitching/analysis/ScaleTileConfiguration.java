package org.janelia.stitching.analysis;

import java.net.URI;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

/**
 * @author Igor Pisarev
 */

public class ScaleTileConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final int dimToScale = Integer.parseInt( args[ 1 ] );
		final double scaleFactor = Double.parseDouble( args[ 2 ] );

		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ].setStagePosition( dimToScale, Math.floor( tiles[ i ].getStagePosition( dimToScale ) * scaleFactor ) );

		TileInfoJSONProvider.saveTilesConfiguration( tiles, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 0 ], "_scaled_" + (dimToScale==0?"x":(dimToScale==1?"y":"z")) ) ) ) );
	}
}
