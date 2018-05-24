package org.janelia.stitching.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.util.Conversions;

import net.imglib2.FinalRealInterval;

/**
 * Finds tiles lying at the specified point.
 *
 * @author Igor Pisarev
 */

public class FindTilesAtPoint
{
	public static void main( final String[] args ) throws FileNotFoundException, IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		TileOperations.translateTilesToOriginReal( tiles );

		final double[] point = Conversions.parseDoubleArray( args[ 1 ].split( "," ) );
		final List< TileInfo > tilesAtPoint = TileOperations.findTilesWithinSubregion( tiles, new FinalRealInterval( point, point ) );

		System.out.println( "There are " + tilesAtPoint.size() + " tiles at the point " + Arrays.toString( point ) + ":" );
		for ( final TileInfo tile : tilesAtPoint )
			System.out.println( tile.getIndex() + ": " + tile.getFilePath() );
	}
}
