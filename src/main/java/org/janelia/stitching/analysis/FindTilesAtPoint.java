package org.janelia.stitching.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;

public class FindTilesAtPoint
{
	public static void main( final String[] args ) throws FileNotFoundException, IOException
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		TileOperations.translateTilesToOrigin( tiles );

		final double[] point = new double[] { 260, 1280, 330 };

		final TileInfo subregion = new TileInfo( point.length );
		subregion.setPosition( point );
		subregion.setSize( new long[] { 1, 1, 1 } );
		final ArrayList< TileInfo > tilesAtPoint = TileOperations.findTilesWithinSubregion( tiles, subregion );

		System.out.println( "There are " + tilesAtPoint.size() + " tiles at the point" );
		for ( final TileInfo tile : tilesAtPoint )
			System.out.println( tile.getIndex() + ": " + tile.getFile() );
	}
}
