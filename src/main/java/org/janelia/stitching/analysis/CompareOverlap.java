package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.Arrays;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.Boundaries;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.Utils;

/**
 * Compares initial and final overlaps of a specified pair of tiles.
 *
 * @author Igor Pisarev
 */

public class CompareOverlap
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		String filename = args[ 0 ];
		final int i1 = Integer.parseInt( args[ 1 ] ), i2 = Integer.parseInt( args[ 2 ] );
		System.out.println( "Tiles " + i1 + " and " + i2 + ":" );
		TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( filename ) );
		ArrayList< TileInfo > twoTiles = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( tile.getIndex() == i1 || tile.getIndex() == i2 )
				twoTiles.add( tile );

		if ( twoTiles.size() != 2 )
			throw new Exception( "tiles count mismatch" );

		TileInfo[] tilesArr = twoTiles.toArray( new TileInfo[ 2 ] );
		TileOperations.translateTilesToOrigin( tilesArr );
		Boundaries overlap = TileOperations.getOverlappingRegionGlobal( tilesArr[ 0 ], tilesArr[ 1 ] );
		System.out.println( "Initial overlap at " + Arrays.toString( overlap.getMin() ) + " with dimensions " + Arrays.toString( overlap.getDimensions() ) );

		final double resultingOffset[] = new double[ tilesArr[ 0 ].numDimensions() ];
		for ( int d = 0; d < resultingOffset.length; d++ )
			resultingOffset[ d ] = tilesArr[ 1 ].getPosition( d ) - tilesArr[ 0 ].getPosition( d );



		filename = Utils.addFilenameSuffix( Utils.removeFilenameSuffix( filename, "_full" ), "_shifted" );
		tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( filename ) );
		TileOperations.translateTilesToOrigin( tiles );
		twoTiles = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( tile.getIndex() == i1 || tile.getIndex() == i2 )
				twoTiles.add( tile );

		if ( twoTiles.size() != 2 )
			throw new Exception( "tiles count mismatch" );

		tilesArr = twoTiles.toArray( new TileInfo[ 2 ] );
		TileOperations.translateTilesToOrigin( tilesArr );
		overlap = TileOperations.getOverlappingRegionGlobal( tilesArr[ 0 ], tilesArr[ 1 ] );
		System.out.println( "Resulting overlap at " + Arrays.toString( overlap.getMin() ) + " with dimensions " + Arrays.toString( overlap.getDimensions() ) );


		for ( int d = 0; d < resultingOffset.length; d++ )
			resultingOffset[ d ] -= (tilesArr[ 1 ].getPosition( d ) - tilesArr[ 0 ].getPosition( d ));
		System.out.println( "------------" );
		System.out.println( "Resulting offset: " + Arrays.toString( resultingOffset ) );
	}
}
