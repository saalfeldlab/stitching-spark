package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.Arrays;

import org.janelia.stitching.Boundaries;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.Utils;

public class CompareOverlap
{
	public static void main( final String[] args ) throws Exception
	{
		final int i1 = 252, i2 = 263;

		System.out.println( "Tiles " + i1 + " and " + i2 + ":" );
		String filename = args[ 0 ];
		TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( filename );
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
		//TileInfoJSONProvider.saveTilesConfiguration( tilesArr, Utils.addFilenameSuffix( filename, "_" + tilesArr[ 0 ].getIndex() + "_" + tilesArr[ 1 ].getIndex() + "_OVERLAP" ) );

		final double resultingOffset[] = new double[ tilesArr[ 0 ].numDimensions() ];
		for ( int d = 0; d < resultingOffset.length; d++ )
			resultingOffset[ d ] = tilesArr[ 1 ].getPosition( d ) - tilesArr[ 0 ].getPosition( d );



		filename = Utils.addFilenameSuffix( Utils.removeFilenameSuffix( filename, "_full" ), "_shifted" );
		tiles = TileInfoJSONProvider.loadTilesConfiguration( filename );
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
		//TileInfoJSONProvider.saveTilesConfiguration( tilesArr, Utils.addFilenameSuffix( filename, "_" + tilesArr[ 0 ].getIndex() + "_" + tilesArr[ 1 ].getIndex() + "_OVERLAP" ) );


		for ( int d = 0; d < resultingOffset.length; d++ )
			resultingOffset[ d ] -= (tilesArr[ 1 ].getPosition( d ) - tilesArr[ 0 ].getPosition( d ));
		System.out.println( "------------" );
		System.out.println( "Resulting offset: " + Arrays.toString( resultingOffset ) );
	}
}
