package org.janelia.stitching.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;

/**
 * Evaluates the quality of a final solution by estimating the distances of every tile
 * from its initial position to a final one.
 *
 * Large offsets indicate that the phase correlation has failed to identify good shifts for some of the tiles,
 * or that the optimization procedure has failed to find a good solution which fits all of the tiles.
 * In this case you may want to increase the number of phase correlation peaks that should be investigated by the pairwise stitching algorithm.
 *
 * @author Igor Pisarev
 */

public class DistanceFromOriginal
{
	public static void main( final String[] args ) throws FileNotFoundException, IOException
	{
		final TileInfo[] tilesOrig = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final TileInfo[] tilesMod  = TileInfoJSONProvider.loadTilesConfiguration( args[ 1 ] );

		TileOperations.translateTilesToOrigin( tilesOrig );
		TileOperations.translateTilesToOrigin( tilesMod  );

		if ( tilesOrig.length != tilesMod.length )
			throw new IllegalArgumentException( "Some tiles are missing" );

		final int n = tilesOrig.length;
		System.out.println( "There are " + n + " tiles" );

		System.out.println( "dimensions : " + Arrays.toString( tilesOrig[ 0 ].getSize() ) );
		System.out.println( "----------------------" );

		final ArrayList< TileInfo > goodTiles = new ArrayList<>();
		final ArrayList< TileInfo > badTiles = new ArrayList<>();
		for ( int i = 0; i < n; i++ )
		{
			if ( !tilesOrig[ i ].getIndex().equals( tilesMod[ i ].getIndex() ) )// || !tilesOrig[ i ].getFile().equals( tilesMod[ i ].getFile() ) )
				throw new IllegalArgumentException( "Tiles mismatch: orig=" + tilesOrig[ i ].getIndex() + ", mod=" + tilesMod[ i ].getIndex() + "\n"
						+ "orig=" + tilesOrig[ i ].getFile() + ", mod=" + tilesMod[ i ].getFile() );

			final int dim = tilesOrig[ i ].numDimensions();
			final double[] dist = new double[ dim ];

			boolean isGoodTile = true;
			for ( int d = 0; d < dim; d++ )
			{
				dist[ d ] = tilesOrig[ i ].getPosition( d ) - tilesMod[ i ].getPosition( d );
				if ( Math.abs( dist[ d ] ) > tilesOrig[ i ].getSize( d ) / 7 )
					isGoodTile = false;
			}

			System.out.println( tilesOrig[ i ].getIndex() + ": " + Arrays.toString( dist ) );

			if ( isGoodTile )
				goodTiles.add( tilesOrig[ i ] );
			else
			{
				badTiles.add( tilesOrig[ i ] );
			}
		}

		System.out.println( "----------------------" );
		System.out.println( "Found " + goodTiles.size() + " good tiles out of " + n );


		if ( !badTiles.isEmpty() )
		{
			System.out.println( "Bad tiles are: " );
			for ( final TileInfo tile : badTiles )
				System.out.println( tile.getIndex() + ", position="+Arrays.toString( tile.getPosition() ));

			//System.out.println( "Removing " + badTilesIndices.size() + " bad ones: " + Arrays.toString( badTilesIndices.toArray() ) );
			//TileInfoJSONProvider.saveTilesConfiguration( goodTiles.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args[ 0 ], "_full" ), "_good" ) );*/
		}
	}
}
