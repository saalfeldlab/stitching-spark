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
 * In this case you may want to:
 * 1. Increase the number of phase correlation peaks that should be investigated by the pairwise stitching algorithm.
 * 2. Preapply gaussian blur to tile images
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
		System.out.println( "----------------------" );

		final ArrayList< TileInfo > goodTiles = new ArrayList<>();
		final ArrayList< TileInfo > badTiles = new ArrayList<>();

		int identical = 0;
		for ( int i = 0; i < n; i++ )
		{

			if ( false )//!tilesOrig[ i ].getIndex().equals( tilesMod[ i ].getIndex() ) || !tilesOrig[ i ].getFilePath().equals( tilesMod[ i ].getFilePath() ) )
				throw new IllegalArgumentException( "Tiles mismatch: orig=" + tilesOrig[ i ].getIndex() + ", mod=" + tilesMod[ i ].getIndex() + "\n"
						+ "orig=" + tilesOrig[ i ].getFilePath() + ", mod=" + tilesMod[ i ].getFilePath() );

			final int dim = tilesOrig[ i ].numDimensions();
			final double[] dist = new double[ dim ];

			boolean isGoodTile = true;
			int diffSum = 0;
			for ( int d = 0; d < dim; d++ )
			{
				dist[ d ] = tilesOrig[ i ].getPosition( d ) - tilesMod[ i ].getPosition( d );
				diffSum += dist[ d ];
				if ( Math.abs( dist[ d ] ) > tilesOrig[ i ].getSize( d ) / 7 )
					isGoodTile = false;
			}
			if ( diffSum == 0 )
				identical++;
			//else
			//	System.out.println( tilesOrig[ i ].getIndex() + ": " + Arrays.toString( dist ) );

			if ( isGoodTile )
			{
				goodTiles.add( tilesOrig[ i ] );
			}
			else
			{
				System.out.println( Arrays.toString( dist ) );
				badTiles.add( tilesOrig[ i ] );
			}



			diffSum = 0;
			for ( int d = 0; d < dim; d++ )
			{
				dist[ d ] = tilesOrig[ i ].getSize( d ) - tilesMod[ i ].getSize( d );
				diffSum += dist[ d ];
			}
			//if ( diffSum != 0 )
			//	System.out.println( tilesOrig[ i ].getIndex() + ": " + Arrays.toString( dist ) );
		}
		if ( identical == n )
			System.out.println( "The configurations are identical!" );
		else
			System.out.println( "Identical positions: " + identical );

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
