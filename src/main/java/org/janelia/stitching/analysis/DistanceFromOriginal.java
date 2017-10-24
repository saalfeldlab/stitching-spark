package org.janelia.stitching.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
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
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tilesOrig = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final TileInfo[] tilesMod  = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) );

		TileOperations.translateTilesToOrigin( tilesOrig );
		TileOperations.translateTilesToOrigin( tilesMod  );

		if ( tilesOrig.length != tilesMod.length )
			throw new IllegalArgumentException( "Some tiles are missing" );

		final int n = tilesOrig.length;
		System.out.println( "There are " + n + " tiles" );
		System.out.println( "----------------------" );

		final ArrayList< TileInfo > goodTiles = new ArrayList<>();
		final ArrayList< TileInfo > badTiles = new ArrayList<>();

		int haveSamePosition = 0, haveSameSize = 0;
		final int[][] range = new int[ tilesOrig[ 0 ].numDimensions() ][ 2 ];

		long[] tileSize = null;
		boolean allTilesHaveSameSize = true;

		for ( int i = 0; i < n; i++ )
		{

			if ( false )//!tilesOrig[ i ].getIndex().equals( tilesMod[ i ].getIndex() ) || !tilesOrig[ i ].getFilePath().equals( tilesMod[ i ].getFilePath() ) )
				throw new IllegalArgumentException( "Tiles mismatch: orig=" + tilesOrig[ i ].getIndex() + ", mod=" + tilesMod[ i ].getIndex() + "\n"
						+ "orig=" + tilesOrig[ i ].getFilePath() + ", mod=" + tilesMod[ i ].getFilePath() );

			final int dim = tilesOrig[ i ].numDimensions();
			final double[] dist = new double[ dim ];
			final long[] sizeDiff = new long[ dim ];

			boolean isGoodTile = true, samePosition = true, sameSize = true;
			for ( int d = 0; d < dim; d++ )
			{
				dist[ d ] = tilesOrig[ i ].getPosition( d ) - tilesMod[ i ].getPosition( d );
				sizeDiff[ d ] = tilesOrig[ i ].getSize( d ) - tilesMod[ i ].getSize( d );

				if ( dist[ d ] != 0 )
					samePosition = false;

				if ( sizeDiff[ d ] != 0 )
					sameSize = false;

				if ( Math.abs( dist[ d ] ) > tilesOrig[ i ].getSize( d ) / 3 )
					isGoodTile = false;
			}
			if ( samePosition )
				haveSamePosition++;
			else
				System.out.println( "Position diff: " + tilesOrig[ i ].getIndex() + ": " + Arrays.toString( dist ) );

			if ( sameSize )
				haveSameSize++;
			else
				System.out.println( "Size diff: " + tilesOrig[ i ].getIndex() + ": " + Arrays.toString( sizeDiff ) );

			if ( tileSize == null )
			{
				tileSize = tilesOrig[ i ].getSize().clone();
				allTilesHaveSameSize = sameSize;
			}
			else
			{
				for ( int d = 0; d < dim; d++ )
					allTilesHaveSameSize &= sameSize && tileSize[ d ] == tilesOrig[ i ].getSize( d );
			}

			//System.out.println( tilesOrig[ i ].getIndex() + ": " + Arrays.toString( dist ) );

			if ( isGoodTile )
			{
				goodTiles.add( tilesOrig[ i ] );
			}
			else
			{
				//System.out.println( Arrays.toString( dist ) );
				badTiles.add( tilesOrig[ i ] );
			}


			for ( int d = 0; d < range.length; d++ )
			{
				range[ d ][ 0 ] = ( int ) Math.min( dist[ d ], range[ d ][ 0 ] );
				range[ d ][ 1 ] = ( int ) Math.max( dist[ d ], range[ d ][ 1 ] );
			}
		}
		if ( haveSamePosition == n )
			System.out.println( "The configurations have identical positions!" );
		else
			System.out.println( "Identical positions: " + haveSamePosition );

		if ( haveSameSize == n )
			System.out.println( "The configurations have identical size!" );
		else
			System.out.println( "Identical size: " + haveSameSize );

		if ( allTilesHaveSameSize )
			System.out.println( "Even better -- all tiles have the same size!" );

		System.out.println( "----------------------" );
		System.out.println( "Found " + goodTiles.size() + " good tiles out of " + n );

		System.out.println( "Range is " + Arrays.deepToString( range ) );


		if ( !badTiles.isEmpty() )
		{
			//System.out.println( "Bad tiles are: " );
			//for ( final TileInfo tile : badTiles )
			//	System.out.println( tile.getIndex() + ", position="+Arrays.toString( tile.getPosition() ));

			//System.out.println( "Removing " + badTilesIndices.size() + " bad ones: " + Arrays.toString( badTilesIndices.toArray() ) );
			//TileInfoJSONProvider.saveTilesConfiguration( goodTiles.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args[ 0 ], "_full" ), "_good" ) );*/
		}
	}
}
