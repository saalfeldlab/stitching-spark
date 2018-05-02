package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class FilterTilesByGridCoordinates
{
	private static final char[] DIMENSION_STR = new char[] { 'x', 'y', 'z' };

	public static void main( final String[] args ) throws Exception
	{
		final String tileConfigPath = args[ 0 ];

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > tiles = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( tileConfigPath ) ) ) );
		final Map< Integer, int[] > tileGridCoordinates = Utils.getTilesCoordinatesMap( tiles.values().toArray( new TileInfo[ 0 ] ) );

		final int dimension = new String( DIMENSION_STR ).indexOf( args[ 1 ] );
		if ( dimension == -1 )
			throw new IllegalArgumentException( "Expected x/y/z, got " + args[ 1 ] );

		final int filteredGridPositionMin, filteredGridPositionMax;
		{
			final String gridPositionRangeStr = args[ 2 ].trim();
			final int delimiterIndex = gridPositionRangeStr.indexOf( "-" );
			if ( delimiterIndex == -1 )
			{
				filteredGridPositionMin = filteredGridPositionMax = Integer.parseInt( gridPositionRangeStr );
			}
			else
			{
				int overallGridPositionMin = Integer.MAX_VALUE, overallGridPositionMax = Integer.MIN_VALUE;
				for ( final int[] tileCoord : tileGridCoordinates.values() )
				{
					overallGridPositionMin = Math.min( tileCoord[ dimension ], overallGridPositionMin );
					overallGridPositionMax = Math.max( tileCoord[ dimension ], overallGridPositionMax );
				}

				filteredGridPositionMin = delimiterIndex == 0 ? overallGridPositionMin : Integer.parseInt( gridPositionRangeStr.substring( 0, delimiterIndex ) );
				filteredGridPositionMax = delimiterIndex == gridPositionRangeStr.length() - 1 ? overallGridPositionMax :  Integer.parseInt( gridPositionRangeStr.substring( delimiterIndex + 1 ) );
			}
		}

		if ( filteredGridPositionMin > filteredGridPositionMax )
			throw new IllegalArgumentException( "from > to" );

		final Map< Integer, List< TileInfo > > gridPositionTiles = new TreeMap<>();
		for ( final Entry< Integer, int[] > tileIndexAndGridCoordinate : tileGridCoordinates.entrySet() )
		{
			final int key = tileIndexAndGridCoordinate.getValue()[ dimension ];
			if ( !gridPositionTiles.containsKey( key ) )
				gridPositionTiles.put( key, new ArrayList<>() );
			gridPositionTiles.get( key ).add( tiles.get( tileIndexAndGridCoordinate.getKey() ) );
		}

		final List< TileInfo > filteredTiles = new ArrayList<>();
		for ( int gridPosition = filteredGridPositionMin; gridPosition <= filteredGridPositionMax; ++gridPosition )
			filteredTiles.addAll( gridPositionTiles.get( gridPosition ) );

		final String filteredFilenameSuffix;
		if ( filteredGridPositionMin == filteredGridPositionMax )
			filteredFilenameSuffix = String.valueOf( Math.min( filteredGridPositionMin, filteredGridPositionMax ) ) + DIMENSION_STR[ dimension ];
		else
			filteredFilenameSuffix = String.valueOf( filteredGridPositionMin ) + "-" + String.valueOf( filteredGridPositionMax ) + DIMENSION_STR[ dimension ];

		TileInfoJSONProvider.saveTilesConfiguration(
				filteredTiles.toArray( new TileInfo[ 0 ] ),
				dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( tileConfigPath, "_" + filteredFilenameSuffix ) ) )
			);

		System.out.println( "Filtered " + filteredTiles.size() + " tiles out of " + tiles.size() + " with grid coordinates " + filteredFilenameSuffix );
	}
}
