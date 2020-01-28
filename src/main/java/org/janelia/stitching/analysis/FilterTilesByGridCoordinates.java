package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.AxisMapping;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class FilterTilesByGridCoordinates
{
	public static void main( final String[] args ) throws Exception
	{
		final String tileConfigPath = args[ 0 ];
		final AxisMapping axisMapping = new AxisMapping( args[ 1 ] );
		final int dimension = AxisMapping.getDimension( args[ 2 ] );
		if ( dimension == -1 )
			throw new IllegalArgumentException( "Expected x/y/z, got " + args[ 2 ] );

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > tiles = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( tileConfigPath ) ) );
		final Map< Integer, int[] > tileGridCoordinates = Utils.getTilesCoordinatesMap( tiles.values().toArray( new TileInfo[ 0 ] ), axisMapping );

		if ( args.length <= 3 || args[ 3 ].isEmpty() )
		{
			// no range requested, only print the min and max grid coordinates in the specified dimension
			int minGridPosition = Integer.MAX_VALUE, maxGridPosition = Integer.MIN_VALUE;
			for ( final int[] gridCoords : tileGridCoordinates.values() )
			{
				minGridPosition = Math.min( gridCoords[ dimension ], minGridPosition );
				maxGridPosition = Math.max( gridCoords[ dimension ], maxGridPosition );
			}
			System.err.println( "Please provide a range of grid positions to extract in the format of <from>-<to> between " + minGridPosition + " and " + maxGridPosition + " in " + AxisMapping.getAxisStr( dimension ) );
			System.exit( 1 );
		}

		final int filteredGridPositionMin, filteredGridPositionMax;
		{
			final String gridPositionRangeStr = args[ 3 ].trim();
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
			filteredFilenameSuffix = String.valueOf( Math.min( filteredGridPositionMin, filteredGridPositionMax ) ) + AxisMapping.getAxisStr( dimension );
		else
			filteredFilenameSuffix = String.valueOf( filteredGridPositionMin ) + "-" + String.valueOf( filteredGridPositionMax ) + AxisMapping.getAxisStr( dimension );

		TileInfoJSONProvider.saveTilesConfiguration(
				filteredTiles.toArray( new TileInfo[ 0 ] ),
				dataProvider.getJsonWriter( Utils.addFilenameSuffix( tileConfigPath, "_" + filteredFilenameSuffix ) )
			);

		System.out.println( "Filtered " + filteredTiles.size() + " tiles out of " + tiles.size() + " with grid coordinates " + filteredFilenameSuffix );
	}
}
