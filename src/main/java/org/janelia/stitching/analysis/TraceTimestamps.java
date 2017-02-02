package org.janelia.stitching.analysis;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.janelia.util.ComparablePair;

import net.imglib2.util.Pair;

public class TraceTimestamps
{
	public static void main( final String[] args ) throws Exception
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final List< Pair< TileInfo, Long > > timestamps = Utils.getTileTimestamps( tiles );
		final List< Pair< TileInfo, int[] > > coordinates = Utils.getTileCoordinates( tiles );

		final int[][] coordsMinMax = new int[ 3 ][ 2 ];
		for ( int d = 0; d < coordsMinMax.length; d++ )
		{
			coordsMinMax[ d ][ 0 ] = Integer.MAX_VALUE;
			coordsMinMax[ d ][ 1 ] = Integer.MIN_VALUE;
		}

		for ( final Pair< TileInfo, int[] > pair : coordinates )
		{
			for ( int d = 0; d < pair.getB().length; d++ )
			{
				coordsMinMax[ d ][ 0 ] = Math.min( pair.getB()[ d ], coordsMinMax[ d ][ 0 ] );
				coordsMinMax[ d ][ 1 ] = Math.max( pair.getB()[ d ], coordsMinMax[ d ][ 1 ] );
			}
		}

		int coordsMaxLen = 0;
		for ( int d = 0; d < coordsMinMax.length; d++ )
			coordsMaxLen = Math.max( new Integer( coordsMinMax[ d ][ 1 ] ).toString().length(), coordsMaxLen );

		System.out.println( "Coordinate range:" );
		System.out.println( "   x : " + Arrays.toString( coordsMinMax[ 0 ] ) );
		System.out.println( "   y : " + Arrays.toString( coordsMinMax[ 1 ] ) );
		System.out.println( "   z : " + Arrays.toString( coordsMinMax[ 2 ] ) );

		final TreeMap< Integer, int[] > tileIndexToCoordinate = new TreeMap<>();
		for ( final Pair< TileInfo, int[] > pair : coordinates )
			tileIndexToCoordinate.put( pair.getA().getIndex(), pair.getB() );

		final TreeSet< ComparablePair< Long, Integer > > timestampToTileIndex = new TreeSet<>();
		for ( final Pair< TileInfo, Long > pair : timestamps )
			timestampToTileIndex.add( new ComparablePair<>( pair.getB(), pair.getA().getIndex() ) );

		System.out.println( "Tiles in total: " + tiles.length );
		System.out.println( "Tiles sorted by the timestamp: " + timestampToTileIndex.size() );

		try ( final PrintWriter writer = new PrintWriter( Utils.addFilenameSuffix( args[ 0 ], "_sort_by_timestamp.txt" ) ) )
		{
			for ( final Pair< Long, Integer > pair : timestampToTileIndex )
				writer.println( pad( pair.getA(), timestampToTileIndex.last().getA().toString().length() ) + ":   tile " + pad( new Long( pair.getB() ), tileIndexToCoordinate.lastKey().toString().length() ) + ",  coords = " + pad( tileIndexToCoordinate.get( pair.getB() ), coordsMaxLen ) );
		}

		try ( final PrintWriter writer = new PrintWriter( Utils.addFilenameSuffix( args[ 0 ], "_sort_by_index.txt" ) ) )
		{
			Integer lastIndex = null;
			for ( int i = 0; i < tiles.length; i++ )
			{
				if ( lastIndex != null && tiles[ i ].getIndex().intValue() != lastIndex.intValue() + 1 )
					throw new Exception( "Tiles are not sorted by index" );

				if ( tiles[ i ].getIndex().intValue() != timestamps.get( i ).getA().getIndex().intValue() || tiles[ i ].getIndex().intValue() != coordinates.get( i ).getA().getIndex().intValue() )
					throw new Exception( "Index mismatch" );

				writer.println( pad( new Long( tiles[ i ].getIndex() ), tileIndexToCoordinate.lastKey().toString().length() ) + ":   coords = " + pad( coordinates.get( i ).getB(), coordsMaxLen ) + ",   timestamp = " + pad( timestamps.get( i ).getB(), timestampToTileIndex.last().getA().toString().length() ) );

				lastIndex = tiles[ i ].getIndex();
			}
		}
	}

	private static String pad( final Long val, final int maxLen )
	{
		String s = val.toString();
		final int spaces = maxLen - s.length();
		for ( int i = 0; i < spaces; i++ )
			s = ' ' + s;
		return s;
	}
	private static String pad( final int[] val, final int maxLen )
	{
		final String[] arr = new String[ val.length ];
		for ( int i = 0; i < val.length; i++ )
			arr[ i ] = pad( ( long ) val[ i ], maxLen );
		return Arrays.toString( arr );
	}
}
