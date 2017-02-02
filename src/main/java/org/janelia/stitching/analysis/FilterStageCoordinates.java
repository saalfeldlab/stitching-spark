package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import net.imglib2.util.Pair;

public class FilterStageCoordinates
{
	// TODO: add flexibility
	/*public static void main( final String[] args ) throws Exception
	{
		final int d = 2;
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args[ 0 ] );
		final List< Pair< TileInfo, int[] > > tileCoords = Utils.getTileCoordinates( Utils.createTilesMap( shifts, true ).values().toArray( new TileInfo[ 0 ] ) );
		System.out.println("There are "+tileCoords.size()+" tiles and " + shifts.size() + " pairwise shifts");

		int zMin = Integer.MAX_VALUE, zMax = Integer.MIN_VALUE;
		for ( final Pair< TileInfo, int[] > tileCoord : tileCoords )
		{
			zMin = Math.min( tileCoord.getB()[ d ], zMin );
			zMax = Math.max( tileCoord.getB()[ d ], zMax );
		}

		System.out.println( "zmin="+zMin+",zmax="+zMax );

		for ( int z = 13; z <= 16; ++z )
		{
			final Map< Integer, int[] > tileIndexToCoord = new TreeMap<>();
			for ( final Pair< TileInfo, int[] > tileCoord : tileCoords )
				tileIndexToCoord.put( tileCoord.getA().getIndex(), tileCoord.getB() );

			final List< SerializablePairWiseStitchingResult > shiftsDesired = new ArrayList<>();
			for ( final SerializablePairWiseStitchingResult shift : shifts )
			{
				boolean desired = true;
				for ( final TileInfo tile : shift.getTilePair().toArray() )
					if ( tileIndexToCoord.get( tile.getIndex() )[ d ] != z )
						desired = false;

				if ( desired )
					shiftsDesired.add( shift );
			}

			final TileInfo[] tilesDesired = Utils.createTilesMap( shiftsDesired, true ).values().toArray( new TileInfo[ 0 ] );
			System.out.println("There are "+tilesDesired.length+" desired tiles and " + shiftsDesired.size() + " desired pairwise shifts");

			final String newConfigPath = Utils.addFilenameSuffix(args[0], "_"+z+"z" ) ;
			TileInfoJSONProvider.saveTilesConfiguration( tilesDesired, newConfigPath );
			TileInfoJSONProvider.savePairwiseShifts( shiftsDesired, Utils.addFilenameSuffix(newConfigPath, "_pairwise") );
		}
	}*/

	public static void main( final String[] args ) throws Exception
	{
		final int d = 1;
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final List< Pair< TileInfo, int[] > > tileCoords = Utils.getTileCoordinates( tiles );
		System.out.println("There are "+tileCoords.size()+" tiles");

		int yMin = Integer.MAX_VALUE, yMax = Integer.MIN_VALUE;
		for ( final Pair< TileInfo, int[] > tileCoord : tileCoords )
		{
			yMin = Math.min( tileCoord.getB()[ d ], yMin );
			yMax = Math.max( tileCoord.getB()[ d ], yMax );
		}

		System.out.println( "ymin="+yMin+",Ymax="+yMax );

		final Map< Integer, int[] > tileIndexToCoord = new TreeMap<>();
		for ( final Pair< TileInfo, int[] > tileCoord : tileCoords )
			tileIndexToCoord.put( tileCoord.getA().getIndex(), tileCoord.getB() );

		final List< TileInfo > tilesDesired = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			if ( tileIndexToCoord.get( tile.getIndex() )[ d ] >= 20 && tileIndexToCoord.get( tile.getIndex() )[ d ] <= 22 )
				tilesDesired.add( tile );

		System.out.println("There are "+tilesDesired.size()+" desired tiles");

		TileInfoJSONProvider.saveTilesConfiguration( tilesDesired.toArray( new TileInfo[ 0 ] ), Utils.addFilenameSuffix(args[0], "_20-22y" ) );
	}
}
