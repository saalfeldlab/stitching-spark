package org.janelia.stitching.analysis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import net.imglib2.util.Pair;

public class FilterPairwiseShifts
{
	// TODO: Add flexibility. Currently hardcoded for Z
	public static void main( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args [ 0 ] );

		final TileInfo[] tiles = Utils.createTilesMap( shifts, true ).values().toArray( new TileInfo[ 0 ] );
		final Map< Integer, int[] > tileIndexToCoordinates = new HashMap<>();
		for ( final Pair< TileInfo, int[] > tileCoordinates : Utils.getTileCoordinates( tiles ) )
			tileIndexToCoordinates.put( tileCoordinates.getA().getIndex(), tileCoordinates.getB() );

		final int sizeBefore = shifts.size();
		for ( final Iterator< SerializablePairWiseStitchingResult > it = shifts.iterator(); it.hasNext(); )
		{
			final SerializablePairWiseStitchingResult shift = it.next();
			if ( tileIndexToCoordinates.get( shift.getTilePair().getA().getIndex() )[ 2 ] - tileIndexToCoordinates.get( shift.getTilePair().getB().getIndex() )[ 2 ] != 0 )
				it.remove();
		}
		final int sizeAfter = shifts.size();

		System.out.println( "Size before = " + sizeBefore + ", size after = " + sizeAfter );

		TileInfoJSONProvider.savePairwiseShifts( shifts, Utils.addFilenameSuffix( args[ 0 ], "_without_z" ) );
	}
}
