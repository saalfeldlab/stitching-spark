package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;

public class UnitePairwiseShifts
{
	public static void main( final String[] args ) throws Exception
	{
		final Map< Integer, Map< Integer, SerializablePairWiseStitchingResult > > unitedShiftsMap = new TreeMap<>();
		for ( final String input : args )
		{
			final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( input );
			for ( final SerializablePairWiseStitchingResult shift : shifts )
			{
				final int ind1 = Math.min( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() );
				final int ind2 = Math.max( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() );

				if ( !unitedShiftsMap.containsKey( ind1 ) )
					unitedShiftsMap.put( ind1, new TreeMap<>() );

				if ( !unitedShiftsMap.get( ind1 ).containsKey( ind2 ) || ( shift.getIsValidOverlap() && !unitedShiftsMap.get( ind1 ).get( ind2 ).getIsValidOverlap() ) )
					unitedShiftsMap.get( ind1 ).put( ind2, shift );
			}
		}

		final List< SerializablePairWiseStitchingResult > unitedShifts = new ArrayList<>();
		for ( final Map< Integer, SerializablePairWiseStitchingResult > entry : unitedShiftsMap.values() )
			for ( final SerializablePairWiseStitchingResult shift : entry.values() )
				unitedShifts.add( shift );

		if ( !unitedShifts.isEmpty() )
			TileInfoJSONProvider.savePairwiseShifts( unitedShifts, "united_pairwise_shifts_set.json" );


		int valid = 0;
		for ( final SerializablePairWiseStitchingResult shift : unitedShifts )
			if ( shift.getIsValidOverlap() )
				valid++;
		System.out.println( "Total=" + unitedShifts.size() + ", valid="+valid );
	}
}
