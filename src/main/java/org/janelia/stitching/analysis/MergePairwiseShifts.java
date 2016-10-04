package org.janelia.stitching.analysis;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;

public class MergePairwiseShifts
{
	public static void main( final String[] args ) throws Exception
	{
		String workingDir = null;
		final Map< Integer, Map< Integer, SerializablePairWiseStitchingResult > > mergedShiftsMap = new TreeMap<>();
		for ( final String input : args )
		{
			if ( workingDir == null )
				workingDir = Paths.get( input ).getParent().toString()+"/";
			else if (!workingDir.isEmpty() && !workingDir.equals( Paths.get( input ).getParent().toString()+"/" ))
				workingDir="";

			final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( input );
			for ( final SerializablePairWiseStitchingResult shift : shifts )
			{
				final int ind1 = Math.min( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() );
				final int ind2 = Math.max( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() );

				if ( !mergedShiftsMap.containsKey( ind1 ) )
					mergedShiftsMap.put( ind1, new TreeMap<>() );

				if ( !mergedShiftsMap.get( ind1 ).containsKey( ind2 ) || ( shift.getIsValidOverlap() && !mergedShiftsMap.get( ind1 ).get( ind2 ).getIsValidOverlap() ) )
					mergedShiftsMap.get( ind1 ).put( ind2, shift );
			}
		}

		final List< SerializablePairWiseStitchingResult > mergedShifts = new ArrayList<>();
		for ( final Map< Integer, SerializablePairWiseStitchingResult > entry : mergedShiftsMap.values() )
			for ( final SerializablePairWiseStitchingResult shift : entry.values() )
				mergedShifts.add( shift );

		if ( !mergedShifts.isEmpty() )
			TileInfoJSONProvider.savePairwiseShifts( mergedShifts, workingDir +"merged_pairwise_shifts_set.json" );


		int valid = 0;
		for ( final SerializablePairWiseStitchingResult shift : mergedShifts )
			if ( shift.getIsValidOverlap() )
				valid++;
		System.out.println( "Total=" + mergedShifts.size() + ", valid="+valid );
	}
}
