package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class ChooseSameSubset {

	public static void main( String[] args ) throws Exception
	{
		List< SerializablePairWiseStitchingResult > origShifts = TileInfoJSONProvider.loadPairwiseShifts(args[0]);
		TreeMap<Integer, TreeMap<Integer, SerializablePairWiseStitchingResult>> subsetShiftsMap = Utils.createPairwiseShiftsMap(TileInfoJSONProvider.loadPairwiseShifts(args[1]), false); 
		ArrayList< SerializablePairWiseStitchingResult > finalShifts = new ArrayList<>();
		
		int totalCount = 0, finalCount = 0;
		for ( final SerializablePairWiseStitchingResult shift : origShifts )
		{
			if ( shift.getIsValidOverlap() )
			{
				totalCount++;
				
				int ind1 = Math.min(shift.getTilePair().getA().getIndex(), shift.getTilePair().getB().getIndex());
				int ind2 = Math.max(shift.getTilePair().getA().getIndex(), shift.getTilePair().getB().getIndex());

				if ( !subsetShiftsMap.containsKey(ind1) || !subsetShiftsMap.get(ind1).containsKey(ind2))
				{
					finalCount++;
					finalShifts.add(shift);
				}
			}
		}
		
		System.out.println("totalCount="+totalCount);
		System.out.println("finalCount="+finalCount);
		
		TileInfoJSONProvider.savePairwiseShifts(finalShifts, Utils.addFilenameSuffix(args[0], "_subset"));
	}
}
