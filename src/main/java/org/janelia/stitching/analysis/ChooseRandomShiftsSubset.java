package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class ChooseRandomShiftsSubset 
{
	public static void main( String[] args ) throws Exception
	{
		List<SerializablePairWiseStitchingResult> origShifts = TileInfoJSONProvider.loadPairwiseShifts(args[0]);
		ArrayList<SerializablePairWiseStitchingResult> finalShifts = new ArrayList<>();
		double prob = 0.01;
		Random rnd = new Random();
		int origCount = 0, finalCount = 0;
		for ( SerializablePairWiseStitchingResult shift : origShifts )
		{
			if ( shift.getIsValidOverlap() )
			{
				origCount++;
				
				if ( rnd.nextDouble() > prob )
				{
					shift.setIsValidOverlap( false );
					finalShifts.add(shift);
				}
				else
				{
					finalCount++;
				}
			}
			else
			{
				finalShifts.add(shift);
			}
		}
		
		System.out.println( "origCount="+origCount );
		System.out.println( "finalCount="+finalCount );
		TileInfoJSONProvider.savePairwiseShifts(finalShifts, Utils.addFilenameSuffix(args[0], "_subset"));
	}
}
