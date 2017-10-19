package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class ChooseRandomShiftsSubset
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List<SerializablePairWiseStitchingResult> origShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( args[0] ) );
		final ArrayList<SerializablePairWiseStitchingResult> finalShifts = new ArrayList<>();
		final double prob = 0.01;
		final Random rnd = new Random();
		int origCount = 0, finalCount = 0;
		for ( final SerializablePairWiseStitchingResult shift : origShifts )
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
		TileInfoJSONProvider.savePairwiseShifts( finalShifts, dataProvider.getJsonWriter( Utils.addFilenameSuffix(args[0], "_subset") ) );
	}
}
