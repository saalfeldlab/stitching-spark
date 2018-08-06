package org.janelia.stitching.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class ChooseBestPeak
{
	public static void main(final String[] args) throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult[] > shiftsMulti = TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( args[0] ) );
		final List< SerializablePairWiseStitchingResult > shifts = new ArrayList<>();

		for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMulti )
			shifts.add( shiftMulti[ 0 ] );

		int valid = 0;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( shift.getIsValidOverlap() )
				valid++;
		System.out.println( "There are " + valid + " valid pairs out of " + shifts.size() );

		TileInfoJSONProvider.savePairwiseShifts( shifts, dataProvider.getJsonWriter( Utils.addFilenameSuffix( args[0], "_best" ) ) );
	}
}
