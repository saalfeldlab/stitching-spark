package org.janelia.stitching.analysis;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;

public class TraceHighestCrossCorrelation
{
	public static void main( final String[] args ) throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		double maxCrossCorr = 0;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			System.out.println( "   " + shift.getCrossCorrelation() );
			maxCrossCorr = Math.max( shift.getCrossCorrelation(), maxCrossCorr );
		}
		System.out.println( "maxCrossCorr = " + maxCrossCorr );
	}
}
