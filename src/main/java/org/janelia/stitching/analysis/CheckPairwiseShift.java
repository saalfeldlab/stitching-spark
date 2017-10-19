package org.janelia.stitching.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;

public class CheckPairwiseShift
{
	public static void main( final String[] args ) throws FileNotFoundException, IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( args[ 0 ] ) );

		final int i1 = 1126;
		final int i2 = 1129;

		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			final int ind1 = Math.min( shift.getTilePair().getA().getIndex(), shift.getTilePair().getB().getIndex() );
			final int ind2 = Math.max( shift.getTilePair().getA().getIndex(), shift.getTilePair().getB().getIndex() );

			if ( i1 == ind1 && i2 == ind2 )
			{
				System.out.println( "valid="+shift.getIsValidOverlap() + ", offset="+Arrays.toString( shift.getOffset() ) + ", cross corr="+shift.getCrossCorrelation() );
			}
		}
	}
}
