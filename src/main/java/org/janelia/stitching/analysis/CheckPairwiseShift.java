package org.janelia.stitching.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;

public class CheckPairwiseShift
{
	public static void main( final String[] args ) throws FileNotFoundException, IOException
	{
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args[ 0 ] );

		final int i1 = 1126;
		final int i2 = 1129;

		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			final int ind1 = Math.min( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() );
			final int ind2 = Math.max( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() );

			if ( i1 == ind1 && i2 == ind2 )
			{
				System.out.println( "valid="+shift.getIsValidOverlap() + ", offset="+Arrays.toString( shift.getOffset() ) + ", cross corr="+shift.getCrossCorrelation() );
			}
		}
	}
}
