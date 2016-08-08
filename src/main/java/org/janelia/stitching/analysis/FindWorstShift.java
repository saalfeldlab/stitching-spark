package org.janelia.stitching.analysis;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.janelia.stitching.Boundaries;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;

public class FindWorstShift
{
	public static void main( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args[ 0 ] );


		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( !shift.getIsValidOverlap() )
				throw new Exception( "Invalid overlap" );


		final TreeMap< Double, SerializablePairWiseStitchingResult > bad = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			final double dist[] = new double[ shift.getNumDimensions() ];

			for ( int d = 0; d < shift.getNumDimensions(); d++ )
				dist[d] = (shift.getPairOfTiles()._2.getPosition( d ) - shift.getPairOfTiles()._1.getPosition( d )) - shift.getOffset( d );

			double score = 0;
			for ( int d = 0; d < dist.length; d++ )
				score -= dist[ d ];

			double av = 0;
			for ( int d = 0; d < dist.length; d++ )
				av -= shift.getPairOfTiles()._1.getSize( d );
			av /= dist.length;

			if ( score < av )
			{
				bad.put( score, shift );
				System.out.println( shift.getPairOfTiles()._1.getIndex() + " and " + shift.getPairOfTiles()._2.getIndex() + ": " + Arrays.toString( dist ) + ", cross=" + shift.getCrossCorrelation());
			}
		}

		int noCorrPairs = 0;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( shift.getCrossCorrelation() == 0 )
				noCorrPairs++;
		System.out.println( "no correlation pairs: " + noCorrPairs );


		// 340 - usual run
		// 29 - blurred run
		System.out.println( "There are " + bad.size() + " bad pairs out of " + shifts.size() );
		System.out.println( "-----------------------------------" );

		final SerializablePairWiseStitchingResult worstShift = bad.firstEntry().getValue();
		final Boundaries overlap = TileOperations.getOverlappingRegionGlobal( worstShift.getPairOfTiles()._1, worstShift.getPairOfTiles()._2 );
		System.out.println( "Worst score: " + bad.firstKey() + ", tiles " + worstShift.getPairOfTiles()._1.getIndex() + " and " + worstShift.getPairOfTiles()._2.getIndex() );
		System.out.println( "Initial overlap at " + Arrays.toString( overlap.getMin() ) + " with dimensions " + Arrays.toString( overlap.getDimensions() ) );
		System.out.println( "Offset: " + Arrays.toString( worstShift.getOffset() ) );
		System.out.println( "-----------------------------------" );

	}
}
