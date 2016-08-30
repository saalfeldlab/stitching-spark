package org.janelia.stitching.analysis;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.janelia.stitching.Boundaries;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;

/**
 * Evaluates the quality of pairwise matching by finding the farthest shift between any pair of tiles.
 * We assume that initial tile positions (obtained from the microscope) are almost correct, so the farthest shift here is considered as the worst.
 *
 * Large offset indicates that the phase correlation has failed to identify good shifts for some of the tiles.
 * In this case you may want to:
 * 1. Increase the number of phase correlation peaks that should be investigated by the pairwise stitching algorithm.
 * 2. Preapply gaussian blur to tile images
 *
 * @author Igor Pisarev
 */

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
				dist[d] = (shift.getTilePair().second().getPosition( d ) - shift.getTilePair().first().getPosition( d )) - shift.getOffset( d );

			double score = 0;
			for ( int d = 0; d < dist.length; d++ )
				score -= dist[ d ];

			double av = 0;
			for ( int d = 0; d < dist.length; d++ )
				av -= shift.getTilePair().first().getSize( d );
			av /= dist.length;

			if ( score < av )
			{
				bad.put( score, shift );
				System.out.println( shift.getTilePair().first().getIndex() + " and " + shift.getTilePair().second().getIndex() + ": " + Arrays.toString( dist ) + ", cross=" + shift.getCrossCorrelation());
			}
		}

		int noCorrPairs = 0;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( shift.getCrossCorrelation() == 0 )
				noCorrPairs++;
		System.out.println( "no correlation pairs: " + noCorrPairs );


		System.out.println( "There are " + bad.size() + " bad pairs out of " + shifts.size() );
		System.out.println( "-----------------------------------" );

		final SerializablePairWiseStitchingResult worstShift = bad.firstEntry().getValue();
		final Boundaries overlap = TileOperations.getOverlappingRegionGlobal( worstShift.getTilePair().first(), worstShift.getTilePair().second() );
		System.out.println( "Worst score: " + bad.firstKey() + ", tiles " + worstShift.getTilePair().first().getIndex() + " and " + worstShift.getTilePair().second().getIndex() );
		System.out.println( worstShift.getTilePair().first().getIndex() + ": " + worstShift.getTilePair().first().getFilePath() );
		System.out.println( worstShift.getTilePair().second().getIndex() + ": " + worstShift.getTilePair().second().getFilePath() );
		System.out.println( "Initial overlap at " + Arrays.toString( overlap.getMin() ) + " with dimensions " + Arrays.toString( overlap.getDimensions() ) );
		final double dist[] = new double[ worstShift.getNumDimensions() ];
		for ( int d = 0; d < worstShift.getNumDimensions(); d++ )
			dist[d] = (worstShift.getTilePair().second().getPosition( d ) - worstShift.getTilePair().first().getPosition( d )) - worstShift.getOffset( d );
		System.out.println( "Offset from initial positions: " + Arrays.toString( dist ) );
		System.out.println( "-----------------------------------" );

	}
}
