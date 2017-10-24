package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;

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
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );

		int validShiftsCount = 0;
		int badShiftsCount = 0;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			if ( !shift.getIsValidOverlap() )
				continue;

			validShiftsCount++;
			final TilePair pair = shift.getTilePair();
			final double dist[] = new double[ shift.getNumDimensions() ];

			for ( int d = 0; d < shift.getNumDimensions(); d++ )
				dist[d] = (pair.getB().getPosition( d ) - pair.getA().getPosition( d )) - shift.getOffset( d );

			//if ( Math.abs( dist[ 0 ] ) >= 38 || Math.abs( dist[ 1 ] ) >= 36 || Math.abs( dist[ 2 ] ) >= 44 ) // Yoshi
			if ( Math.abs( dist[ 0 ] ) >= 25 || Math.abs( dist[ 1 ] ) >= 20 || Math.abs( dist[ 2 ] ) >= 57 ) // Sample9
			{
				shift.setIsValidOverlap( false );
				badShiftsCount++;
			}

			/*final Boundaries overlap = TileOperations.getOverlappingRegionGlobal( pair.first(), pair.second() );
			if ( shift.getCrossCorrelation() >= 0.9 && Math.abs( pair.second().getPosition(2) - pair.first().getPosition( 2 ) ) < 100 )
				//writer.println( overlap.dimension( 0 ) + " " +overlap.dimension( 1 ) );
				writer.println( dist[0] + " " + dist[1] );
			//if ( Math.abs( pair.second().getPosition(2) - pair.first().getPosition( 2 ) ) < 100 )
			//	writer.println( Math.abs(pair.first().getPosition(0) + pair.first().getSize(0) - pair.second().getPosition( 0 )) + " " +
			//			Math.abs(pair.first().getPosition(1) + pair.first().getSize(1) - pair.second().getPosition( 1 )) );

			//			if ( shift.getCrossCorrelation() >= 0.8 && Math.abs( pair.second().getPosition(2) - pair.first().getPosition( 2 ) ) > 100 )
			//				writer.println( dist[0] +" " + dist[1] +" " + dist[2] + " " + Math.abs(pair.first().getPosition(2) + pair.first().getSize(2) - pair.second().getPosition( 2 )) );

			double score = 0;
			for ( int d = 0; d < dist.length; d++ )
				score -= dist[ d ];

			double av = 0;
			for ( int d = 0; d < dist.length; d++ )
				av -= shift.getTilePair().first().getSize( d );
			av /= dist.length;


			boolean isBad = false;
			for ( int d = 0; d < dist.length; d++ )
				if ( Math.abs( dist[ d ] ) > 100 )
					isBad = true;
			//if ( score < av )
			if (isBad)
			{
				badShifts.put( score, shift );
				System.out.println( shift.getTilePair().first().getIndex() + " and " + shift.getTilePair().second().getIndex() + ": " + Arrays.toString( dist ) + ", crossCorr=" + shift.getCrossCorrelation());
			}*/
		}

		/*int noCorrPairs = 0;
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( shift.getCrossCorrelation() == 0 )
				noCorrPairs++;
		System.out.println( "no correlation pairs: " + noCorrPairs );*/


		System.out.println( "There are " + badShiftsCount + " bad pairs out of " + validShiftsCount + " (total="+shifts.size()+")" );
		System.out.println( "-----------------------------------" );

		/*final SerializablePairWiseStitchingResult worstShift = badShifts.firstEntry().getValue();
		final Boundaries overlap = TileOperations.getOverlappingRegionGlobal( worstShift.getTilePair().first(), worstShift.getTilePair().second() );
		System.out.println( "Worst score: " + badShifts.firstKey() + ", tiles " + worstShift.getTilePair().first().getIndex() + " and " + worstShift.getTilePair().second().getIndex() );
		System.out.println( worstShift.getTilePair().first().getIndex() + ": " + worstShift.getTilePair().first().getFilePath() );
		System.out.println( worstShift.getTilePair().second().getIndex() + ": " + worstShift.getTilePair().second().getFilePath() );
		System.out.println( "Initial overlap at " + Arrays.toString( overlap.getMin() ) + " with dimensions " + Arrays.toString( overlap.getDimensions() ) );
		final double dist[] = new double[ worstShift.getNumDimensions() ];
		for ( int d = 0; d < worstShift.getNumDimensions(); d++ )
			dist[d] = (worstShift.getTilePair().second().getPosition( d ) - worstShift.getTilePair().first().getPosition( d )) - worstShift.getOffset( d );
		System.out.println( "Offset from initial positions: " + Arrays.toString( dist ) );
		System.out.println( "-----------------------------------" );*/


		if ( badShiftsCount > 0)
		{
			System.out.println( "Filtering out bad shifts" );
			TileInfoJSONProvider.savePairwiseShifts( shifts, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 0 ], "_good" ) ) ) );
		}
	}
}
