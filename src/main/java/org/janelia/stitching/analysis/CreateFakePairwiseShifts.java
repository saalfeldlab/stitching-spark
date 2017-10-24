package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.Boundaries;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;

/**
 * @author Igor Pisarev
 */

public class CreateFakePairwiseShifts
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final List< TilePair > overlappingPairs = TileOperations.findOverlappingTiles( tiles );
		final List< SerializablePairWiseStitchingResult > nonAdjacentShifts = new ArrayList<>();

		final int[] adjShiftsCount = new int[ 3 ];

		for ( final TilePair pair : overlappingPairs )
		{
			final Boundaries overlap = TileOperations.getOverlappingRegionGlobal( pair.getA(), pair.getB() );

			final boolean[] shortEdges = new boolean[overlap.numDimensions() ];
			for ( int d = 0; d < overlap.numDimensions(); d++ )
			{
				final int maxPossibleOverlap = ( int ) Math.min( pair.getA().getSize( d ), pair.getB().getSize( d ) );
				if ( overlap.dimension( d ) < maxPossibleOverlap / 2 )
					shortEdges[d] = true;
			}

			if (
					( !shortEdges[0] || shortEdges[1] || shortEdges[2] ) && // x
					( shortEdges[0] || !shortEdges[1] || shortEdges[2] ) && // y
					( shortEdges[0] || shortEdges[1] || !shortEdges[2] ) && // z

					true
					)
			{
				// non-adjacent shift doesn't need to be computed, replace it with a fake
				final SerializablePairWiseStitchingResult shift = new SerializablePairWiseStitchingResult( pair, new float[pair.getA().numDimensions()], 0.f, 0.f );
				shift.setIsValidOverlap( false );
				nonAdjacentShifts.add( shift );
			}
			else
			{
				boolean check = false;
				if ( !( !shortEdges[0] || shortEdges[1] || shortEdges[2] ) )
				{
					check = true;
					adjShiftsCount[0]++;
				}

				if ( !( shortEdges[0] || !shortEdges[1] || shortEdges[2] ) )
				{
					if ( check )
						throw new Exception( "Check failed for Y");
					check = true;
					adjShiftsCount[1]++;
				}

				if ( !( shortEdges[0] || shortEdges[1] || !shortEdges[2] ) )
				{
					if ( check )
						throw new Exception( "Check failed for Z");
					check = true;
					adjShiftsCount[2]++;
				}
			}
		}

		System.out.println( "Total number of pairs = " + overlappingPairs.size() );
		System.out.println( "Adjacent pairs = " + ( overlappingPairs.size() - nonAdjacentShifts.size() ));

		if ( overlappingPairs.size() - nonAdjacentShifts.size() != adjShiftsCount[0]+adjShiftsCount[1]+adjShiftsCount[2] )
			throw new Exception( "Adj shifts count mismatch" );
		System.out.println( "Adjacent pairs for X = " + adjShiftsCount[0]);
		System.out.println( "Adjacent pairs for Y = " + adjShiftsCount[1]);
		System.out.println( "Adjacent pairs for Z = " + adjShiftsCount[2]);

		TileInfoJSONProvider.savePairwiseShifts( nonAdjacentShifts, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 0 ], "_pairwise_fake" ) ) ) );

		System.out.println( "Done" );
	}
}
