package org.janelia.stitching.analysis;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;

public class CheckFinalVsStageOverlaps
{
	private static final String[] DIMENSION_STR = new String[] { "x", "y" ,"z" };

	public static void main( final String[] args ) throws Exception
	{
		final String inputTilesPath = args[ 0 ], outputTilesPath = args[ 1 ];
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final TileInfo[] inputTiles = dataProvider.loadTiles( inputTilesPath ), outputTiles = dataProvider.loadTiles( outputTilesPath );

		final int dim = inputTiles[ 0 ].numDimensions();
		final List< TilePair > overlappingInputPairs = TileOperations.findOverlappingTiles( inputTiles );
		final Map< Integer, TileInfo > outputTilesMap = Utils.createTilesMap( dataProvider.loadTiles( outputTilesPath ) );

		@SuppressWarnings( "unchecked" )
		final List< Double >[] errorsInEachDimension = new List[ dim ];

		for ( int d = 0; d < dim; ++d )
		{
			double minError = Double.POSITIVE_INFINITY, maxError = Double.NEGATIVE_INFINITY, avgError = 0;
			int numProcessedPairs = 0;
			errorsInEachDimension[ d ] = new ArrayList<>();

			final List< TilePair > overlappingAdjacentInputPairs = FilterAdjacentShifts.filterAdjacentPairs( overlappingInputPairs, d );

			for ( final TilePair inputTilePair : overlappingAdjacentInputPairs )
			{
				if ( !outputTilesMap.containsKey( inputTilePair.getA().getIndex() ) || !outputTilesMap.containsKey( inputTilePair.getB().getIndex() ) )
					continue;

				final TilePair outputTilePair = new TilePair(
						outputTilesMap.get( inputTilePair.getA().getIndex() ),
						outputTilesMap.get( inputTilePair.getB().getIndex() )
					);

				final double inputOffset = inputTilePair.getB().getPosition( d ) - inputTilePair.getA().getPosition( d );
				final double outputOffset = outputTilePair.getB().getPosition( d ) - outputTilePair.getA().getPosition( d );

				final double error = outputOffset - inputOffset;
				errorsInEachDimension[ d ].add( error );

				minError = Math.min( error, minError );
				maxError = Math.max( error, maxError );
				avgError += error;
				++numProcessedPairs;
			}
			avgError /= numProcessedPairs;

			System.out.println( String.format(
					"Adjacent overlapping pairs in %s: input=%d, output=%d",
					DIMENSION_STR[ d ],
					overlappingAdjacentInputPairs.size(),
					numProcessedPairs
				) );

			System.out.println( String.format(
					"Offset error in %s: min=%.2f, avg=%.2f, max=%.2f",
					DIMENSION_STR[ d ],
					minError,
					avgError,
					maxError
				) );

			System.out.println();
		}

		for ( int d = 0; d < dim; ++d )
		{
			try ( final PrintWriter writer = new PrintWriter( PathResolver.get( PathResolver.getParent( outputTilesPath ), "offset-errors-" + DIMENSION_STR[ d ] + ".txt" ) ) )
			{
				Collections.sort( errorsInEachDimension[ d ] );
				for ( final double error : errorsInEachDimension[ d ] )
					writer.println( error );
			}
		}
	}
}
