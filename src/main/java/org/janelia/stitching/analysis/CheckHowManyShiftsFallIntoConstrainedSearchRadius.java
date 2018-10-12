package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.DoubleStream;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.ErrorEllipse;
import org.janelia.stitching.OffsetUncertaintyEstimator;
import org.janelia.stitching.PairwiseTileOperations;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.SubTile;
import org.janelia.stitching.TileInfoJSONProvider;

import net.imglib2.RealPoint;
import net.imglib2.util.Intervals;

public class CheckHowManyShiftsFallIntoConstrainedSearchRadius
{
	private static final double searchRadiusAsTileSizeRatio = 0.1;

	public static void main( final String[] args ) throws Exception
	{
		final String pairwiseShiftsPath = args[ 0 ];
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwiseShiftsPath ) ) );

		int numInvalidShifts = 0, numPairsWhereEstimatedOffsetIsInsideSearchRadius = 0;
		final List< SerializablePairWiseStitchingResult > pairsOutsideSearchRadius = new ArrayList<>();

		for ( final SerializablePairWiseStitchingResult pairwiseShift : pairwiseShifts )
		{
			if ( !pairwiseShift.getIsValidOverlap() )
			{
				++numInvalidShifts;
				continue;
			}

			final SubTile[] subTiles = pairwiseShift.getSubTilePair().toArray();

			final double[] uncorrelatedErrorEllipseRadius = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipseRadius(
					subTiles[ 0 ].getFullTile().getSize(),
					searchRadiusAsTileSizeRatio
				);
			final ErrorEllipse movingSubTileSearchRadius = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipse( uncorrelatedErrorEllipseRadius );
			movingSubTileSearchRadius.setErrorEllipseTransform(
					PairwiseTileOperations.getErrorEllipseTransform( subTiles, pairwiseShift.getEstimatedFullTileTransformPair().toArray() )
				);
			System.out.println( String.format(
					"%s: created uncorrelated error ellipse of size %s centered at %s to constrain matching",
					pairwiseShift.getSubTilePair(),
					Arrays.toString( Intervals.dimensionsAsLongArray( Intervals.smallestContainingInterval( movingSubTileSearchRadius.estimateBoundingBox() ) ) ),
					Arrays.toString( DoubleStream.of( movingSubTileSearchRadius.getTransformedEllipseCenter() ).mapToLong( val -> Math.round( val ) ).toArray() )
				) );

			if ( movingSubTileSearchRadius.testPeak( new RealPoint( pairwiseShift.getOffset() ) ) )
				++numPairsWhereEstimatedOffsetIsInsideSearchRadius;
			else
				pairsOutsideSearchRadius.add( pairwiseShift );
		}

		System.out.println( System.lineSeparator() + String.format(
				"Number of pairs where the estimated offset is inside the constrained search radius: %d out of %d (and %d invalid shifts)",
				numPairsWhereEstimatedOffsetIsInsideSearchRadius,
				pairwiseShifts.size() - numInvalidShifts,
				numInvalidShifts
			) );

		if ( !pairsOutsideSearchRadius.isEmpty() )
		{
			System.out.println( System.lineSeparator() + "Pairs outside search radius:" );
			for ( final SerializablePairWiseStitchingResult pairwiseShift : pairsOutsideSearchRadius )
			{
				final SubTile[] subTiles = pairwiseShift.getSubTilePair().toArray();

				final double[] uncorrelatedErrorEllipseRadius = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipseRadius(
						subTiles[ 0 ].getFullTile().getSize(),
						searchRadiusAsTileSizeRatio
					);
				final ErrorEllipse movingSubTileSearchRadius = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipse( uncorrelatedErrorEllipseRadius );
				movingSubTileSearchRadius.setErrorEllipseTransform(
						PairwiseTileOperations.getErrorEllipseTransform( subTiles, pairwiseShift.getEstimatedFullTileTransformPair().toArray() )
					);

				final double offsetUnitLength = movingSubTileSearchRadius.getOffsetUnitLength( pairwiseShift.getOffset() );
				System.out.println( String.format( "  %s: offset unit length=%.5f", pairwiseShift.getSubTilePair().toString(), offsetUnitLength ) );
			}
		}
	}
}
