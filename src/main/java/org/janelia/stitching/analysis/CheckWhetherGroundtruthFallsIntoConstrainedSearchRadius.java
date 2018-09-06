package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.DoubleStream;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.ErrorEllipse;
import org.janelia.stitching.OffsetUncertaintyEstimator;
import org.janelia.stitching.PairwiseTileOperations;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.SubTile;
import org.janelia.stitching.SubTileOperations;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TransformedTileOperations;
import org.janelia.stitching.Utils;
import org.janelia.util.Conversions;

import mpicbg.models.Point;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.Translation;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;

public class CheckWhetherGroundtruthFallsIntoConstrainedSearchRadius
{
	private static final int fixedIndex = 0;
	private static final int movingIndex = 1;

	private static final double searchRadiusAsTileSizeRatio = 0.1;

	public static void main( final String[] args ) throws Exception
	{
		final String groundtruthTilesPath = args[ 0 ], pairwiseShiftsPath = args[ 1 ];
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > groundtruthTilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( groundtruthTilesPath ) ) ) );
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwiseShiftsPath ) ) );

		final List< SerializablePairWiseStitchingResult > removedPairsNotInGroundtruth = new ArrayList<>();
		int numInvalidPairs = 0;
		for ( final Iterator< SerializablePairWiseStitchingResult > it = pairwiseShifts.iterator(); it.hasNext(); )
		{
			final SerializablePairWiseStitchingResult pairwiseShift = it.next();
			if ( !pairwiseShift.getIsValidOverlap() )
			{
				++numInvalidPairs;
				it.remove();
			}
			else
			{
				for ( int i = 0; i < 2; ++i )
				{
					if ( !groundtruthTilesMap.containsKey( pairwiseShift.getSubTilePair().toArray()[ i ].getFullTile().getIndex() ) )
					{
						removedPairsNotInGroundtruth.add( pairwiseShift );
						it.remove();
						break;
					}
				}
			}
		}
		System.out.println( "Removed " + numInvalidPairs + " invalid pairs where no correlation peaks were found" );
		System.out.println( "Removed " + removedPairsNotInGroundtruth.size() + " pairs where at least one tile is not contained in the groundtruth set:" );
		for ( final SerializablePairWiseStitchingResult pairwiseShift : removedPairsNotInGroundtruth )
			System.out.println( "  " + pairwiseShift.getSubTilePair() );
		System.out.println();

		int numPairsWhereEstimatedOffsetIsOutsideSearchRadius = 0;
		final List< Double > offsetDistancesInsideSearchRadius = new ArrayList<>();

		for ( final SerializablePairWiseStitchingResult pairwiseShift : pairwiseShifts )
		{
			final SubTile[] subTiles = pairwiseShift.getSubTilePair().toArray();

			final SubTile[] groundtruthSubTiles = new SubTile[ subTiles.length ];
			for ( int i = 0; i < subTiles.length; ++i )
			{
				final TileInfo groundtruthTile = groundtruthTilesMap.get( subTiles[ i ].getFullTile().getIndex() );

				final int[] subtileGridSize = new int[ groundtruthTile.numDimensions() ];
				Arrays.fill( subtileGridSize, 2 );
				final List< SubTile > subdividedGroundtruthTile = SubTileOperations.subdivideTiles( new TileInfo[] { groundtruthTile }, subtileGridSize );
				for ( final SubTile groundtruthSubTile : subdividedGroundtruthTile )
				{
					if ( Intervals.equals( groundtruthSubTile, subTiles[ i ] ) )
					{
						if ( groundtruthSubTiles[ i ] != null )
							throw new Exception( "duplicated subtile" );
						groundtruthSubTiles[ i ] = groundtruthSubTile;
					}
				}
				if ( groundtruthSubTiles[ i ] == null )
					throw new Exception( "no matching subtile" );
			}

			final double[][] groundtruthSubTilePositions = new double[ groundtruthSubTiles.length ][];
			for ( int i = 0; i < groundtruthSubTilePositions.length; ++i )
			{
				final AffineGet groundtruthTileTransform = TransformedTileOperations.getTileTransform( groundtruthSubTiles[ i ].getFullTile(), false );
				if ( !( groundtruthTileTransform instanceof Translation ) )
					throw new RuntimeException( "supposed to work with translation-only groundtruth" );

				groundtruthSubTilePositions[ i ] = new double[ groundtruthTileTransform.numDimensions() ];
				groundtruthTileTransform.apply( Intervals.minAsDoubleArray( groundtruthSubTiles[ i ] ), groundtruthSubTilePositions[ i ] );
			}

			final double[][] stageSubTilePositions = new double[ subTiles.length ][];
			for ( int i = 0; i < stageSubTilePositions.length; ++i )
			{
				stageSubTilePositions[ i ] = new double[ subTiles[ i ].numDimensions() ];
				for ( int d = 0; d < stageSubTilePositions[ i ].length; ++d )
					stageSubTilePositions[ i ][ d ] = subTiles[ i ].getFullTile().getStagePosition( d ) + subTiles[ i ].min( d );
			}

			final double[] groundtruthSubTilePairwiseOffset = new double[ 3 ];
			for ( int d = 0; d < groundtruthSubTilePairwiseOffset.length; ++d )
				groundtruthSubTilePairwiseOffset[ d ] = groundtruthSubTilePositions[ movingIndex ][ d ] - groundtruthSubTilePositions[ fixedIndex ][ d ];

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

			final double[] stageSubTilePairwiseOffset = new double[ 3 ];
			for ( int d = 0; d < groundtruthSubTilePairwiseOffset.length; ++d )
				stageSubTilePairwiseOffset[ d ] = stageSubTilePositions[ movingIndex ][ d ] - stageSubTilePositions[ fixedIndex ][ d ];
			if ( !testOffsetApprox( movingSubTileSearchRadius, stageSubTilePairwiseOffset ) )
				throw new RuntimeException( "bug? the stage offset is reported to be outside the constrained search radius" );
			if ( !Util.isApproxEqual( movingSubTileSearchRadius.getOffsetUnitLength( stageSubTilePairwiseOffset ), 0, 1e-8 ) )
				throw new RuntimeException( String.format( "bug? the stage offset converted to unit sphere coordinates in the error ellipse space should be 0" ) );

			if ( !testOffsetApprox( movingSubTileSearchRadius, Conversions.toDoubleArray( pairwiseShift.getOffset() ) ) )
				++numPairsWhereEstimatedOffsetIsOutsideSearchRadius;
//				throw new RuntimeException( "bug? the estimated offset is reported to be outside the constrained search radius" );

			if ( testOffsetApprox( movingSubTileSearchRadius, groundtruthSubTilePairwiseOffset ) )
			{
				offsetDistancesInsideSearchRadius.add( Point.distance(
						new Point( groundtruthSubTilePairwiseOffset ),
						new Point( Conversions.toDoubleArray( pairwiseShift.getOffset() ) ) )
					);
			}
			else
			{
				System.out.println( String.format(
						"  groundtruth offset is outside the constrained search radius for pair %s, error between groundtruth and estimated offset is %.2f",
						pairwiseShift.getSubTilePair(),
						Point.distance(
								new Point( groundtruthSubTilePairwiseOffset ),
								new Point( Conversions.toDoubleArray( pairwiseShift.getOffset() ) ) )
					) );
			}
		}

		System.out.println( System.lineSeparator() + String.format(
				"Number of pairs where the groundtruth offset is inside the constrained search radius: %d out of %d",
				offsetDistancesInsideSearchRadius.size(),
				pairwiseShifts.size()
			) );

		double avgError = 0, maxError = 0;
		for ( final double error : offsetDistancesInsideSearchRadius )
		{
			avgError += error;
			maxError = Math.max( error, maxError );
		}
		avgError /= offsetDistancesInsideSearchRadius.size();
		System.out.println( String.format( "Groundtruth vs. estimated offset errors: avg=%.2f, max=%.2f", avgError, maxError ) );

		System.out.println( "Number of pairs where estimated offset is outside the constrained search radius: " + numPairsWhereEstimatedOffsetIsOutsideSearchRadius );
	}

	private static boolean testOffsetApprox( final ErrorEllipse errorEllipse, final double... offset )
	{
		final double offsetUnitLength = errorEllipse.getOffsetUnitLength( offset );
		return offsetUnitLength <= 1 || Util.isApproxEqual( offsetUnitLength, 1, 1e-1 );
	}
}
