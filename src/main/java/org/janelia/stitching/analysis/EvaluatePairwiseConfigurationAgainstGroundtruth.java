package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.PairwiseTileOperations;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.SubTile;
import org.janelia.stitching.SubTileOperations;
import org.janelia.stitching.SubTilePair;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TransformedTileOperations;
import org.janelia.stitching.Utils;
import org.janelia.util.Conversions;

import mpicbg.models.Model;
import net.imglib2.RealPoint;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;

public class EvaluatePairwiseConfigurationAgainstGroundtruth
{
	private static final int fixedIndex = 0;
	private static final int movingIndex = 1;

	public static < M extends Model< M > > void main( final String[] args ) throws Exception
	{
		final String groundtruthTilesPath = args[ 0 ], pairwiseConfigurationPath = args[ 1 ];
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > groundtruthTilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( groundtruthTilesPath ) ) ) );
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwiseConfigurationPath ) ) );

		int numShiftsRemovedNotInGroundtruth = 0;
		for ( final Iterator< SerializablePairWiseStitchingResult > it = pairwiseShifts.iterator(); it.hasNext(); )
		{
			final SerializablePairWiseStitchingResult pairwiseShift = it.next();
			for ( int i = 0; i < 2; ++i )
			{
				if ( !groundtruthTilesMap.containsKey( pairwiseShift.getSubTilePair().toArray()[ i ].getFullTile().getIndex() ) )
				{
					it.remove();
					++numShiftsRemovedNotInGroundtruth;
					break;
				}
			}
		}
		System.out.println( "Removed " + numShiftsRemovedNotInGroundtruth + " pairs where at least one tile is not contained in the groundtruth set" );

		System.out.println( "Calculating errors using pairwise match configuration consisting of " + pairwiseShifts.size() + " elements" );

		final Map< Integer, Map< Integer, List< Pair< SubTilePair, Double > > > > fixedToMovingPairwiseErrors = new TreeMap<>();

		for ( final SerializablePairWiseStitchingResult pairwiseShift : pairwiseShifts )
		{
			if ( !pairwiseShift.getIsValidOverlap() )
				throw new Exception( "invalid pairwise match" );

			final double[] estimatedMovingSubTileMiddlePointInFixedTile = PairwiseTileOperations.mapMovingSubTileMiddlePointIntoFixedTile(
					pairwiseShift.getSubTilePair().toArray(),
					pairwiseShift.getEstimatedFullTileTransformPair().toArray(),
					Conversions.toDoubleArray( pairwiseShift.getOffset() )
				);

			final SubTile[] groundtruthSubTiles = new SubTile[ 2 ];
			for ( int i = 0; i < groundtruthSubTiles.length; ++i )
			{
				final SubTile subTile = pairwiseShift.getSubTilePair().toArray()[ i ];
				final TileInfo groundtruthTile = groundtruthTilesMap.get( subTile.getFullTile().getIndex() );
				final int[] subtileGridSize = new int[ groundtruthTile.numDimensions() ];
				Arrays.fill( subtileGridSize, 2 );
				final List< SubTile > subdividedGroundtruthTile = SubTileOperations.subdivideTiles( new TileInfo[] { groundtruthTile }, subtileGridSize );
				for ( final SubTile groundtruthSubTile : subdividedGroundtruthTile )
				{
					if ( Intervals.equals( groundtruthSubTile, subTile ) )
					{
						if ( groundtruthSubTiles[ i ] != null )
							throw new Exception( "duplicated subtile" );
						groundtruthSubTiles[ i ] = groundtruthSubTile;
					}
				}
				if ( groundtruthSubTiles[ i ] == null )
					throw new Exception( "no matching subtile" );
			}

			final AffineGet[] groundtruthFullTileTransforms = new AffineGet[ 2 ];
			for ( int i = 0; i < groundtruthFullTileTransforms.length; ++i )
				groundtruthFullTileTransforms[ i ] = TransformedTileOperations.getTileTransform( groundtruthSubTiles[ i ].getFullTile(), false );

			final double[] groundtruthMovingSubTileMiddlePointInFixedTile = TransformedTileOperations.transformSubTileMiddlePoint(
					groundtruthSubTiles[ movingIndex ],
					PairwiseTileOperations.getMovingTileToFixedTileTransform( groundtruthFullTileTransforms )
				);

			final double error = Util.distance(
					new RealPoint( estimatedMovingSubTileMiddlePointInFixedTile ),
					new RealPoint( groundtruthMovingSubTileMiddlePointInFixedTile )
				);


			final SubTilePair subTilePair = pairwiseShift.getSubTilePair();
			final int fixedTileIndex = subTilePair.getFullTilePair().toArray()[ fixedIndex ].getIndex(), movingTileIndex = subTilePair.getFullTilePair().toArray()[ movingIndex ].getIndex();

			if ( !fixedToMovingPairwiseErrors.containsKey( fixedTileIndex ) )
				fixedToMovingPairwiseErrors.put( fixedTileIndex, new TreeMap<>() );

			if ( !fixedToMovingPairwiseErrors.get( fixedTileIndex ).containsKey( movingTileIndex ) )
				fixedToMovingPairwiseErrors.get( fixedTileIndex ).put( movingTileIndex, new ArrayList<>() );

			fixedToMovingPairwiseErrors.get( fixedTileIndex ).get( movingTileIndex ).add( new ValuePair<>( subTilePair, error ) );
		}


		double avgError = 0, maxError = 0;
		int numMatches = 0;
		for ( final Map< Integer, List< Pair< SubTilePair, Double > > > movingTileErrors : fixedToMovingPairwiseErrors.values() )
		{
			for ( final List< Pair< SubTilePair, Double > > subTilePairsAndErrors : movingTileErrors.values() )
			{
				for ( final Pair< SubTilePair, Double > subTilePairAndError : subTilePairsAndErrors )
				{
					final double error = subTilePairAndError.getB();
					maxError = Math.max( error, maxError );
					avgError += error;
					++numMatches;
				}
			}
		}

		if ( numMatches != pairwiseShifts.size() )
			throw new Exception( "number of pairwise shifts does not match" );

		avgError /= numMatches;

		System.out.println( System.lineSeparator() + String.format( "avg.error=%.2f, max.error=%.2f", avgError, maxError ) );
	}
}
