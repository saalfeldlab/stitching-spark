package mpicbg.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class SimilarityModelConfigurationTest
{
	private static final double REGULARIZER_TRANSLATION = 0.5;

	public static void main( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult[] > shiftsMulti = TileInfoJSONProvider.loadPairwiseShiftsMulti( "/nrs/saalfeld/igor/MB_310C_run2/test-similarity-model/iter0/pairwise.json" );

		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMapMulti( shiftsMulti, false );

		final TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult[] > > connectedTiles = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMulti )
		{
			final int[] vals = new int[] { shiftMulti[ 0 ].getTilePair().getA().getIndex().intValue(), shiftMulti[ 0 ].getTilePair().getB().getIndex().intValue() };
			for ( int i = 0; i < 2; ++i )
			{
				if ( !connectedTiles.containsKey( vals[ i ] ) )
					connectedTiles.put( vals[ i ], new TreeMap<>() );
				connectedTiles.get( vals[ i ] ).put( vals[ ( i + 1 ) % 2 ], shiftMulti );
			}
		}

		final List< Integer > square = new ArrayList<>();
		A: for ( final Entry< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult[] > > firstCornerTileEntry : connectedTiles.entrySet() )
		{
			for ( final Entry< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult[] > > secondCornerTileEntry : connectedTiles.entrySet() )
			{
				if ( firstCornerTileEntry.getKey().intValue() == secondCornerTileEntry.getKey().intValue() )
					continue;

				final Set< Integer > commonNeighbors = new TreeSet<>( firstCornerTileEntry.getValue().keySet() );
				commonNeighbors.retainAll( secondCornerTileEntry.getValue().keySet() );
				commonNeighbors.removeAll( Arrays.asList( firstCornerTileEntry.getKey(), secondCornerTileEntry.getKey() ) );
				if ( commonNeighbors.size() >= 2 )
				{
					final List< Integer > commonNeighborsList = new ArrayList<>( commonNeighbors );
					square.addAll( Arrays.asList( firstCornerTileEntry.getKey(), commonNeighborsList.get( 0 ), secondCornerTileEntry.getKey(), commonNeighborsList.get( 1 ) ) );
					break A;
				}
			}
		}
		if ( square.isEmpty() )
			throw new Exception( "no square subconfiguration found" );

		for ( int i = 1; i < square.size(); ++i )
			if ( square.get( i ) <= square.get( i - 1 ) )
				throw new Exception( "inconsistent square subconfiguration: " + square );

		System.out.println( "Found square formed by tiles " + square + ":" );
		for ( final Integer tileIndex : square )
			System.out.println( "  " + Utils.getTileCoordinatesString( tilesMap.get( tileIndex ) ) );
		System.out.println();

		final TreeMap< Integer, Tile< ? > > tiles = new TreeMap<>();
		for ( final Integer index : square )
		{
			tiles.put( index, new Tile<>(
					new RigidModel3D()

//					new InterpolatedAffineModel3D<>(
//							new SimilarityModel3D(),
//							new TranslationModel3D(),
//							REGULARIZER_TRANSLATION
//						)
				) );
		}

		for ( int i = 0; i < square.size(); ++i )
		{
			final int ind1 = square.get( i ), ind2 = square.get( ( i + 1 ) % square.size() );
			if ( !connectedTiles.get( ind1 ).containsKey( ind2 ) )
				throw new Exception( "inconsistent square configuration: tiles " + ind1 + " and " + ind2 + " are not connected" );

			final Tile< ? > tile1 = tiles.get( ind1 ), tile2 = tiles.get( ind2 );
			tile1.addConnectedTile( tile2 );
			tile2.addConnectedTile( tile1 );

			final List< PointMatch > matches = new ArrayList<>();
			for ( final SerializablePairWiseStitchingResult shift : connectedTiles.get( ind1 ).get( ind2 ) )
				matches.add( new PointMatch( shift.getPointPair().getA(), shift.getPointPair().getB(), shift.getCrossCorrelation() ) );

			tile1.addMatches( matches );
			tile2.addMatches( PointMatch.flip( matches ) );

			// check if matches are close to the original shift between tiles
			double[] stageOffset = null;
			for ( final SerializablePairWiseStitchingResult shift : connectedTiles.get( ind1 ).get( ind2 ) )
			{
				if ( stageOffset == null )
				{
					stageOffset = new double[ shift.getNumDimensions() ];
					for ( int d = 0; d < stageOffset.length; ++d )
						stageOffset[ d ] = shift.getTilePair().getB().getPosition( d ) - shift.getTilePair().getA().getPosition( d );
					System.out.println( shift.getTilePair() );//+ ": stageOffset=" + Arrays.toString( stageOffset ) );
				}
				else
				{
					for ( int d = 0; d < stageOffset.length; ++d )
						if ( Math.abs( stageOffset[ d ] - shift.getTilePair().getB().getPosition( d ) - shift.getTilePair().getA().getPosition( d ) ) > 1e10 )
							throw new RuntimeException( "inconsistent tile configuration" );
				}

				for ( int d = 0; d < stageOffset.length; ++d )
					if ( Math.abs( shift.getOffset( d ) - ( shift.getPointPair().getA().getL()[ d ] - shift.getPointPair().getB().getL()[ d ] ) ) > 1e10 )
						throw new RuntimeException( "point matches are inconsistent with estimated offset" );

				final double[] diff = new double[ stageOffset.length ];
				for ( int d = 0; d < stageOffset.length; ++d )
					diff[ d ] = stageOffset[ d ] - shift.getOffset( d );
				System.out.println( "  diff=" + Arrays.toString( diff ) );
			}
			System.out.println();
		}

		final TileConfiguration tc = new TileConfiguration();
		tc.addTiles( tiles.values() );
		tc.preAlign();
		tc.optimize( 10, 1000, 1000, 1.f, 1 );

		System.out.println();

		for ( final Entry< Integer, Tile< ? > > entry : tiles.entrySet() )
		{
			final Tile< ? > tile = entry.getValue();
			final Affine3D< ? > affineModel = ( Affine3D< ? > ) tile.getModel();
			final double[][] matrix = new double[ 3 ][ 4 ];
			affineModel.toMatrix( matrix );
			final double[] scaling = new double[] { matrix[ 0 ][ 0 ], matrix[ 1 ][ 1 ], matrix[ 2 ][ 2 ] };
			final double[] translation = new double[] { matrix[ 0 ][ 3 ], matrix[ 1 ][ 3 ], matrix[ 2 ][ 3 ] };
			System.out.println( "Tile " + entry.getKey() + ": scaling=" + Arrays.toString( scaling ) + ", translation=" + Arrays.toString( translation ) + ", error=" + PointMatch.maxDistance( tile.getMatches() ) );
		}
	}
}
