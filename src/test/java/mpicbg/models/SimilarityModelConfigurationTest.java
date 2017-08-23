package mpicbg.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;

public class SimilarityModelConfigurationTest
{
	private static final double REGULARIZER_TRANSLATION = 0.5;

	public static void main( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult[] > shiftsMulti = TileInfoJSONProvider.loadPairwiseShiftsMulti( "/nrs/saalfeld/igor/MB_310C_run2/test-similarity-model/iter0/pairwise.json" );

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
		square.add( connectedTiles.firstKey() );
		square.add( connectedTiles.firstEntry().getValue().firstKey() );
		A: for ( final Integer firstTileNeighbor : connectedTiles.get( square.get( 0 ) ).keySet() )
		{
			if ( firstTileNeighbor.intValue() == square.get( 1 ).intValue() )
				continue;

			for ( final Integer lastTileNeighbor : connectedTiles.get( square.get( 1 ) ).keySet() )
			{
				if ( lastTileNeighbor.intValue() == square.get( 0 ).intValue() )
					continue;

				if ( connectedTiles.get( firstTileNeighbor ).containsKey( lastTileNeighbor ) )
				{
					square.add( firstTileNeighbor );
					square.add( lastTileNeighbor );
					break A;
				}
			}
		}

		System.out.println( "Found square formed by tiles " + square );
		System.out.println();

		final TreeMap< Integer, Tile< ? > > tiles = new TreeMap<>();
		for ( final Integer index : square )
		{
			tiles.put( index, new Tile<>(
					new InterpolatedAffineModel3D<>(
							new SimilarityModel3D(),
							new TranslationModel3D(),
							REGULARIZER_TRANSLATION
						)
				) );
		}

		for ( int i = 0; i < square.size(); ++i )
		{
			for ( int j = i + 1; j < square.size(); ++j )
			{
				if ( connectedTiles.get( square.get( i ) ).containsKey( square.get( j ) ) )
				{
					final int ind1 = square.get( i ), ind2 = square.get( j );
					final Tile< ? > tile1 = tiles.get( ind1 ), tile2 = tiles.get( ind2 );
					tile1.addConnectedTile( tile2 );
					tile2.addConnectedTile( tile1 );

					final List< PointMatch > matches = new ArrayList<>();
					for ( final SerializablePairWiseStitchingResult shift : connectedTiles.get( ind1 ).get( ind2 ) )
						matches.add( new PointMatch( shift.getPointPair().getA(), shift.getPointPair().getB(), shift.getCrossCorrelation() ) );

					tile1.addMatches( matches );
					tile2.addMatches( PointMatch.flip( matches ) );
				}
			}
		}

		final TileConfiguration tc = new TileConfiguration();
		tc.addTiles( tiles.values() );
		tc.fixTile( tiles.firstEntry().getValue() );
		tc.preAlign();
		tc.optimize( 10, 1000, 1000, 1.f, 1 );

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
