package org.janelia.stitching.analysis;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileModelFactory;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;
import org.janelia.util.Conversions;

import ij.ImagePlus;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import mpicbg.models.Tile;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
import mpicbg.stitching.TileConfigurationStitching;

public class AddMissingTilesMultichannel
{
	public static void main( final String[] args ) throws Exception
	{
		// Read inputs
		final TreeMap< Integer, TileInfo >[] tilesFinal = new TreeMap[ 2 ];
		final List< SerializablePairWiseStitchingResult >[] shifts = new List[ 2 ];
		for ( int ch = 0; ch < 2; ch++ )
			tilesFinal[ ch ] = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( args[ ch ] ) );
		for ( int ch = 0; ch < 2; ch++ )
			shifts[ ch ] = TileInfoJSONProvider.loadPairwiseShifts( args[ tilesFinal.length + ch ] );

		// Find "tile indices offset" between corresponding tiles across channels
		final int tilesPerChannel = Utils.createTilesMap( shifts[ 0 ], false ).size();

		// Find missing tiles
		final TreeMap< Integer, TileInfo >[] missingTiles = new TreeMap[ 2 ];
		for ( int ch = 0; ch < 2; ch++ )
		{
			missingTiles[ ch ] = Utils.createTilesMap( shifts[ ch ], false );
			missingTiles[ ch ].keySet().removeAll( tilesFinal[ ch ].keySet() );

			System.out.println( "Missing tiles for ch"+ch+": " + missingTiles[ch].size() );
		}

		// Find adjacent matches for missing tiles
		//final TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult > >[] missingAdjacentPairwiseShifts = new TreeMap[ 2 ];
		final List< SerializablePairWiseStitchingResult >[] missingAdjacentPairwiseShifts = new List[ 2 ];
		for ( int ch = 0; ch < 2; ch++ )
		{
			final List< SerializablePairWiseStitchingResult > missingPairwiseShifts = new ArrayList<>();
			final List< TilePair > missingPairs = new ArrayList<>();
			for ( final SerializablePairWiseStitchingResult shift : shifts[ ch ] )
			{
				for ( final TileInfo tile : shift.getTilePair().toArray() )
				{
					if ( missingTiles[ ch ].containsKey( tile.getIndex() ) )
					{
						missingPairwiseShifts.add( shift );
						missingPairs.add( shift.getTilePair() );
						break;
					}
				}
			}
			final List< TilePair > missingAdjacentPairs = FilterAdjacentShifts.filterAdjacentPairs( missingPairs );
			final TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult > > missingPairwiseShiftsMap = Utils.createPairwiseShiftsMap( missingPairwiseShifts, false );
			missingAdjacentPairwiseShifts[ ch ] = new ArrayList<>();
			for ( final TilePair pair : missingAdjacentPairs )
			{
				final int ind1 = Math.min( pair.getA().getIndex(), pair.getB().getIndex() );
				final int ind2 = Math.max( pair.getA().getIndex(), pair.getB().getIndex() );
				missingAdjacentPairwiseShifts[ ch ].add( missingPairwiseShiftsMap.get( ind1 ).get( ind2 ) );
			}
			System.out.println( "Missing pairs for ch"+ch+": " + missingPairs.size() );
			System.out.println( "Missing adjacent pairs for ch"+ch+": " + missingAdjacentPairwiseShifts[ch].size() );
			//missingAdjacentPairwiseShifts[ ch ] = Utils.createPairwiseShiftsMap( missingAdjacentPairwiseShiftsList, false );
		}

		// Create fake tile objects so that they don't hold any image data
		final TreeMap< Integer, Tile< ? > > fakeTileImages = new TreeMap<>();
		for ( int ch = 0; ch < 2; ch++ )
		{
			for ( final SerializablePairWiseStitchingResult shift : shifts[ ch ] )
			{
				for ( final TileInfo tile : shift.getTilePair().toArray() )
				{
					if ( !fakeTileImages.containsKey( tile.getIndex() ) )
					{
						try
						{
							final ImageCollectionElement e = new ImageCollectionElement( new File( tile.getFilePath() ), tile.getIndex() );
							e.setOffset( Conversions.toFloatArray( tile.getPosition() ) );
							e.setDimensionality( tile.numDimensions() );
							e.setModel( TileModelFactory.createDefaultModel( tile.numDimensions() ) );
							final ImagePlus fakeImage = new ImagePlus( tile.getIndex().toString(), (java.awt.Image)null );
							final Tile< ? > fakeTileImage = new ImagePlusTimePoint( fakeImage, e.getIndex(), 1, e.getModel(), e );
							fakeTileImages.put( tile.getIndex(), fakeTileImage );
						}
						catch ( final Exception e ) {
							e.printStackTrace();
						}
					}
				}
			}
		}

		// Prepare pairwise matches within channels
		final ArrayList< Tile< ? > >[] fixedTilesList = new ArrayList[ 2 ];
		for ( int ch = 0; ch < 2; ch++ )
		{
			final Set< Tile< ? > > fixedTilesSet = new HashSet<>();
			fixedTilesList[ ch ] = new ArrayList<>();

			for ( final SerializablePairWiseStitchingResult shift : shifts[ ch ] )
			{
				if ( !shift.getIsValidOverlap() ||
						( !tilesFinal[ ch ].containsKey( shift.getTilePair().getA().getIndex() ) || !tilesFinal[ ch ].containsKey( shift.getTilePair().getB().getIndex() ) ) )
					continue;

				final Tile[] tilePair = new Tile[ 2 ];
				for ( int j = 0; j < 2; j++ )
					tilePair[ j ] = fakeTileImages.get( shift.getTilePair().toArray()[ j ].getIndex() );

				final Point[] points = new Point[ 2 ];
				points[ 0 ] = new Point( new double[ shift.getTilePair().getA().numDimensions() ] );
				final double[] offset = new double[ shift.getTilePair().getA().numDimensions() ];
				for ( int d = 0; d < offset.length; d++ )
					offset[ d ] = tilesFinal[ ch ].get( shift.getTilePair().getB().getIndex() ).getPosition( d ) - tilesFinal[ ch ].get( shift.getTilePair().getA().getIndex() ).getPosition( d );
				points[ 1 ] = new Point( offset );

				for ( int j = 0; j < 2; j++ )
				{
					tilePair[ j ].addMatch( new PointMatch( points[ j ], points[ ( j + 1 ) % 2 ] ) );
					tilePair[ j ].addConnectedTile( tilePair[ ( j + 1 ) % 2 ] );

					if ( !fixedTilesSet.contains( tilePair[ j ] ) )
					{
						fixedTilesSet.add( tilePair[ j ] );
						fixedTilesList[ ch ].add( tilePair[ j ] );
					}
				}
			}
		}
		if ( tilesFinal[ 0 ].size() != fixedTilesList[ 0 ].size() || tilesFinal[ 1 ].size() != fixedTilesList[ 1 ].size() )
			throw new Exception( "Fixed tiles count mismatch" );




		// Create tile configuration with final tiles being fixed
		final TileConfigurationStitching tc = new TileConfigurationStitching();
		for ( int ch = 0; ch < 2; ch++ )
		{
			tc.addTiles( fixedTilesList[ ch ] );
			for ( final Tile tile : fixedTilesList[ ch ] )
				tc.fixTile( tile );
		}

		// Trace connected graphs
		final ArrayList< Set< Tile< ? > > > graphs = Tile.identifyConnectedGraphs( tc.getTiles() );
		final ArrayList< Integer > graphsSize = new ArrayList<>();
		int graphSizeSum = 0;
		for ( final Set< Tile< ? > > graph : graphs )
		{
			graphsSize.add( graph.size() );
			graphSizeSum += graph.size();
		}
		Collections.sort( graphsSize );
		Collections.reverse( graphsSize );
		System.out.println( "Tiles total = " + tc.getTiles().size() + ",   graphs=" + graphs.size() + ",   sum="+graphSizeSum );
		System.out.println( graphsSize );






		System.out.println( "Done" );
	}
}
