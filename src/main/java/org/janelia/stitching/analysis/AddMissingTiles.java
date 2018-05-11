package org.janelia.stitching.analysis;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileModelFactory;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;

import ij.ImagePlus;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import mpicbg.models.Tile;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;

@Deprecated
public class AddMissingTiles
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		// Read inputs
		final TreeMap< Integer, TileInfo > tilesInfoFinal = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) ) );
		final List< SerializablePairWiseStitchingResult[] > shiftsMulti = TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) );

		final List< TileInfo > tilesInfoAdded = new ArrayList<>();
		final TreeMap< Integer, TileInfo > initialTilesInfo = Utils.createTilesMapMulti( shiftsMulti, false );

		System.out.println( "Tiles fixed: " + tilesInfoFinal.size() );

		int validPairsCount = 0;
		for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMulti )
		{
			boolean isValidOverlap = true;
			for ( final SerializablePairWiseStitchingResult shift: shiftMulti )
				if ( !shift.getIsValidOverlap() )
					isValidOverlap = false;
			if ( isValidOverlap )
				validPairsCount++;
		}
		System.out.println( "Total valid pairs: " + validPairsCount );

		// Find missing tiles
		final TreeMap< Integer, TileInfo > missingTilesInfo = Utils.createTilesMapMulti( shiftsMulti, false );
		final int tilesPerChannel = missingTilesInfo.firstKey();
		missingTilesInfo.keySet().removeAll( tilesInfoFinal.keySet() );

		System.out.println( "Number of missing tiles: " + missingTilesInfo.size() );

		// Find adjacent matches for missing tiles
		final Set< SerializablePairWiseStitchingResult[] > missingAdjacentPairwiseShiftsMulti = new HashSet<>();
		final List< SerializablePairWiseStitchingResult[] > missingPairwiseShiftsMulti = new ArrayList<>();
		final List< TilePair > missingPairs = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMulti )
		{
			for ( final TileInfo tileInfo : shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray() )
			{
				if ( missingTilesInfo.containsKey( tileInfo.getIndex() ) )
				{
					missingPairwiseShiftsMulti.add( shiftMulti );
					missingPairs.add( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair() );
					break;
				}
			}
		}
		final List< TilePair > missingAdjacentPairs = FilterAdjacentShifts.filterAdjacentPairs( missingPairs );
		final TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult[] > > missingPairwiseShiftsMultiMap = Utils.createPairwiseShiftsMultiMap( missingPairwiseShiftsMulti, false );
		for ( final TilePair pair : missingAdjacentPairs )
		{
			final int ind1 = Math.min( pair.getA().getIndex(), pair.getB().getIndex() );
			final int ind2 = Math.max( pair.getA().getIndex(), pair.getB().getIndex() );
			missingAdjacentPairwiseShiftsMulti.add( missingPairwiseShiftsMultiMap.get( ind1 ).get( ind2 ) );
		}
		System.out.println( "Number of missing adjacent pairs: " + missingAdjacentPairwiseShiftsMulti.size() );



		// Create fake tile objects so that they don't hold any image data
		final TreeMap< Integer, Tile< ? > > tiles = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMulti )
		{
			for ( final TileInfo tileInfo : shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray() )
			{
				if ( !tiles.containsKey( tileInfo.getIndex() ) )
				{
					try
					{
						final ImageCollectionElement e = new ImageCollectionElement( new File( tileInfo.getFilePath() ), tileInfo.getIndex() );
						e.setDimensionality( tileInfo.numDimensions() );

						if ( tilesInfoFinal.containsKey( tileInfo.getIndex() ) )
							e.setModel( TileModelFactory.createOffsetModel( tilesInfoFinal.get( tileInfo.getIndex() ) ) );
						else
							e.setModel( TileModelFactory.createDefaultModel( tileInfo.numDimensions() ) );

						final ImagePlus fakeImage = new ImagePlus( tileInfo.getIndex().toString(), (java.awt.Image)null );
						final Tile< ? > tile = new ImagePlusTimePoint( fakeImage, e.getIndex(), 1, e.getModel(), e );
						tiles.put( tileInfo.getIndex(), tile );
					}
					catch ( final Exception e ) {
						e.printStackTrace();
					}
				}
			}
		}

		// Prepare pairwise matches
		for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMulti )
		{
			boolean isValidOverlap = true;
			for ( final SerializablePairWiseStitchingResult shift: shiftMulti )
				if ( !shift.getIsValidOverlap() )
					isValidOverlap = false;
			if ( !isValidOverlap )
				continue;

			// It could be the case that the pairwise shift is valid, but some of the tiles have formed a separate small graph and therefore have been thrown out
			if ( !tilesInfoFinal.containsKey( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().getIndex() ) || !tilesInfoFinal.containsKey( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getB().getIndex() ) )
				continue;

			final Tile[] tilePair = new Tile[ 2 ];
			for ( int j = 0; j < 2; j++ )
				tilePair[ j ] = tiles.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray()[ j ].getIndex() );

			final Point[] points = new Point[ 2 ];
			points[ 0 ] = new Point( new double[ shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().numDimensions() ] );
			final double[] offset = new double[ shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().numDimensions() ];
			for ( int d = 0; d < offset.length; d++ )
				offset[ d ] = tilesInfoFinal.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().getIndex() ).getPosition( d ) - tilesInfoFinal.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getB().getIndex() ).getPosition( d );
			points[ 1 ] = new Point( offset );

			for ( int j = 0; j < 2; j++ )
			{
				tilePair[ j ].addMatch( new PointMatch( points[ j ], points[ ( j + 1 ) % 2 ] ) );
				tilePair[ j ].addConnectedTile( tilePair[ ( j + 1 ) % 2 ] );
			}
		}


		// Start adding missing tiles one by one
		while ( !missingTilesInfo.isEmpty() )
		{
			// For each missing tile, construct a set of matches with the fixed tiles set
			final TreeMap< Integer, List< SerializablePairWiseStitchingResult[] > > possibleMatches = new TreeMap<>();
			for ( final Integer key : missingTilesInfo.keySet() )
				possibleMatches.put( key, new ArrayList<>() );
			for ( final SerializablePairWiseStitchingResult[] shiftMulti : missingAdjacentPairwiseShiftsMulti )
				for ( int j = 0; j < 2; j++ )
					if ( tilesInfoFinal.containsKey( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray()[ j ].getIndex() ) )
						possibleMatches.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray()[ ( j + 1 ) % 2 ].getIndex() ).add( shiftMulti );

			// Choose the missing tile which has the largest number of matches with the fixed tiles set
			int maxMatchesTileIndex = -1;
			/*for ( final Entry< Integer, List< SerializablePairWiseStitchingResult[] > > entry : possibleMatches.entrySet() )
				if ( tileInfoToAdd == null || entry.getValue().size() > possibleMatches.get( tileInfoToAdd.getIndex() ).size() )
					tileInfoToAdd = missingTilesInfo.get( entry.getKey() );*/
			for ( final Entry< Integer, List< SerializablePairWiseStitchingResult[] > > entry : possibleMatches.entrySet() )
				if ( maxMatchesTileIndex == -1 || entry.getValue().size() > possibleMatches.get( maxMatchesTileIndex ).size() )
					maxMatchesTileIndex = entry.getKey();


			if ( possibleMatches.get( maxMatchesTileIndex ).size() == 1 )
			{
				// try to find a pair of overlapping tiles to add them simultaneously (so that we would have 3 matches)
				SerializablePairWiseStitchingResult[] shiftMultiPairToAdd = null;
				for ( final SerializablePairWiseStitchingResult[] shiftMulti : missingAdjacentPairwiseShiftsMulti )
				{
					boolean found = true;
					for ( final TileInfo key : shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray() )
						if ( !missingTilesInfo.containsKey( key.getIndex() ) || possibleMatches.get( key.getIndex() ).isEmpty() )
							found = false;

					if ( found )
					{
						shiftMultiPairToAdd = shiftMulti;
						break;
					}
				}

				if ( shiftMultiPairToAdd != null )
				{
					System.out.println( "Found a pair of tiles that forms a 3 matches set" );

					final TileInfo[] tilePairInfoToAdd = shiftMultiPairToAdd[ 0 ].getTileBoxPair().getOriginalTilePair().toArray();

					// Add new tile to the configuration set
					final Tile< ? >[] tilePairToAdd = new Tile[ 2 ];
					final List< SerializablePairWiseStitchingResult[] > shiftsMultiPairToAdd = new ArrayList<>();

					for ( int j = 0; j < 2; j++ )
					{
						final int key = tilePairInfoToAdd[ j ].getIndex();
						tilePairToAdd[ j ] = tiles.get( key );
						shiftsMultiPairToAdd.addAll( possibleMatches.get( key ) );
					}
					shiftsMultiPairToAdd.add( shiftMultiPairToAdd );
					final int matches = shiftsMultiPairToAdd.size();

					if ( matches != 3 )
						throw new Exception( "impossible" );

					System.out.println( "Tiles to add: (" + tilePairInfoToAdd[0].getIndex()+"," +tilePairInfoToAdd[1].getIndex()+")" );

					final int[] matchesConfiguration = new int[ matches ];
					int[] bestMatchesConfiguration = null;
					double lowestAvgMaxDisplacement = Double.MAX_VALUE;
					double lowestAvgMeanDisplacement = Double.MAX_VALUE;
					final double[][] bestPositions = new double[ 2 ][ tilePairInfoToAdd[ 0 ].numDimensions() ];

					int peaks = -1;
					for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMultiPairToAdd )
						if ( peaks == -1 )
							peaks = shiftMulti.length;
						else if ( peaks != shiftMulti.length )
							throw new Exception( "Peaks count mismatch: expected="+peaks+", actual="+shiftMulti.length );
					if ( peaks == 1 )
						throw new Exception( "There is a single peak, expected a collection of peaks" );

					// Try all combinations
					final int totalConfigurationsCount = ( int ) Math.pow( peaks, matches );

					for ( int config = 0; config < totalConfigurationsCount; config++ )
					{
						int x = config;
						for ( int i = matches - 1; i >= 0; i-- )
						{
							matchesConfiguration[ i ] = x % peaks;
							x /= peaks;
						}

						// Add matches according to the current configuration
						final PointMatch tilePairMatch = null;
						for ( int i = 0; i < matches; i++ )
						{
							// Take desired shift according to the current configuration
							final SerializablePairWiseStitchingResult[] shiftMulti = shiftsMultiPairToAdd.get( i );
							final SerializablePairWiseStitchingResult shift = shiftMulti[ matchesConfiguration[ i ] ];

							final Tile[] tilePair = new Tile[ 2 ];
							for ( int j = 0; j < 2; j++ )
								tilePair[ j ] = tiles.get( shift.getTileBoxPair().getOriginalTilePair().toArray()[ j ].getIndex() );

							final Point[] points = new Point[ 2 ];
							points[ 0 ] = new Point( new double[ shift.getTileBoxPair().getOriginalTilePair().getA().numDimensions() ] );
							final double[] offset = new double[ shift.getTileBoxPair().getOriginalTilePair().getA().numDimensions() ];
							for ( int d = 0; d < offset.length; d++ )
								offset[ d ] = -shift.getOffset( d );
							points[ 1 ] = new Point( offset );

							for ( int j = 0; j < 2; j++ )
							{
								final PointMatch match = new PointMatch( points[ j ], points[ ( j + 1 ) % 2 ], shift.getCrossCorrelation() );
								tilePair[ j ].addMatch( match );
								tilePair[ j ].addConnectedTile( tilePair[ ( j + 1 ) % 2 ] );

								/*if ( shiftMulti == shiftMultiPairToAdd && j == 0 )
								{
									if ( tilePairMatch != null )
										throw new Exception( "Impossible" );
									tilePairMatch = match;
								}*/
							}
						}
						/*if ( tilePairMatch == null )
							throw new Exception( "Impossible" );*/

						// Transform all relative coordinates into global coordinates
						for ( final Tile< ? > tileToAdd : tilePairToAdd )
							for ( final Tile< ? > connectedTile : tileToAdd.getConnectedTiles() )
								connectedTile.apply();

						// Optimize the configuration
						for ( final Tile< ? > tileToAdd : tilePairToAdd )
						{
							tileToAdd.fitModel();
							tileToAdd.apply();
						}

						double avgMeanDisplacement = 0, avgMaxDisplacement = 0;
						for ( final Tile< ? > tileToAdd : tilePairToAdd )
						{
							avgMeanDisplacement += PointMatch.meanDistance( tileToAdd.getMatches() );
							//avgMaxDisplacement += PointMatch.maxDistance( tileToAdd.getMatches() );
							avgMaxDisplacement = Math.max( PointMatch.maxDistance( tileToAdd.getMatches() ), avgMaxDisplacement );
						}
						avgMeanDisplacement /= 2;
						//avgMaxDisplacement /= 2;

						if ( lowestAvgMeanDisplacement > avgMeanDisplacement )
							lowestAvgMeanDisplacement = avgMeanDisplacement;

						if ( lowestAvgMaxDisplacement > avgMaxDisplacement )
						{
							lowestAvgMaxDisplacement = avgMaxDisplacement;
							bestMatchesConfiguration = matchesConfiguration.clone();

							for ( int j = 0; j < 2; j++ )
							{
								Arrays.fill( bestPositions[ j ], 0.0 );
								tilePairToAdd[ j ].getModel().applyInPlace( bestPositions[ j ] );
							}
						}

						for ( int j = 0; j < 2; j++ )
						{
							final TileInfo tileInfoToAdd = tilePairInfoToAdd[ j ];
							final Tile< ? > tileToAdd = tilePairToAdd[ j ];

							final Set<Tile<?>> oldConnectedTiles = new HashSet<>( tileToAdd.getConnectedTiles() );

							// Reset matches for this tile
							for ( final Tile< ? > oldConnectedTile : tileToAdd.getConnectedTiles() )
								oldConnectedTile.removeConnectedTile( tileToAdd );
							tileToAdd.getMatches().clear();
							tileToAdd.getConnectedTiles().clear();

							// Reset the model for this tile
							tileToAdd.getModel().set( TileModelFactory.createOffsetModel( tileInfoToAdd ) );
						}
					}

					System.out.println( "avg for two tiles: " + lowestAvgMeanDisplacement + " " + lowestAvgMaxDisplacement );

					// Finally fix this pair of tiles
					if ( lowestAvgMaxDisplacement <= 12.0 )
					{
						System.out.println( "+" );

						for ( int j = 0; j < 2; j++ )
						{
							final TileInfo tileInfoAdded = tilePairInfoToAdd[ j ].clone();
							final Tile< ? > tileToAdd = tilePairToAdd[ j ];

							tileInfoAdded.setPosition( bestPositions[ j ].clone() );
							tilesInfoFinal.put( tileInfoAdded.getIndex(), tileInfoAdded );
							tilesInfoAdded.add( tileInfoAdded );

							tileToAdd.getModel().set( TileModelFactory.createOffsetModel( tileInfoAdded ) );
						}

						for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMultiPairToAdd )
						{
							final Tile[] tilePair = new Tile[ 2 ];
							for ( int j = 0; j < 2; j++ )
								tilePair[ j ] = tiles.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray()[ j ].getIndex() );

							final Point[] points = new Point[ 2 ];
							points[ 0 ] = new Point( new double[ shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().numDimensions() ] );
							final double[] offset = new double[ shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().numDimensions() ];
							for ( int d = 0; d < offset.length; d++ )
								offset[ d ] = tilesInfoFinal.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().getIndex() ).getPosition( d ) - tilesInfoFinal.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getB().getIndex() ).getPosition( d );
							points[ 1 ] = new Point( offset );

							for ( int j = 0; j < 2; j++ )
							{
								tilePair[ j ].addMatch( new PointMatch( points[ j ], points[ ( j + 1 ) % 2 ] ) );
								tilePair[ j ].addConnectedTile( tilePair[ ( j + 1 ) % 2 ] );
							}
						}
					}
					else
					{
						final Set< SerializablePairWiseStitchingResult[] > shiftsMultiToRemove = new HashSet<>();
						for ( final TileInfo tileInfoToAdd : tilePairInfoToAdd )
							for ( final SerializablePairWiseStitchingResult[] shiftMulti : missingAdjacentPairwiseShiftsMulti )
								for ( int j = 0; j < 2; j++ )
									if ( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray()[ j ].getIndex().intValue() == tileInfoToAdd.getIndex().intValue() )
										shiftsMultiToRemove.add( shiftMulti );
						missingAdjacentPairwiseShiftsMulti.removeAll( shiftsMultiToRemove );
					}

					for ( final TileInfo tileInfoToAdd : tilePairInfoToAdd )
						missingTilesInfo.remove( tileInfoToAdd.getIndex() );

					missingAdjacentPairwiseShiftsMulti.removeAll( shiftsMultiPairToAdd );

					continue;
				}
				else
				{
					System.out.println( "No pair that forms a 3 matches set :(" );
				}
			}


			// Add new tile to the configuration set
			final TileInfo tileInfoToAdd = missingTilesInfo.get( maxMatchesTileIndex );
			final Tile< ? > tileToAdd = tiles.get( tileInfoToAdd.getIndex() );

			final List< SerializablePairWiseStitchingResult[] > shiftsMultiToAdd = possibleMatches.get( tileInfoToAdd.getIndex() );
			final int matches = shiftsMultiToAdd.size();

			//System.out.println( "Tile to add: " + tileInfoToAdd.getIndex() + " (has " + matches + " matches)" );



			// Add matches one by one and optimize the configuration, preserve a configuration with the smallest displacement
			final int[] matchesConfiguration = new int[ matches ];
			int[] bestMatchesConfiguration = null;
			double lowestMaxDisplacement = Double.MAX_VALUE;
			double lowestMeanDisplacement = Double.MAX_VALUE;
			final double[] bestPosition = new double[ tileInfoToAdd.numDimensions() ];


			if ( matches > 1 )
			{
				int peaks = -1;
				for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMultiToAdd )
					if ( peaks == -1 )
						peaks = shiftMulti.length;
					else if ( peaks != shiftMulti.length )
						throw new Exception( "Peaks count mismatch: expected="+peaks+", actual="+shiftMulti.length );
				if ( peaks == 1 )
					throw new Exception( "There is a single peak, expected a collection of peaks" );

				// Try all combinations
				final int totalConfigurationsCount = ( int ) Math.pow( peaks, matches );

				for ( int config = 0; config < totalConfigurationsCount; config++ )
				{
					int x = config;
					for ( int i = matches - 1; i >= 0; i-- )
					{
						matchesConfiguration[ i ] = x % peaks;
						x /= peaks;
					}

					// Add matches according to the current configuration
					for ( int i = 0; i < matches; i++ )
					{
						// Take desired shift according to the current configuration
						final SerializablePairWiseStitchingResult[] shiftMulti = shiftsMultiToAdd.get( i );
						final SerializablePairWiseStitchingResult shift = shiftMulti[ matchesConfiguration[ i ] ];

						final Tile[] tilePair = new Tile[ 2 ];
						for ( int j = 0; j < 2; j++ )
							tilePair[ j ] = tiles.get( shift.getTileBoxPair().getOriginalTilePair().toArray()[ j ].getIndex() );

						final Point[] points = new Point[ 2 ];
						points[ 0 ] = new Point( new double[ shift.getTileBoxPair().getOriginalTilePair().getA().numDimensions() ] );
						final double[] offset = new double[ shift.getTileBoxPair().getOriginalTilePair().getA().numDimensions() ];
						for ( int d = 0; d < offset.length; d++ )
							offset[ d ] = -shift.getOffset( d );
						points[ 1 ] = new Point( offset );

						for ( int j = 0; j < 2; j++ )
						{
							tilePair[ j ].addMatch( new PointMatch( points[ j ], points[ ( j + 1 ) % 2 ], shift.getCrossCorrelation() ) );
							tilePair[ j ].addConnectedTile( tilePair[ ( j + 1 ) % 2 ] );
						}
					}

					// Transform all relative coordinates into global coordinates
					for ( final Tile< ? > connectedTile : tileToAdd.getConnectedTiles() )
						connectedTile.apply();
					tileToAdd.apply();

					// Optimize the configuration
					tileToAdd.fitModel();
					tileToAdd.apply();

					//tileToAdd.updateCost();
					//final double displacement = tileToAdd.getDistance();
					final double meanDisplacement = PointMatch.meanDistance( tileToAdd.getMatches() );
					if ( lowestMeanDisplacement > meanDisplacement )
						lowestMeanDisplacement = meanDisplacement;
					final double maxDisplacement = PointMatch.maxDistance( tileToAdd.getMatches() );
					if ( lowestMaxDisplacement > maxDisplacement )
					{
						lowestMaxDisplacement = maxDisplacement;
						bestMatchesConfiguration = matchesConfiguration.clone();

						Arrays.fill( bestPosition, 0.0 );
						tileToAdd.getModel().applyInPlace( bestPosition );
					}

					final Set<Tile<?>> oldConnectedTiles = new HashSet<>( tileToAdd.getConnectedTiles() );

					// Reset matches for this tile
					for ( final Tile< ? > oldConnectedTile : tileToAdd.getConnectedTiles() )
						oldConnectedTile.removeConnectedTile( tileToAdd );
					tileToAdd.getMatches().clear();
					tileToAdd.getConnectedTiles().clear();

					// Reset the model for this tile
					tileToAdd.getModel().set( TileModelFactory.createOffsetModel( tileInfoToAdd ) );
				}

				System.out.println( lowestMeanDisplacement + " " + lowestMaxDisplacement );
			}
			else
			{
				System.out.println( "Skipping a tile with " + matches + " matches" );
			}

			//final int tileIndexOut = tileInfoToAdd.getIndex() - tilesPerChannel;
			//System.out.println( tileIndexOut + ":" + (tileIndexOut<1000?" ":"")+(tileIndexOut<100?" ":"")+(tileIndexOut<10?" ":"") + " Error: " + lowestDisplacement );
			//System.out.println( "Config: " + Arrays.toString( bestMatchesConfiguration ) );
			//final double[] correlations = new double[ matches ];
			//for ( int i = 0; i < matches; i++ )
			//	correlations[ i ] = shiftsMultiToAdd.get( i )[ bestMatchesConfiguration[ i ] ].getCrossCorrelation();
			//System.out.println( "CrCorr: " + Arrays.toString( correlations ) );
			//System.out.println( "Position before="+Arrays.toString( tileInfoToAdd.getPosition() ) + ",  after=" + Arrays.toString( bestPosition ) );


			// Finally fix the tile
			if ( lowestMaxDisplacement <= 5.0 )
			{
				final TileInfo tileInfoAdded = tileInfoToAdd.clone();
				tileInfoAdded.setPosition( bestPosition.clone() );
				tilesInfoFinal.put( tileInfoAdded.getIndex(), tileInfoAdded );
				tilesInfoAdded.add( tileInfoAdded );

				tileToAdd.getModel().set( TileModelFactory.createOffsetModel( tileInfoAdded ) );

				for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMultiToAdd )
				{
					final Tile[] tilePair = new Tile[ 2 ];
					for ( int j = 0; j < 2; j++ )
						tilePair[ j ] = tiles.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray()[ j ].getIndex() );

					final Point[] points = new Point[ 2 ];
					points[ 0 ] = new Point( new double[ shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().numDimensions() ] );
					final double[] offset = new double[ shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().numDimensions() ];
					for ( int d = 0; d < offset.length; d++ )
						offset[ d ] = tilesInfoFinal.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().getIndex() ).getPosition( d ) - tilesInfoFinal.get( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getB().getIndex() ).getPosition( d );
					points[ 1 ] = new Point( offset );

					for ( int j = 0; j < 2; j++ )
					{
						tilePair[ j ].addMatch( new PointMatch( points[ j ], points[ ( j + 1 ) % 2 ] ) );
						tilePair[ j ].addConnectedTile( tilePair[ ( j + 1 ) % 2 ] );
					}
				}
			}
			else
			{
				final Set< SerializablePairWiseStitchingResult[] > shiftsMultiToRemove = new HashSet<>();
				for ( final SerializablePairWiseStitchingResult[] shiftMulti : missingAdjacentPairwiseShiftsMulti )
					for ( int j = 0; j < 2; j++ )
						if ( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().toArray()[ j ].getIndex().intValue() == tileInfoToAdd.getIndex().intValue() )
							shiftsMultiToRemove.add( shiftMulti );
				missingAdjacentPairwiseShiftsMulti.removeAll( shiftsMultiToRemove );
			}
			missingTilesInfo.remove( tileInfoToAdd.getIndex() );
			missingAdjacentPairwiseShiftsMulti.removeAll( shiftsMultiToAdd );
		}


		System.out.println( "--------------------------------------------------" );
		System.out.println( "Added " + tilesInfoAdded.size() + " tiles" );
		for ( final TileInfo t : tilesInfoAdded )
		{
			/*final double[] offset = new double[3];
			for ( int i = 0; i <3; i++)
				offset[i] = t.getPosition( i )-initialTilesInfo.get( t.getIndex() ).getPosition( i );
			System.out.println( Arrays.toString( offset ) );*/

			//System.out.println( t.getPosition(0)+" "+t.getPosition(1)+" "+t.getPosition(2) );
		}

		TileInfoJSONProvider.saveTilesConfiguration( tilesInfoFinal.values().toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 0 ], "_all" ) ) ) );


		System.out.println( "Done" );
	}
}