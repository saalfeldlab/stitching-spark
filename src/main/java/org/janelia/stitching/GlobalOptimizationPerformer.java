package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import mpicbg.models.Tile;
import mpicbg.models.TranslationModel2D;
import mpicbg.models.TranslationModel3D;
import mpicbg.stitching.ComparePair;
import mpicbg.stitching.ImagePlusTimePoint;
import mpicbg.stitching.PointMatchStitching;
import mpicbg.stitching.StitchingParameters;
import mpicbg.stitching.TileConfigurationStitching;
import stitching.utils.Log;


// based on the GlobalOptimization class from the original Fiji's Stitching plugin repository
public class GlobalOptimizationPerformer
{
	public interface TileConfigurationObserver
	{
		public void configurationUpdated( final ArrayList< ImagePlusTimePoint > tiles );
	}

	public static boolean ignoreZ = false;

	public static Set< Tile<?> > lostTiles = null;

	private static java.io.PrintStream originalOut, suppressedOut;

	public static ArrayList< ImagePlusTimePoint > optimize(
			final Vector< ComparePair > pairs,
			final ImagePlusTimePoint fixedImage,
			final StitchingParameters params )
	{
		return optimize(
				pairs,
				fixedImage,
				params,
				null,
				null );
	}
	public static ArrayList< ImagePlusTimePoint > optimize(
			final Vector< ComparePair > pairs,
			final ImagePlusTimePoint fixedImage,
			final StitchingParameters params,
			final TileConfigurationObserver observer )
	{
		return optimize(
				pairs,
				fixedImage,
				params,
				observer,
				null );
	}

	public static ArrayList< ImagePlusTimePoint > optimize(
			final Vector< ComparePair > pairs,
			final ImagePlusTimePoint fixedImage,
			final StitchingParameters params,
			final TileConfigurationObserver observer,
			final Map< Integer, Map< Integer, ComparePair > > anotherChannel )
	{
		/*final Set< Integer > tilesPerChannel = new HashSet< Integer >();
		for ( final ComparePair pair : pairs )
			for ( final int index : new int[] { pair.getTile1().getImpId(), pair.getTile2().getImpId() } )
				tilesPerChannel.add( index );
		System.out.println( "GlobalOptimization: Tiles per channel = " + tilesPerChannel.size() );*/


		// cache full pairs set (valid+invalid)
		final Vector< ComparePair > fullPairsSet = new Vector< >();
		fullPairsSet.addAll( pairs );
		if ( anotherChannel != null )
			for ( final Map< Integer, ComparePair > entryVal : anotherChannel.values() )
				for ( final ComparePair val : entryVal.values() )
					fullPairsSet.addElement( val );

		// cache the first channel
		final Vector< ComparePair > firstChannel = new Vector< >();
		for ( final ComparePair pair : pairs )
			if ( pair.getIsValidOverlap() )
				firstChannel.add( pair );

		pairs.clear();
		pairs.addAll( firstChannel );

		// Add another channel pairs to the main set
		if ( anotherChannel != null )
			for ( final Map< Integer, ComparePair > entryVal : anotherChannel.values() )
				for ( final ComparePair val : entryVal.values() )
					if ( val.getIsValidOverlap() )
						pairs.addElement( val );

		// create a set of bad identity connections
		final Set< Integer > badIdentityConnections = new HashSet< >();


		Map< Integer, Tile< ? > > firstChannelTilesSet = null, firstChannelTilesSetPrev = null;
		Map< Integer, Tile< ? > > anotherChannelTilesSet = null, anotherChannelTilesSetPrev = null;


		System.out.println( "First channel pairs count = " + firstChannel.size() );
		System.out.println( "Total pairs count = " + pairs.size() );



		originalOut = System.out;
		suppressedOut = new java.io.PrintStream(new java.io.OutputStream() {
			@Override public void write(final int b) {}
		}) {
			@Override public void flush() {}
			@Override public void close() {}
			@Override public void write(final int b) {}
			@Override public void write(final byte[] b) {}
			@Override public void write(final byte[] buf, final int off, final int len) {}
			@Override public void print(final boolean b) {}
			@Override public void print(final char c) {}
			@Override public void print(final int i) {}
			@Override public void print(final long l) {}
			@Override public void print(final float f) {}
			@Override public void print(final double d) {}
			@Override public void print(final char[] s) {}
			@Override public void print(final String s) {}
			@Override public void print(final Object obj) {}
			@Override public void println() {}
			@Override public void println(final boolean x) {}
			@Override public void println(final char x) {}
			@Override public void println(final int x) {}
			@Override public void println(final long x) {}
			@Override public void println(final float x) {}
			@Override public void println(final double x) {}
			@Override public void println(final char[] x) {}
			@Override public void println(final String x) {}
			@Override public void println(final Object x) {}
			@Override public java.io.PrintStream printf(final String format, final Object... args) { return this; }
			@Override public java.io.PrintStream printf(final java.util.Locale l, final String format, final Object... args) { return this; }
			@Override public java.io.PrintStream format(final String format, final Object... args) { return this; }
			@Override public java.io.PrintStream format(final java.util.Locale l, final String format, final Object... args) { return this; }
			@Override public java.io.PrintStream append(final CharSequence csq) { return this; }
			@Override public java.io.PrintStream append(final CharSequence csq, final int start, final int end) { return this; }
			@Override public java.io.PrintStream append(final char c) { return this; }
		};


		boolean redo;
		TileConfigurationStitching tc;
		int itersCount = 0;
		do
		{
			redo = false;
			//final ArrayList< Tile< ? > > tiles = new ArrayList< Tile< ? > >();
			final Set< Tile< ? > > tilesSet = new HashSet< >();

			//final int good=0,bad=0;
			//final float smallestGoodWeight=1;

			int pairsAdded = 0;

			for ( final ComparePair pair : pairs )
			{
				if ( pair.getCrossCorrelation() >= params.regThreshold && pair.getIsValidOverlap() )
				{

					pairsAdded++;



					/*if (pair.getCrossCorrelation() >= Float.MAX_VALUE)
					{
						pair.setIsValidOverlap( false );
						continue;
					}*/





					final Tile t1 = pair.getTile1();
					final Tile t2 = pair.getTile2();

					Point p1, p2;

					if ( params.dimensionality == 3 )
					{
						// the transformations that map each tile into the relative global coordinate system (that's why the "-")
						p1 = new Point( new double[]{ 0,0,0 } );

						if ( ignoreZ )
							p2 = new Point( new double[]{ -pair.getRelativeShift()[ 0 ], -pair.getRelativeShift()[ 1 ], 0 } );
						else
							p2 = new Point( new double[]{ -pair.getRelativeShift()[ 0 ], -pair.getRelativeShift()[ 1 ], -pair.getRelativeShift()[ 2 ] } );
					}
					else
					{
						p1 = new Point( new double[]{ 0, 0 } );
						p2 = new Point( new double[]{ -pair.getRelativeShift()[ 0 ], -pair.getRelativeShift()[ 1 ] } );
					}

					final float weight = pair.getCrossCorrelation();
					/*final float weight = ( pair.getCrossCorrelation() < Float.MAX_VALUE - 2 ? pair.getCrossCorrelation() : ( float ) 0.1 );
					if (weight >= 0.15)
					{
						good++;
						smallestGoodWeight = Math.min( weight, smallestGoodWeight );
					}
					else {
						bad++;
					}*/

					t1.addMatch( new PointMatchStitching( p1, p2, weight, pair ) );
					t2.addMatch( new PointMatchStitching( p2, p1, weight, pair ) );





					/*final int ind1 = Math.min( ((ImagePlusTimePoint)t1).getImpId(), ((ImagePlusTimePoint)t2).getImpId() );
					final int ind2 = Math.max( ((ImagePlusTimePoint)t1).getImpId(), ((ImagePlusTimePoint)t2).getImpId() );
					if ( anotherChannel != null && anotherChannel.containsKey( ind1 ) && anotherChannel.get( ind1 ).containsKey( ind2 ) )
					{
						additionalMatches++;
						final ComparePair anotherChannelPair = anotherChannel.get( ind1 ).get( ind2 );
						if ( params.dimensionality == 3 )
						{
							// the transformations that map each tile into the relative global coordinate system (that's why the "-")
							p1 = new Point( new double[]{ 0,0,0 } );

							if ( ignoreZ )
								p2 = new Point( new double[]{ -anotherChannelPair.getRelativeShift()[ 0 ], -anotherChannelPair.getRelativeShift()[ 1 ], 0 } );
							else
								p2 = new Point( new double[]{ -anotherChannelPair.getRelativeShift()[ 0 ], -anotherChannelPair.getRelativeShift()[ 1 ], -anotherChannelPair.getRelativeShift()[ 2 ] } );
						}
						else
						{
							p1 = new Point( new double[]{ 0, 0 } );
							p2 = new Point( new double[]{ -anotherChannelPair.getRelativeShift()[ 0 ], -anotherChannelPair.getRelativeShift()[ 1 ] } );
						}

						t1.addMatch( new PointMatchStitching( p1, p2, anotherChannelPair.getCrossCorrelation(), pair ) );
						t2.addMatch( new PointMatchStitching( p2, p1, anotherChannelPair.getCrossCorrelation(), pair ) );
					}
					else
					{
						System.out.println( "Where is another channel???" );
					}*/







					t1.addConnectedTile( t2 );
					t2.addConnectedTile( t1 );

					/*if (!tilesSet.contains(t1))
					{
						tilesSet.add( t1 );
						tiles.add( t1 );
					}

					if (!tilesSet.contains(t2))
					{
						tilesSet.add( t2 );
						tiles.add( t2 );
					}*/

					tilesSet.add( t1 );
					tilesSet.add( t2 );

					pair.setIsValidOverlap( true );
				}
				else
				{
					pair.setIsValidOverlap( false );
				}
			}


			System.out.println( "Pairs above the threshold: " + pairsAdded );


			firstChannelTilesSet = new TreeMap< >();
			// construct first channel tiles set
			for ( final ComparePair pair : firstChannel )
				if ( pair.getIsValidOverlap() )
					for ( final ImagePlusTimePoint img : new ImagePlusTimePoint[] { pair.getTile1(), pair.getTile2() } )
						firstChannelTilesSet.put( img.getImpId(), img );
			System.out.println( "firstChannelTilesSet size = " + firstChannelTilesSet.size() );

			// check what we've lost
			if ( firstChannelTilesSetPrev != null )
			{
				final Set< Integer > lostTilesOnLastIteration = new HashSet< >( firstChannelTilesSetPrev.keySet() );
				lostTilesOnLastIteration.removeAll( firstChannelTilesSet.keySet() );
				if ( !lostTilesOnLastIteration.isEmpty() )
					System.out.println( "-- Lost some tiles for channel 0: " + lostTilesOnLastIteration + " --" );
			}
			firstChannelTilesSetPrev = firstChannelTilesSet;

			anotherChannelTilesSet = new TreeMap< >();
			if ( anotherChannel != null )
			{
				// Construct another channel tiles set
				for ( final Entry< Integer, Map< Integer, ComparePair > > entry : anotherChannel.entrySet() )
				{
					for ( final Entry< Integer, ComparePair > valEntry : entry.getValue().entrySet() )
					{
						if ( !valEntry.getValue().getIsValidOverlap() )
							continue;

						final int[] ids = new int[] { entry.getKey(), valEntry.getKey() };
						final ImagePlusTimePoint[] imgs =
								valEntry.getValue().getTile1().getImpId() < valEntry.getValue().getTile2().getImpId() ?
										new ImagePlusTimePoint[] { valEntry.getValue().getTile1(), valEntry.getValue().getTile2() } :
											new ImagePlusTimePoint[] { valEntry.getValue().getTile2(), valEntry.getValue().getTile1() };

										for ( int j = 0; j < 2; j++ )
											anotherChannelTilesSet.put( ids[ j ], imgs[ j ] );
					}
				}
				System.out.println( "anotherChannelTilesSet size = " + anotherChannelTilesSet.size() );

				// check what we've lost
				if ( anotherChannelTilesSetPrev != null )
				{
					final Set< Integer > lostTilesOnLastIteration = new HashSet< >( anotherChannelTilesSetPrev.keySet() );
					lostTilesOnLastIteration.removeAll( anotherChannelTilesSet.keySet() );
					final Set< Integer > lostTilesOnLastIterationOffsetIndices = new HashSet< >();
					for ( final Integer key : lostTilesOnLastIteration )
						lostTilesOnLastIterationOffsetIndices.add( ((ImagePlusTimePoint)anotherChannelTilesSetPrev.get( key )).getImpId() );
					if ( !lostTilesOnLastIterationOffsetIndices.isEmpty() )
						System.out.println( "-- Lost some tiles for channel 1: " + lostTilesOnLastIterationOffsetIndices + " --" );
				}
				anotherChannelTilesSetPrev = anotherChannelTilesSet;

				// Add identity matches
				int identityMatches=0;
				for ( final Entry< Integer, Tile< ? > > entry : firstChannelTilesSet.entrySet() )
				{
					if ( anotherChannelTilesSet.containsKey( entry.getKey() ) && !badIdentityConnections.contains( entry.getKey() ) )
					{
						identityMatches++;

						final Point p1 = new Point( new double[ params.dimensionality ] );
						final Point p2 = new Point( new double[ params.dimensionality ] );

						final Tile t1 = entry.getValue();
						final Tile t2 = anotherChannelTilesSet.get( entry.getKey() );

						final FakeComparePair fakePair = new FakeComparePair((ImagePlusTimePoint)t1,(ImagePlusTimePoint)t2);

						t1.addMatch( new PointMatchStitching( p1, p2, ( float ) 0.1, fakePair ) );
						t2.addMatch( new PointMatchStitching( p2, p1, ( float ) 0.1, fakePair ) );

						t1.addConnectedTile( t2 );
						t2.addConnectedTile( t1 );
					}
				}
				System.out.println( "Added " + identityMatches + " identity matches" );
			}






			if ( tilesSet.isEmpty() )
			{

				if ( params.dimensionality == 3 )
				{
					Log.error( "Error: No correlated tiles found, setting the first tile to (0, 0, 0)." );
					final TranslationModel3D model = (TranslationModel3D)fixedImage.getModel();
					model.set( 0, 0, 0 );
				}
				else
				{
					Log.error( "Error: No correlated tiles found, setting the first tile to (0, 0)." );
					final TranslationModel2D model = (TranslationModel2D)fixedImage.getModel();
					model.set( 0, 0 );
				}

				final ArrayList< ImagePlusTimePoint > imageInformationList = new ArrayList< >();
				imageInformationList.add( fixedImage );

				Log.info(" number of tiles = " + imageInformationList.size() );

				return imageInformationList;
			}






			// trash everything but the largest graph
			final ArrayList< Set< Tile< ? > > > graphs = Tile.identifyConnectedGraphs( tilesSet );
			Log.info( "Number of tile graphs = " + graphs.size() );

			int largestGraphSize = 0;
			int largestGraphId = -1;

			final TreeMap< Integer, Integer > graphSizeToCount = new TreeMap<>();

			for ( int i = 0; i < graphs.size(); ++i )
			{
				final int graphSize = graphs.get( i ).size();

				if ( graphSize > largestGraphSize )
				{
					largestGraphSize = graphSize;
					largestGraphId = i;
				}

				graphSizeToCount.put( graphSize, graphSizeToCount.getOrDefault( graphSize, 0 ) + 1 );
			}

			for ( final Entry< Integer, Integer > entry : graphSizeToCount.descendingMap().entrySet() )
				System.out.println("   " + entry.getKey() + " : " + entry.getValue() + " graphs" );

			final ArrayList< Tile< ? > > largestGraph = new ArrayList< >();
			largestGraph.addAll( graphs.get( largestGraphId ) );
			tilesSet.clear();
			tilesSet.addAll( largestGraph );
			System.out.println( " Replacing tiles with the largest graph" );


			tc = new TileConfigurationStitching();
			tc.addTiles( tilesSet );

			// find a useful fixed tile
			if ( fixedImage.getConnectedTiles().size() > 0 )
			{
				tc.fixTile( fixedImage );
			}
			else
			{
				for ( final Tile<?> tile : tilesSet )
					if ( tile.getConnectedTiles().size() > 0 )
					{
						tc.fixTile( tile );
						break;
					}
			}
			//Log.info(" tiles size =" + tiles.size());
			//Log.info(" tc.getTiles() size =" + tc.getTiles().size());

			final ArrayList< Set< Tile< ? > > > graphsDebug = Tile.identifyConnectedGraphs( tilesSet );
			final ArrayList< Integer > graphsSize = new ArrayList<>();
			int graphSizesSum = 0;
			for ( final Set< Tile< ? > > graph : graphsDebug )
			{
				graphsSize.add( graph.size() );
				graphSizesSum += graph.size();
			}
			Collections.sort( graphsSize );
			Collections.reverse( graphsSize );
			System.out.println( "Tiles total = " + tc.getTiles().size() + ",   graphs=" + graphsDebug.size() + ",   sum="+graphSizesSum );
			System.out.println( graphsSize );

			try
			{
				System.setOut( suppressedOut );

				long elapsed = System.nanoTime();
				tc.preAlign();
				tc.optimize( 10, 2000, 2000 );
				elapsed = System.nanoTime() - elapsed;

				System.setOut( originalOut );

				System.out.println("Optimization round took " + elapsed/1e9 + "s" );

				// Notify the observer that the configuration has been updated
				if ( observer != null )
				{
					lostTiles = new HashSet<>();
					for ( final ComparePair pair : fullPairsSet )
						for ( final ImagePlusTimePoint t : new ImagePlusTimePoint[] { pair.getTile1(), pair.getTile2() } )
							lostTiles.add( t );
					for ( final Tile< ? > t : tc.getTiles() )
						lostTiles.remove( t );

					final ArrayList< ImagePlusTimePoint > updatedTilesConfiguration = new ArrayList< >();
					for ( final Tile< ? > t : tc.getTiles() )
						updatedTilesConfiguration.add( (ImagePlusTimePoint)t );
					Collections.sort( updatedTilesConfiguration );
					observer.configurationUpdated( updatedTilesConfiguration );
				}


				final double avgError = tc.getError();
				final double maxError = tc.getMaxError();

				itersCount++;

				double longestDisplacement = 0;
				PointMatch worstMatch = null;


				// new way of finding biggest error to look for the largest displacement
				for ( final Tile t : tc.getTiles() )
				{
					for ( final PointMatch p :  (Set< PointMatch >)t.getMatches() )
					{
						if ( p.getDistance() > longestDisplacement )
						{
							longestDisplacement = p.getDistance();
							worstMatch = p;
						}
					}
				}
				Log.info( "Maximal displacement: " + longestDisplacement );
				Log.info( "avgError (avg avg displacement) = " + avgError + ",  maxError (max avg displacement) = " + maxError );


				if ( ( ( avgError*params.relativeThreshold < maxError && maxError > 0.95 ) || avgError > params.absoluteThreshold ) )
				{
					/*
					Tile worstTile = tc.getWorstTile();
					Set< PointMatch > matches = worstTile.getMatches();

					float longestDisplacement = 0;
					PointMatch worstMatch = null;

					//Log.info( "worstTile: " + ((ImagePlusTimePoint)worstTile).getImagePlus().getTitle() );

					for (PointMatch p : matches)
					{
						//Log.info( "distance: " + p.getDistance() + " to " + ((PointMatchStitching)p).getPair().getImagePlus2().getTitle() );

						if (p.getDistance() > longestDisplacement)
						{
							longestDisplacement = p.getDistance();
							worstMatch = p;
						}
					}
					 */
					final ComparePair pair = ((PointMatchStitching)worstMatch).getPair();

					Log.info( "Identified link between " + pair.getImagePlus1().getTitle() + "[" + pair.getTile1().getTimePoint() + "] and " +
							pair.getImagePlus2().getTitle() + "[" + pair.getTile2().getTimePoint() + "] (R=" + pair.getCrossCorrelation() +") to be bad. Reoptimizing.");


					// removing identity connection
					if ( ((PointMatchStitching)worstMatch).getPair() instanceof FakeComparePair )
					{
						final int ind1 = ((PointMatchStitching)worstMatch).getPair().getTile1().getImpId();
						final int ind2 = ((ImagePlusTimePoint)anotherChannelTilesSet.get( ind1 )).getImpId();

						if ( ind2 != ((PointMatchStitching)worstMatch).getPair().getTile2().getImpId() )
						{
							System.out.println( "Indices mismatch in identity connection!" );
							System.out.println( "tile1: " + ((PointMatchStitching)worstMatch).getPair().getTile1().getImpId() );
							System.out.println( "tile2: " + ((PointMatchStitching)worstMatch).getPair().getTile2().getImpId() );
						}
						badIdentityConnections.add( ind1 );
					}
					else
					{
						((PointMatchStitching)worstMatch).getPair().setIsValidOverlap( false );
					}

					redo = true;

					for ( final Tile< ? > t : tilesSet )
					{
						t.getConnectedTiles().clear();
						t.getMatches().clear();
					}
				}
			}
			catch ( final Exception e )
			{
				Log.error( "Cannot compute global optimization: " + e, e );
			}
		}
		while(redo);

		System.out.println( "*** Global optimization made " + itersCount + " iterations ***" );



		lostTiles = new HashSet<>();
		for ( final ComparePair pair : fullPairsSet )
			for ( final ImagePlusTimePoint t : new ImagePlusTimePoint[] { pair.getTile1(), pair.getTile2() } )
				lostTiles.add( t );
		for ( final Tile< ? > t : tc.getTiles() )
			lostTiles.remove( t );
		System.out.println( "Tiles lost: " + lostTiles.size() );


		// create a list of image informations containing their positions
		final ArrayList< ImagePlusTimePoint > imageInformationList = new ArrayList< >();
		for ( final Tile< ? > t : tc.getTiles() )
			imageInformationList.add( (ImagePlusTimePoint)t );

		Collections.sort( imageInformationList );





		// Create a helper lookup structure
		/*final Map< Integer, Integer > anotherToFirst = new HashMap< Integer, Integer >();
		for ( final Entry< Integer, Tile< ? > > entry : anotherChannelTilesSet.entrySet() )
			anotherToFirst.put( ((ImagePlusTimePoint)entry.getValue()).getImpId(), entry.getKey() );

		// Fix existing tiles
		for ( final Tile< ? > t : tc.getTiles() )
			tc.fixTile( t );
		// Add lost tiles one by one
		final Set<Tile<?>> lostTilesFirstChannel = new HashSet<Tile<?>>();
		final Set<Tile<?>> lostTilesAnotherChannel = new HashSet<Tile<?>>();
		for ( final Tile<?> lostTile : lostTiles )
		{
			if ( firstChannelTilesSet.containsKey( ((ImagePlusTimePoint)lostTile).getImpId() ) )
				lostTilesFirstChannel.add( lostTile );
			else
				//if ( anotherChannelTilesSet.containsKey( ((ImagePlusTimePoint)lostTile).getImpId() ) )
				lostTilesAnotherChannel.add( lostTile ); // must be another channel tile
		}

		// First, add tiles that are present for one of the channels but are missing for the another one
		final List< Integer > lostOnlyForFirstChannel = new ArrayList< Integer >();
		for ( final Tile<?> tile : lostTilesFirstChannel )
			if ( !lostTilesAnotherChannel.contains( anotherChannelTilesSet.get( ((ImagePlusTimePoint)tile).getImpId() ) ) )
				lostOnlyForFirstChannel.add( ((ImagePlusTimePoint)tile).getImpId() );
		System.out.println( "lostOnlyForFirstChannel: " + lostOnlyForFirstChannel.size() );

		// Same vice versa
		final List< Integer > lostOnlyForAnotherChannel = new ArrayList< Integer >();
		for ( final Tile<?> tile : lostTilesAnotherChannel )
			if ( !lostTilesFirstChannel.contains( firstChannelTilesSet.get( anotherToFirst.get( ((ImagePlusTimePoint)tile).getImpId() ) ) ) )
				lostOnlyForAnotherChannel.add( ((ImagePlusTimePoint)tile).getImpId() );
		System.out.println( "lostOnlyForAnotherChannel: " + lostOnlyForAnotherChannel.size() );*/



		/*while ( !lostTilesFirstChannel.isEmpty() )
		{
			// TODO: strategy of choosing the lost tile (ordering them by a number of matches)
			final Tile<?> tileFirstChannel = lostTilesFirstChannel.toArray( new Tile[0] )[ 0 ];
			lostTilesFirstChannel.remove( tileFirstChannel );
			if ( anotherChannel != null )
			{
				final Tile<?> tileAnotherChannel = anotherChannelTilesSet.get( ((ImagePlusTimePoint)tileFirstChannel).getImpId() );
				lostTilesAnotherChannel.remove( tileFirstChannel );
			}
		}*/










		return imageInformationList;
	}
}



class FakeComparePair extends ComparePair
{
	public FakeComparePair( final ImagePlusTimePoint impA, final ImagePlusTimePoint impB )
	{
		super( impA, impB );
	}
}
