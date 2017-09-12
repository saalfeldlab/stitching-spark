package org.janelia.stitching;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import mpicbg.models.IllDefinedDataPointsException;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.PointMatch;
import mpicbg.models.Tile;
import mpicbg.models.TileConfiguration;
import mpicbg.models.TranslationModel3D;
import mpicbg.stitching.ImagePlusTimePoint;


// based on the GlobalOptimization class from the original Fiji's Stitching plugin repository
public class GlobalOptimizationPerformer
{
	private static final double DAMPNESS_FACTOR = 0.9;

	public Map< Integer, Tile< ? > > lostTiles = null;

	public int replacedTiles = 0;

	static private final java.io.PrintStream originalOut, suppressedOut;
	static
	{
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
	}

	public int remainingGraphSize;
	public double avgDisplacement, maxDisplacement;

	public static void suppressOutput()
	{
		System.setOut( suppressedOut );
	}
	public static void restoreOutput()
	{
		System.setOut( originalOut );
	}

	public ArrayList< ImagePlusTimePoint > optimize(
			final Vector< ComparePointPair > comparePointPairs,
			final SerializableStitchingParameters params ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		return optimize( comparePointPairs, params, null );
	}

	public ArrayList< ImagePlusTimePoint > optimize(
			final Vector< ComparePointPair > comparePointPairs,
			final SerializableStitchingParameters params,
			final PrintWriter logWriter ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		final Set< Tile< ? > > tilesSet = new HashSet<>();

		int pairsAdded = 0;

		for ( final ComparePointPair comparePointPair : comparePointPairs )
		{
			if ( comparePointPair.getIsValidOverlap() )
			{
				++pairsAdded;

				final Tile< ? > t1 = comparePointPair.getTile1();
				final Tile< ? > t2 = comparePointPair.getTile2();

				final float weight = comparePointPair.getCrossCorrelation();

				t1.addMatch( new PointMatch( comparePointPair.getPointPair().getA(), comparePointPair.getPointPair().getB(), weight ) );
				t2.addMatch( new PointMatch( comparePointPair.getPointPair().getB(), comparePointPair.getPointPair().getA(), weight ) );

				t1.addConnectedTile( t2 );
				t2.addConnectedTile( t1 );

				tilesSet.add( t1 );
				tilesSet.add( t2 );
			}
		}

		// check if there are enough point matches
		{
			final Set< Tile< ? > > newTilesSet = new HashSet<>();
			for ( final Tile< ? > tile : tilesSet )
			{
				if ( tile.getMatches().size() < tile.getModel().getMinNumMatches() )
				{
					final Tile< ? > replacementTile = new ImagePlusTimePoint(
							( ( ImagePlusTimePoint ) tile ).getImagePlus(),
							( ( ImagePlusTimePoint ) tile ).getImpId(),
							( ( ImagePlusTimePoint ) tile ).getTimePoint(),
							new TranslationModel3D(),
							null
						);
					replacementTile.addMatches( tile.getMatches() );
					for ( final Tile< ? > connectedTile : tile.getConnectedTiles() )
					{
						// don't use removeConnectedTile() because it "overdoes" it by removing point matches from both sides which we would like to preserve
						connectedTile.getConnectedTiles().remove( tile );
						connectedTile.addConnectedTile( replacementTile );
						replacementTile.addConnectedTile( connectedTile );
					}
					newTilesSet.add( replacementTile );
					++replacedTiles;
				}
				else
				{
					newTilesSet.add( tile );
				}
			}
			tilesSet.clear();
			tilesSet.addAll( newTilesSet );
		}

		writeLog( logWriter, "Pairs above the threshold: " + pairsAdded + ", pairs total = " + comparePointPairs.size() );

		if ( tilesSet.isEmpty() )
			return null;

		// trash everything but the largest graph
		final ArrayList< Set< Tile< ? > > > graphs = Tile.identifyConnectedGraphs( tilesSet );
		writeLog( logWriter, "Number of tile graphs = " + graphs.size() );

		int largestGraphSize = 0;
		int largestGraphId = -1;
		int graphSizesSum = 0;

		final TreeMap< Integer, Integer > graphSizeToCount = new TreeMap<>();

		for ( int i = 0; i < graphs.size(); ++i )
		{
			final int graphSize = graphs.get( i ).size();

			graphSizesSum += graphSize;

			if ( graphSize > largestGraphSize )
			{
				largestGraphSize = graphSize;
				largestGraphId = i;
			}

			graphSizeToCount.put( graphSize, graphSizeToCount.getOrDefault( graphSize, 0 ) + 1 );
		}

		for ( final Entry< Integer, Integer > entry : graphSizeToCount.descendingMap().entrySet() )
			writeLog( logWriter, "   " + entry.getKey() + " tiles: " + entry.getValue() + " graphs" );

		final ArrayList< Tile< ? > > largestGraph = new ArrayList< >();
		largestGraph.addAll( graphs.get( largestGraphId ) );
		tilesSet.clear();
		tilesSet.addAll( largestGraph );

		writeLog( logWriter, "Using the largest graph of size " + largestGraphSize + " (throwing away " + ( graphSizesSum - largestGraphSize ) + " tiles from smaller graphs)" );
		remainingGraphSize = largestGraphSize;

		final TileConfiguration tc = new TileConfiguration();
		tc.addTiles( tilesSet );

		// find a useful fixed tile
		for ( final Tile<?> tile : tilesSet )
			if ( tile.getConnectedTiles().size() > 0 )
			{
				tc.fixTile( tile );
				break;
			}

		final ArrayList< Set< Tile< ? > > > graphsDebug = Tile.identifyConnectedGraphs( tilesSet );
		final ArrayList< Integer > graphsSize = new ArrayList<>();
		graphSizesSum = 0;
		for ( final Set< Tile< ? > > graph : graphsDebug )
		{
			graphsSize.add( graph.size() );
			graphSizesSum += graph.size();
		}
		Collections.sort( graphsSize );
		Collections.reverse( graphsSize );
		writeLog( logWriter, "Tiles total = " + tc.getTiles().size() + ",   graphs=" + graphsDebug.size() + ",   sum="+graphSizesSum + ", " + graphsSize );

		long elapsed = System.nanoTime();

		final int iterations = 5000;
		tc.preAlign();
		tc.optimize( 10, iterations, iterations, DAMPNESS_FACTOR, 1 );
		elapsed = System.nanoTime() - elapsed;

		writeLog( logWriter, "Optimization round took " + elapsed/1e9 + "s" );

		final double avgError = tc.getError();
		final double maxError = tc.getMaxError();


		final List< Double > errors = new ArrayList<>();

		double longestDisplacement = 0;
		PointMatch worstMatch = null;

		// new way of finding biggest error to look for the largest displacement
		for ( final Tile t : tc.getTiles() )
		{
			for ( final PointMatch p :  (Set< PointMatch >)t.getMatches() )
			{
				final double error = p.getDistance();
				errors.add( error );
				if ( longestDisplacement < error )
				{
					longestDisplacement = error;
					worstMatch = p;
				}
			}
		}

		writeLog( logWriter, "" );
		writeLog( logWriter, "Max pairwise match displacement: " + longestDisplacement );
		writeLog( logWriter, String.format( "avg error: %.2fpx", avgError ) );
		writeLog( logWriter, String.format( "max error: %.2fpx", maxError ) );

		avgDisplacement = avgError;
		maxDisplacement = maxError;


		Collections.sort( errors );
		Collections.reverse( errors );

		lostTiles = new TreeMap<>();
		for ( final ComparePointPair comparePointPair : comparePointPairs )
			for ( final ImagePlusTimePoint t : new ImagePlusTimePoint[] { comparePointPair.getTile1(), comparePointPair.getTile2() } )
				lostTiles.put( t.getImpId(), t );
		for ( final Tile< ? > t : tc.getTiles() )
			lostTiles.remove( ( ( ImagePlusTimePoint ) t ).getImpId() );
		System.out.println( "Tiles lost: " + lostTiles.size() );


		// create a list of image informations containing their positions
		final ArrayList< ImagePlusTimePoint > imageInformationList = new ArrayList< >();
		for ( final Tile< ? > t : tc.getTiles() )
			imageInformationList.add( (ImagePlusTimePoint)t );

		Collections.sort( imageInformationList );

		return imageInformationList;
	}

	private void writeLog( final PrintWriter logWriter, final String log )
	{
		if ( logWriter != null )
			logWriter.println( log );
		System.out.println( log );
	}
}