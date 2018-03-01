package org.janelia.stitching;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.NotImplementedException;

import mpicbg.models.AffineModel2D;
import mpicbg.models.AffineModel3D;
import mpicbg.models.ConstantAffineModel2D;
import mpicbg.models.ConstantAffineModel3D;
import mpicbg.models.ErrorStatistic;
import mpicbg.models.IllDefinedDataPointsException;
import mpicbg.models.InterpolatedAffineModel2D;
import mpicbg.models.InterpolatedAffineModel3D;
import mpicbg.models.Model;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.PointMatch;
import mpicbg.models.RigidModel2D;
import mpicbg.models.RigidModel3D;
import mpicbg.models.SimilarityModel2D;
import mpicbg.models.SimilarityModel3D;
import mpicbg.models.Tile;
import mpicbg.models.TileConfiguration;
import mpicbg.models.TranslationModel2D;
import mpicbg.models.TranslationModel3D;
import mpicbg.stitching.ImagePlusTimePoint;

// based on the GlobalOptimization class from the original Fiji's Stitching plugin repository
public class GlobalOptimizationPerformer
{
	private static final double DAMPNESS_FACTOR = 0.9;
	private static final double REGULARIZER_TRANSLATION = 0.1;

	public Map< Integer, Tile< ? > > lostTiles = null;

	public boolean translationOnlyStitching = false;

	public int replacedTilesTranslation = 0, replacedTilesSimilarity = 0;

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

	public int remainingGraphSize, remainingPairs;
	public double avgDisplacement, maxDisplacement;

	public synchronized static void suppressOutput()
	{
		System.setOut( suppressedOut );
	}
	public synchronized static void restoreOutput()
	{
		System.setOut( originalOut );
	}

	public ArrayList< ImagePlusTimePoint > optimize(
			final Vector< ComparePointPair > comparePointPairs,
			final SerializableStitchingParameters params ) throws NotEnoughDataPointsException, IllDefinedDataPointsException, InterruptedException, ExecutionException
	{
		return optimize( comparePointPairs, params, null );
	}

	public ArrayList< ImagePlusTimePoint > optimize(
			final Vector< ComparePointPair > comparePointPairs,
			final SerializableStitchingParameters params,
			final PrintWriter logWriter ) throws NotEnoughDataPointsException, IllDefinedDataPointsException, InterruptedException, ExecutionException
	{
		// create a set of tiles
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

		writeLog( logWriter, "Pairs above the threshold: " + pairsAdded + ", pairs total = " + comparePointPairs.size() );

		if ( tilesSet.isEmpty() )
			return null;

		// print graph sizes
		final TreeMap< Integer, Integer > graphSizeToCount = getGraphsSize( tilesSet );
		writeLog( logWriter, "Number of tile graphs = " + graphSizeToCount.values().stream().mapToInt( Number::intValue ).sum() );
		for ( final Entry< Integer, Integer > entry : graphSizeToCount.descendingMap().entrySet() )
			writeLog( logWriter, "   " + entry.getKey() + " tiles: " + entry.getValue() + " graphs" );

		// trash everything but the largest graph
		final int numTilesBeforeRetainingLargestGraph = tilesSet.size();
		preserveOnlyLargestGraph( tilesSet );
		final int numTilesAfterRetainingLargestGraph = tilesSet.size();

		writeLog( logWriter, "Using the largest graph of size " + numTilesAfterRetainingLargestGraph + " (throwing away " + ( numTilesBeforeRetainingLargestGraph - numTilesAfterRetainingLargestGraph ) + " tiles from smaller graphs)" );
		remainingGraphSize = numTilesAfterRetainingLargestGraph;

		remainingPairs = countRemainingPairs( tilesSet, comparePointPairs );

		// if all tiles have underlying translation models, consider this stitching configuration to be translation-only
		translationOnlyStitching = true;
		for ( final Tile< ? > tile : tilesSet )
		{
			if ( !( tile.getModel() instanceof TranslationModel2D || tile.getModel() instanceof TranslationModel3D ) )
			{
				translationOnlyStitching = false;
				break;
			}
		}

		// if some of the tiles do not have enough point matches for a high-order model, fall back to simpler model
		replacedTilesTranslation = ensureEnoughPointMatches( tilesSet );

		// if the point matches are on the same plane, fall back to SimilarityModel3D (otherwise AffineModel3D will throw IllDefinedPointsException)
		replacedTilesSimilarity = ensureNotIllDefined( tilesSet );

		final TileConfiguration tc = new TileConfiguration();
		tc.addTiles( tilesSet );

		// find a useful fixed tile
		for ( final Tile< ? > tile : tilesSet )
		{
			if ( tile.getConnectedTiles().size() > 0 )
			{
				tc.fixTile( tile );
				break;
			}
		}

		if ( translationOnlyStitching )
		{
			tc.preAlign();
		}
		else
		{
			// prealign using translation model, otherwise might get IllDefined exception due to selecting a subset of point matches between a pair of tiles
			if ( tc.getFixedTiles().size() != 1 )
				throw new RuntimeException( "number of fixed tiles: " + tc.getFixedTiles().size() + ", expected 1" );
			final Tile< ? > fixedTile = tc.getFixedTiles().iterator().next();
			prealignWithTranslationModel( tilesSet, fixedTile );
		}

		final int iterations = 5000;

		long elapsed = System.nanoTime();

		tc.optimizeSilently(
				new ErrorStatistic( iterations + 1 ),
				0, // max allowed error -- does not matter as maxPlateauWidth=maxIterations
				iterations,
				iterations,
				translationOnlyStitching ? 1 : DAMPNESS_FACTOR
			);

		elapsed = System.nanoTime() - elapsed;

		writeLog( logWriter, "Optimization round took " + elapsed/1e9 + "s" );

		final double avgError = tc.getError();
		final double maxError = tc.getMaxError();

		// new way of finding biggest error to look for the largest displacement
		double longestDisplacement = 0;
		for ( final Tile< ? > t : tc.getTiles() )
			for ( final PointMatch p :  t.getMatches() )
				longestDisplacement = Math.max( p.getDistance(), longestDisplacement );

		writeLog( logWriter, "" );
		writeLog( logWriter, "Max pairwise match displacement: " + longestDisplacement );
		writeLog( logWriter, String.format( "avg error: %.2fpx", avgError ) );
		writeLog( logWriter, String.format( "max error: %.2fpx", maxError ) );

		avgDisplacement = avgError;
		maxDisplacement = maxError;

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

	private static int ensureEnoughPointMatches( final Set< Tile< ? > > tilesSet )
	{
		int numTilesReplacedWithTranslation = 0;

		final Set< Tile< ? > > newTilesSet = new HashSet<>();
		for ( final Tile< ? > tile : tilesSet )
		{
			if ( tile.getMatches().size() < tile.getModel().getMinNumMatches() )
			{
				newTilesSet.add( getReplacementTile( tile, getTranslationReplacementModel( tile ) ) );
				++numTilesReplacedWithTranslation;
			}
			else
			{
				newTilesSet.add( tile );
			}
		}

		tilesSet.clear();
		tilesSet.addAll( newTilesSet );

		return numTilesReplacedWithTranslation;
	}

	private static int ensureNotIllDefined( final Set< Tile< ? > > tilesSet )
	{
		int numTilesReplacedWithSimilarity = 0;

		final Set< Tile< ? > > newTilesSet = new HashSet<>();
		for ( final Tile< ? > tile : tilesSet )
		{
			if ( tile.getModel() instanceof TranslationModel2D || tile.getModel() instanceof TranslationModel3D )
			{
				newTilesSet.add( tile );
			}
			else
			{
				final Tile< ? > tileCopy = new ImagePlusTimePoint(
						( ( ImagePlusTimePoint ) tile ).getImagePlus(),
						( ( ImagePlusTimePoint ) tile ).getImpId(),
						( ( ImagePlusTimePoint ) tile ).getTimePoint(),
						tile.getModel().copy(),
						( ( ImagePlusTimePoint ) tile ).getElement()
					);

				for ( final PointMatch pointMatch : tile.getMatches() )
				{
					final PointMatch pointMatchCopy = new PointMatch(
							pointMatch.getP1().clone(),
							pointMatch.getP2().clone(),
							pointMatch.getWeight()
						);
					tileCopy.addMatch( pointMatchCopy );
				}

				boolean illDefined = false;
				try
				{
					tileCopy.fitModel();
				}
				catch ( final NotEnoughDataPointsException e )
				{
					throw new RuntimeException( "should not happen! already checked for having enough point matches" );
				}
				catch ( final IllDefinedDataPointsException e )
				{
					illDefined = true;
				}

				if ( illDefined )
				{
					newTilesSet.add( getReplacementTile( tile, getSimilarityReplacementModel( tile ) ) );
					++numTilesReplacedWithSimilarity;
				}
				else
				{
					newTilesSet.add( tile );
				}
			}
		}
		tilesSet.clear();
		tilesSet.addAll( newTilesSet );

		return numTilesReplacedWithSimilarity;
	}

	private static void prealignWithTranslationModel( final Set< Tile< ? > > tilesSet, final Tile< ? > fixedTile ) throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		// create translation tiles to be able to change the graph topology while keeping the original graph intact
		final Set< Tile< ? > > translationTilesSet = new HashSet<>();
		for ( final Tile< ? > tile : tilesSet )
		{
			final Tile< ? > translationTile = new ImagePlusTimePoint(
					( ( ImagePlusTimePoint ) tile ).getImagePlus(),
					( ( ImagePlusTimePoint ) tile ).getImpId(),
					( ( ImagePlusTimePoint ) tile ).getTimePoint(),
					getTranslationReplacementModel( tile ),
					( ( ImagePlusTimePoint ) tile ).getElement()
				);
			translationTile.addMatches( tile.getMatches() );
			translationTilesSet.add( translationTile );
		}

		// translation tiles still have old references to original tiles in connectedTiles, need to update the references
		final Map< Integer, Tile< ? > > translationTileIndexMapping = getTileIndexMapping( translationTilesSet );
		for ( final Tile< ? > tile : tilesSet )
		{
			final Tile< ? > translationTile = translationTileIndexMapping.get( ( ( ImagePlusTimePoint ) tile ).getImpId() );
			for ( final Tile< ? > connectedTile : tile.getConnectedTiles() )
			{
				final Tile< ? > connectedTranslationTile = translationTileIndexMapping.get( ( ( ImagePlusTimePoint ) connectedTile ).getImpId() );
				translationTile.addConnectedTile( connectedTranslationTile );
			}
		}

		// create a translation-based tile configuration
		final TileConfiguration translationTileConfiguration = new TileConfiguration();
		translationTileConfiguration.addTiles( translationTilesSet );
		// fix the same tile as in the original tile configuration
		for ( final Tile< ? > translationTile : translationTilesSet )
		{
			if ( ( ( ImagePlusTimePoint ) translationTile ).getImpId() == ( ( ImagePlusTimePoint ) fixedTile ).getImpId() )
			{
				translationTileConfiguration.fixTile( translationTile );
				break;
			}
		}
		// prealign translation-based configuration
		translationTileConfiguration.preAlign();

		// original point matches have been prealigned, only need to apply the prealigned translation component to original tile models
		final Map< Integer, Tile< ? > > tileIndexToOriginalTile = getTileIndexMapping( tilesSet );
		for ( final Tile< ? > prealignedTranslationTile : translationTilesSet )
		{
			final Tile< ? > originalTile = tileIndexToOriginalTile.get( ( ( ImagePlusTimePoint ) prealignedTranslationTile ).getImpId() );
			final int dim = originalTile.getMatches().iterator().next().getP1().getL().length;
			if ( dim == 2 )
			{
				final double[] prealignedTranslation = ( ( TranslationModel2D ) prealignedTranslationTile.getModel() ).getTranslation();
				updateModel2DWithTranslation(
						originalTile.getModel(),
						prealignedTranslation[ 0 ],
						prealignedTranslation[ 1 ]
					);
			}
			else
			{
				final double[] prealignedTranslation = ( ( TranslationModel3D ) prealignedTranslationTile.getModel() ).getTranslation();
				updateModel3DWithTranslation(
						originalTile.getModel(),
						prealignedTranslation[ 0 ],
						prealignedTranslation[ 1 ],
						prealignedTranslation[ 2 ]
					);
			}
			originalTile.updateCost();
		}
	}

	private static Map< Integer, Tile< ? > > getTileIndexMapping( final Set< Tile< ? > > tilesSet )
	{
		final Map< Integer, Tile< ? > > tileIndexMapping = new HashMap<>();
		for ( final Tile< ? > tile : tilesSet )
			tileIndexMapping.put( ( ( ImagePlusTimePoint ) tile ).getImpId(), tile );
		return tileIndexMapping;
	}

	private static void updateModel2DWithTranslation(
			final Model< ? > model,
			final double tx,
			final double ty )
	{
		if ( model instanceof TranslationModel2D )
		{
			final TranslationModel2D actualModel = ( TranslationModel2D ) model;
			actualModel.set( tx, ty );
		}
		else if ( model instanceof RigidModel2D )
		{
			final RigidModel2D actualModel = ( RigidModel2D ) model;
			actualModel.set( 0, tx, ty );
		}
		else if ( model instanceof SimilarityModel2D )
		{
			final SimilarityModel2D actualModel = ( SimilarityModel2D ) model;
			actualModel.setScaleRotationTranslation( 1, 0, tx, ty );
		}
		else if ( model instanceof AffineModel2D )
		{
			final AffineModel2D actualModel = ( AffineModel2D ) model;
			actualModel.set( 1, 0, 0, 1, tx, ty );
		}
		else if ( model instanceof InterpolatedAffineModel2D< ?, ? > )
		{
			final InterpolatedAffineModel2D< ?, ? > actualModel = ( InterpolatedAffineModel2D< ?, ? > ) model;
			updateModel2DWithTranslation( actualModel.getA(), tx, ty );
			updateModel2DWithTranslation( actualModel.getB(), tx, ty );
			actualModel.set( 1, 0, 0, 1, tx, ty );
		}
		else if ( !( model instanceof ConstantAffineModel2D< ? > ) )
		{
			throw new NotImplementedException( "model updating is not implemented for " + model );
		}
	}

	private static void updateModel3DWithTranslation(
			final Model< ? > model,
			final double tx,
			final double ty,
			final double tz )
	{
		if ( model instanceof TranslationModel3D )
		{
			final TranslationModel3D actualModel = ( TranslationModel3D ) model;
			actualModel.set( tx, ty, tz );
		}
		else if ( model instanceof RigidModel3D )
		{
			final RigidModel3D actualModel = ( RigidModel3D ) model;
			actualModel.set(
					1, 0, 0, tx,
					0, 1, 0, ty,
					0, 0, 1, tz
				);
		}
		else if ( model instanceof SimilarityModel3D )
		{
			final SimilarityModel3D actualModel = ( SimilarityModel3D ) model;
			actualModel.set(
					1, 0, 0, tx,
					0, 1, 0, ty,
					0, 0, 1, tz
				);
		}
		else if ( model instanceof AffineModel3D )
		{
			final AffineModel3D actualModel = ( AffineModel3D ) model;
			actualModel.set(
					1, 0, 0, tx,
					0, 1, 0, ty,
					0, 0, 1, tz
				);
		}
		else if ( model instanceof InterpolatedAffineModel3D )
		{
			final InterpolatedAffineModel3D< ?, ? > actualModel = ( InterpolatedAffineModel3D< ?, ? > ) model;
			updateModel3DWithTranslation( actualModel.getA(), tx, ty, tz );
			updateModel3DWithTranslation( actualModel.getB(), tx, ty, tz );
			actualModel.set(
					1, 0, 0, tx,
					0, 1, 0, ty,
					0, 0, 1, tz
				);
		}
		else if ( !( model instanceof ConstantAffineModel3D< ? > ) )
		{
			throw new NotImplementedException( "model updating is not implemented for " + model );
		}
	}

	// replacing the model of the existing tile is not supported, thus need to replace the tile while keeping all point matches
	private static Tile< ? > getReplacementTile( final Tile< ? > tile, final Model< ? > replacementModel )
	{
		final Tile< ? > replacementTile = new ImagePlusTimePoint(
				( ( ImagePlusTimePoint ) tile ).getImagePlus(),
				( ( ImagePlusTimePoint ) tile ).getImpId(),
				( ( ImagePlusTimePoint ) tile ).getTimePoint(),
				replacementModel,
				( ( ImagePlusTimePoint ) tile ).getElement()
			);
		replacementTile.addMatches( tile.getMatches() );
		for ( final Tile< ? > connectedTile : tile.getConnectedTiles() )
		{
			// don't use removeConnectedTile() because it tries too hard and removes point matches from both sides which we would like to preserve
			connectedTile.getConnectedTiles().remove( tile );
			connectedTile.addConnectedTile( replacementTile );
			replacementTile.addConnectedTile( connectedTile );
		}
		return replacementTile;
	}

	private static Model< ? > getTranslationReplacementModel( final Tile< ? > tile )
	{
		final int dim = tile.getMatches().iterator().next().getP1().getL().length;
		final Model< ? > replacementModel = dim == 2 ? new TranslationModel2D() : new TranslationModel3D();
		return replacementModel;
	}

	private static Model< ? > getSimilarityReplacementModel( final Tile< ? > tile )
	{
		final int dim = tile.getMatches().iterator().next().getP1().getL().length;
		final Model< ? > replacementModel;
		if ( dim == 2 )
		{
			replacementModel = new InterpolatedAffineModel2D<>(
					new SimilarityModel2D(),
					new TranslationModel2D(),
					REGULARIZER_TRANSLATION
				);
		}
		else
		{
			replacementModel = new InterpolatedAffineModel3D<>(
					new SimilarityModel3D(),
					new TranslationModel3D(),
					REGULARIZER_TRANSLATION
				);
		}
		return replacementModel;
	}

	private static TreeMap< Integer, Integer > getGraphsSize( final Set< Tile< ? > > tilesSet )
	{
		final TreeMap< Integer, Integer > graphSizeToCount = new TreeMap<>();

		final ArrayList< Set< Tile< ? > > > graphs = Tile.identifyConnectedGraphs( tilesSet );
		for ( final Set< Tile< ? > > graph : graphs )
		{
			final int graphSize = graph.size();
			graphSizeToCount.put( graphSize, graphSizeToCount.getOrDefault( graphSize, 0 ) + 1 );
		}

		return graphSizeToCount;
	}

	private static void preserveOnlyLargestGraph( final Set< Tile< ? > > tilesSet )
	{
		// get components
		final ArrayList< Set< Tile< ? > > > graphs = Tile.identifyConnectedGraphs( tilesSet );
		int largestGraphSize = 0, largestGraphId = -1;
		for ( int i = 0; i < graphs.size(); ++i )
		{
			final int graphSize = graphs.get( i ).size();
			if ( graphSize > largestGraphSize )
			{
				largestGraphSize = graphSize;
				largestGraphId = i;
			}
		}

		// retain the largest component
		final ArrayList< Tile< ? > > largestGraph = new ArrayList<>();
		largestGraph.addAll( graphs.get( largestGraphId ) );
		tilesSet.clear();
		tilesSet.addAll( largestGraph );
	}

	private static int countRemainingPairs( final Set< Tile< ? > > remainingTilesSet, final Vector< ComparePointPair > comparePointPairs )
	{
		int remainingPairs = 0;
		for ( final ComparePointPair comparePointPair : comparePointPairs )
			if ( comparePointPair.getIsValidOverlap() && remainingTilesSet.contains( comparePointPair.getTile1() ) && remainingTilesSet.contains( comparePointPair.getTile2() ) )
				++remainingPairs;
		return remainingPairs;
	}

	private static void writeLog( final PrintWriter logWriter, final String log )
	{
		if ( logWriter != null )
			logWriter.println( log );
		System.out.println( log );
	}
}
