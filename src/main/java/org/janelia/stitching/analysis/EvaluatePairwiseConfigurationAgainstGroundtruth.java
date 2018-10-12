package org.janelia.stitching.analysis;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

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

	private static class EvaluatePairwiseConfigurationCmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-g", aliases = { "--groundtruthTilesPath" }, required = true,
				usage = "Path to the grountruth tile configuration file.")
		public String groundtruthTilesPath;

		@Option(name = "-p", aliases = { "--pairwiseConfigurationPath" }, required = true,
				usage = "Path to the pairwise file to be evaluated.")
		public String pairwiseConfigurationPath;

		@Option(name = "-t", aliases = { "--tileToInspect" }, required = false,
				usage = "Tile to inspect (will print pairs containing this tile).")
		public Integer tileToInspect;

		@Option(name = "-c", aliases = { "--crossCorrelationThreshold" }, required = false,
				usage = "Cross-correlation threshold to apply to filter the pairwise configuration.")
		public Double crossCorrelationThreshold;

		@Option(name = "-v", aliases = { "--varianceThreshold" }, required = false,
				usage = "Intensity variance threshold to apply to filter the pairwise configuration.")
		public Double varianceThreshold;

		public boolean parsedSuccessfully = false;

		public EvaluatePairwiseConfigurationCmdArgs( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}

			if ( ( crossCorrelationThreshold == null ) != ( varianceThreshold == null ) )
				throw new IllegalArgumentException( "Either no thresholds or both thresholds should be specified" );

			if ( crossCorrelationThreshold == null && varianceThreshold == null )
				System.out.println( "do not threshold pairwise results" );
			else
				System.out.println( String.format( "thresholding pairwise results using min.cross.corr=%.2f and min.variance=%.2f", crossCorrelationThreshold, varianceThreshold ) );
			System.out.println();
		}
	}

	public static < M extends Model< M > > void main( final String[] args ) throws Exception
	{
		final EvaluatePairwiseConfigurationCmdArgs parsedArgs = new EvaluatePairwiseConfigurationCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > groundtruthTilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( parsedArgs.groundtruthTilesPath ) ) ) );
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( parsedArgs.pairwiseConfigurationPath ) ) );

		if ( parsedArgs.crossCorrelationThreshold != null || parsedArgs.varianceThreshold != null )
		{
			int numPairsBelowThresholds = 0;
			for ( final Iterator< SerializablePairWiseStitchingResult > it = pairwiseShifts.iterator(); it.hasNext(); )
			{
				final SerializablePairWiseStitchingResult pairwiseShift = it.next();
				if ( parsedArgs.crossCorrelationThreshold > pairwiseShift.getCrossCorrelation() || parsedArgs.varianceThreshold > pairwiseShift.getVariance() )
				{
					++numPairsBelowThresholds;
					it.remove();
				}
			}
			System.out.println( numPairsBelowThresholds + " pairs out of " + ( numPairsBelowThresholds + pairwiseShifts.size() ) + " were below the treshold" );
			System.out.println();
		}

		final List< SerializablePairWiseStitchingResult > removedPairsNotInGroundtruth = new ArrayList<>();
		for ( final Iterator< SerializablePairWiseStitchingResult > it = pairwiseShifts.iterator(); it.hasNext(); )
		{
			final SerializablePairWiseStitchingResult pairwiseShift = it.next();
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
		System.out.println( "Removed " + removedPairsNotInGroundtruth.size() + " pairs where at least one tile is not contained in the groundtruth set:" );
		for ( final SerializablePairWiseStitchingResult pairwiseShift : removedPairsNotInGroundtruth )
			System.out.println( "  " + pairwiseShift.getSubTilePair() );
		System.out.println();

		System.out.println( "Calculating errors using pairwise match configuration consisting of " + pairwiseShifts.size() + " elements" );

		final Map< Integer, Map< Integer, List< Pair< SerializablePairWiseStitchingResult, Double > > > > fixedToMovingPairwiseErrors = new TreeMap<>();

		int numInvalidPairwiseShifts = 0;

		for ( final SerializablePairWiseStitchingResult pairwiseShift : pairwiseShifts )
		{
			if ( !pairwiseShift.getIsValidOverlap() )
			{
				++numInvalidPairwiseShifts;
				continue;
			}

			final double[] estimatedMovingSubTileMiddlePointInFixedTile = PairwiseTileOperations.mapMovingSubTileMiddlePointIntoFixedTile(
					pairwiseShift.getSubTilePair().toArray(),
					pairwiseShift.getEstimatedFullTileTransformPair().toArray(),
					pairwiseShift.getOffset()
				);

			// find corresponding subtiles in the groundtruth for the current pairwise item
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

			// find the pairwise error between the groundtruth offset and the estimated offset
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

			// add calculated pairwise error into the fixed->moving error mapping
			final SubTilePair subTilePair = pairwiseShift.getSubTilePair();
			final int fixedTileIndex = subTilePair.getFullTilePair().toArray()[ fixedIndex ].getIndex(), movingTileIndex = subTilePair.getFullTilePair().toArray()[ movingIndex ].getIndex();

			if ( !fixedToMovingPairwiseErrors.containsKey( fixedTileIndex ) )
				fixedToMovingPairwiseErrors.put( fixedTileIndex, new TreeMap<>() );

			if ( !fixedToMovingPairwiseErrors.get( fixedTileIndex ).containsKey( movingTileIndex ) )
				fixedToMovingPairwiseErrors.get( fixedTileIndex ).put( movingTileIndex, new ArrayList<>() );

			fixedToMovingPairwiseErrors.get( fixedTileIndex ).get( movingTileIndex ).add( new ValuePair<>( pairwiseShift, error ) );
		}


		double avgError = 0, maxError = 0;
		int numMatches = 0;
		for ( final Map< Integer, List< Pair< SerializablePairWiseStitchingResult, Double > > > movingTileErrors : fixedToMovingPairwiseErrors.values() )
		{
			for ( final List< Pair< SerializablePairWiseStitchingResult, Double > > subTilePairsAndErrors : movingTileErrors.values() )
			{
				for ( final Pair< SerializablePairWiseStitchingResult, Double > subTilePairAndError : subTilePairsAndErrors )
				{
					final double error = subTilePairAndError.getB();
					maxError = Math.max( error, maxError );
					avgError += error;
					++numMatches;
				}
			}
		}
		avgError /= numMatches;

		System.out.println( "Skipped " + numInvalidPairwiseShifts + " invalid shifts" );

		System.out.println( System.lineSeparator() + String.format( "avg.error=%.2fpx, max.error=%.2fpx", avgError, maxError ) );

		if ( parsedArgs.tileToInspect != null )
		{
			System.out.println( System.lineSeparator() + "Tile to inspect: " + parsedArgs.tileToInspect + getTileGridCoordinatesOrEmptyString( groundtruthTilesMap.get( parsedArgs.tileToInspect ) ) );
			for ( final Entry< Integer, Map< Integer, List< Pair< SerializablePairWiseStitchingResult, Double > > > > fixedToMovingPairwiseErrorsEntry : fixedToMovingPairwiseErrors.entrySet() )
			{
				final int fixedTileIndex = fixedToMovingPairwiseErrorsEntry.getKey();
				for ( final Entry< Integer, List< Pair< SerializablePairWiseStitchingResult, Double > > > movingPairwiseErrorsEntry : fixedToMovingPairwiseErrorsEntry.getValue().entrySet() )
				{
					final int movingTileIndex = movingPairwiseErrorsEntry.getKey();
					if ( fixedTileIndex == parsedArgs.tileToInspect.intValue() || movingTileIndex == parsedArgs.tileToInspect.intValue() )
					{
						System.out.println( movingPairwiseErrorsEntry.getValue().iterator().next().getA().getSubTilePair().getFullTilePair() + ":" );
						for ( final Pair< SerializablePairWiseStitchingResult, Double > movingPairwiseError : movingPairwiseErrorsEntry.getValue() )
							System.out.println( String.format( "  %s: %.2fpx   %s,%s (cr.corr=%.2f, variance=%.2f)",
									movingPairwiseError.getA().getSubTilePair(),
									movingPairwiseError.getB(),
									getTileGridCoordinatesOrEmptyString( groundtruthTilesMap.get( fixedTileIndex ) ),
									getTileGridCoordinatesOrEmptyString( groundtruthTilesMap.get( movingTileIndex ) ),
									movingPairwiseError.getA().getCrossCorrelation(),
									movingPairwiseError.getA().getVariance()
								) );
					}
				}
			}
		}
	}

	private static String getTileGridCoordinatesOrEmptyString( final TileInfo tile )
	{
		try
		{
			return " (" + Utils.getTileCoordinatesString( tile ) + ")";
		}
		catch ( final Exception e )
		{
			return "";
		}
	}
}
