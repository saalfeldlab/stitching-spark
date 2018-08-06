package org.janelia.stitching;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;

import ij.ImagePlus;
import mpicbg.models.Tile;
import mpicbg.stitching.ComparePair;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
import mpicbg.stitching.StitchingParameters;

public class StitchingOptimizer implements Serializable
{
	private static final long serialVersionUID = -3873876669757549452L;

	private static final double MAX_ALLOWED_ERROR_LIMIT = 30;
	private static final double INITIAL_MAX_ALLOWED_ERROR = 5;
	private static final double MAX_ALLOWED_ERROR_STEP = 5;

	private static double getMaxAllowedError( final int iteration )
	{
		return Math.min( INITIAL_MAX_ALLOWED_ERROR + iteration * MAX_ALLOWED_ERROR_STEP, MAX_ALLOWED_ERROR_LIMIT );
	}

	private static final class OptimizationParameters
	{
		public final double minCrossCorrelation;
		public final double minVariance;

		public OptimizationParameters( final double minCrossCorrelation, final double minVariance )
		{
			this.minCrossCorrelation = minCrossCorrelation;
			this.minVariance = minVariance;
		}
	}

	private static final class OptimizationResult implements Comparable< OptimizationResult >
	{
		public final OptimizationParameters optimizationParameters;
		public final double score;

		public final int remainingGraphSize;
		public final int remainingPairs;
		public final double maxDisplacement;
		public final double avgDisplacement;

		public OptimizationResult(
				final double maxAllowedError,
				final OptimizationParameters optimizationParameters,
				final int fullGraphSize,
				final int remainingGraphSize,
				final int remainingPairs,
				final double avgDisplacement,
				final double maxDisplacement )
		{
			this.optimizationParameters = optimizationParameters;
			this.remainingGraphSize = remainingGraphSize;
			this.remainingPairs = remainingPairs;
			this.avgDisplacement = avgDisplacement;
			this.maxDisplacement = maxDisplacement;

			final double remainingGraphToFullGraphRatio = ( double ) remainingGraphSize / fullGraphSize;
			assert remainingGraphToFullGraphRatio >= 0 && remainingGraphToFullGraphRatio <= 0;

//			assert maxDisplacement >= 0;
//			final double displacementScore = 1 - maxDisplacement / MAX_DISPLACEMENT_LIMIT;
//
//			score = remainingGraphToFullGraphRatio * displacementScore;

			score = remainingGraphSize == 0 || maxDisplacement > maxAllowedError ? Double.NEGATIVE_INFINITY : remainingGraphToFullGraphRatio;
		}

		/*
		 * Defines descending sorting order based on score
		 */
		@Override
		public int compareTo( final OptimizationResult other )
		{
			return Double.compare( other.score, score );
		}
	}

	private final StitchingJob job;
	private transient final JavaSparkContext sparkContext;

	public StitchingOptimizer( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		this.job = job;
		this.sparkContext = sparkContext;
	}

	public void optimize( final int iteration ) throws IOException
	{
		final DataProvider dataProvider = job.getDataProvider();

		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String iterationDirname = "iter" + iteration;
		final String pairwiseShiftsPath = PathResolver.get( basePath, iterationDirname, "pairwise.json" );

		// FIXME: skip if solution already exists?
//		if ( Files.exists( Paths.get( Utils.addFilenameSuffix( pairwiseShiftsPath, "-used" ) ) ) )
//			return;

		final List< SerializablePairWiseStitchingResult[] > shifts = TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( pairwiseShiftsPath ) );

		try ( final OutputStream logOut = dataProvider.getOutputStream( PathResolver.get( basePath, iterationDirname, "optimizer.txt" ) ) )
		{
			try ( final PrintWriter logWriter = new PrintWriter( logOut ) )
			{
				logWriter.println( "Tiles total per channel: " + job.getTiles( 0 ).length );
				System.out.println( "Stitching iteration " + iteration + ": Tiles total per channel: " + job.getTiles( 0 ).length );

				final double maxAllowedError = getMaxAllowedError( iteration );
				logWriter.println( "Set max allowed error to " + maxAllowedError + "px" );

				final OptimizationParameters bestOptimizationParameters = findBestOptimizationParameters( shifts, maxAllowedError, logWriter );

				logWriter.println();
				logWriter.println( "Determined optimization parameters:  min.cross.correlation=" + bestOptimizationParameters.minCrossCorrelation + ", min.variance=" + bestOptimizationParameters.minVariance );
				System.out.println( "Stitching iteration " + iteration + ": Determined optimization parameters:  min.cross.correlation=" + bestOptimizationParameters.minCrossCorrelation + ", min.variance=" + bestOptimizationParameters.minVariance );

				final Vector< ComparePair > comparePairs = createComparePairs( shifts, bestOptimizationParameters );
				final GlobalOptimizationPerformer optimizationPerformer = new GlobalOptimizationPerformer();
				final List< ImagePlusTimePoint > optimized = optimizationPerformer.optimize( comparePairs, job.getParams(), null, logWriter );

				// Update tile positions
				for ( int channel = 0; channel < job.getChannels(); channel++ )
				{
					final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( job.getTiles( channel ) );

					final List< TileInfo > newTiles = new ArrayList<>();
					for ( final ImagePlusTimePoint optimizedTile : optimized )
					{
						final double[] pos = new double[ job.getDimensionality() ];
						optimizedTile.getModel().applyInPlace( pos );

						if ( tilesMap.containsKey( optimizedTile.getImpId() ) )
						{
							final TileInfo newTile = tilesMap.get( optimizedTile.getImpId() ).clone();
							newTile.setPosition( pos );
							newTiles.add( newTile );
						}
					}

	//				if ( newTiles.size() + optimizationPerformer.lostTiles.size() != tilesMap.size() )
	//					throw new RuntimeException( "Number of stitched tiles does not match" );

					// sort the tiles by their index
					final TileInfo[] tilesToSave = Utils.createTilesMap( newTiles.toArray( new TileInfo[ 0 ] ) ).values().toArray( new TileInfo[ 0 ] );
					TileOperations.translateTilesToOriginReal( tilesToSave );

					// save final tiles configuration
					final String stitchedConfigFile = PathResolver.get(
							basePath,
							iterationDirname,
							Utils.addFilenameSuffix( PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( channel ) ), "-stitched" )
						);
					TileInfoJSONProvider.saveTilesConfiguration( tilesToSave, dataProvider.getJsonWriter( stitchedConfigFile ) );
				}

				// save final pairwise shifts configuration (pairs that have been used on the final step of the optimization routine)
				final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( job.getTiles( 0 ) );
				final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult[] > > initialShiftsMap = Utils.createPairwiseShiftsMultiMap( shifts, false );
				final List< SerializablePairWiseStitchingResult[] > usedPairwiseShifts = new ArrayList<>();
				final List< SerializablePairWiseStitchingResult[] > finalPairwiseShifts = new ArrayList<>();
				for ( final ComparePair finalPair : comparePairs )
				{
					final TilePair tilePair = new TilePair( tilesMap.get( finalPair.getTile1().getImpId() ), tilesMap.get( finalPair.getTile2().getImpId() ) );
					if ( tilePair.getA() != null && tilePair.getB() != null )
					{
						final SerializablePairWiseStitchingResult finalShift = new SerializablePairWiseStitchingResult( tilePair, finalPair.getRelativeShift(), finalPair.getCrossCorrelation() );
						finalShift.setIsValidOverlap( finalPair.getIsValidOverlap() );

						final int ind1 = Math.min( tilePair.getA().getIndex(), tilePair.getB().getIndex() );
						final int ind2 = Math.max( tilePair.getA().getIndex(), tilePair.getB().getIndex() );
						final SerializablePairWiseStitchingResult initialShift = initialShiftsMap.get( ind1 ).get( ind2 )[ 0 ];
						final double[] displacement = new double[ initialShift.getNumDimensions() ];
						for ( int d = 0; d < displacement.length; ++d )
							displacement[ d ] = ( tilePair.getB().getPosition( d ) - tilePair.getA().getPosition( d ) ) - initialShift.getOffset( d );
						finalShift.setDisplacement( displacement );

						finalShift.setVariance( initialShift.getVariance().doubleValue() );

						finalPairwiseShifts.add( new SerializablePairWiseStitchingResult[] { finalShift } );

						if ( finalPair.getIsValidOverlap() )
							usedPairwiseShifts.add( new SerializablePairWiseStitchingResult[] { initialShift } );
					}
				}

				final String pairwiseInputFile = PathResolver.get( basePath, iterationDirname, "pairwise.json" );
				TileInfoJSONProvider.savePairwiseShiftsMulti( finalPairwiseShifts, dataProvider.getJsonWriter( Utils.addFilenameSuffix( pairwiseInputFile, "-stitched" ) ) );
				TileInfoJSONProvider.savePairwiseShiftsMulti( usedPairwiseShifts, dataProvider.getJsonWriter( Utils.addFilenameSuffix( pairwiseInputFile, "-used" ) ) );
			}
		}
	}

	private OptimizationParameters findBestOptimizationParameters( final List< SerializablePairWiseStitchingResult[] > shifts, final double maxAllowedError, final PrintWriter logWriter )
	{
		final List< OptimizationParameters > optimizationParametersList = new ArrayList<>();
		for ( double testMinCrossCorrelation = 0.1; testMinCrossCorrelation <= 1; testMinCrossCorrelation += 0.05 )
			for ( double testMinVariance = 0; testMinVariance <= 300; testMinVariance += 1 + ( int ) testMinVariance / 10 )
				optimizationParametersList.add( new OptimizationParameters( testMinCrossCorrelation, testMinVariance ) );

		final Broadcast< StitchingParameters > broadcastedStitchingParameters = sparkContext.broadcast( job.getParams() );

		final boolean noLeaves = job.getArgs().noLeaves();

		GlobalOptimizationPerformer.suppressOutput();

		final List< OptimizationResult > optimizationResultList = new ArrayList<>( sparkContext.parallelize( optimizationParametersList, optimizationParametersList.size() ).map( optimizationParameters ->
			{
				final Vector< ComparePair > comparePairs = createComparePairs( shifts, optimizationParameters );

				if ( noLeaves && hasLeaves( comparePairs ) )
					return null;

				int validPairs = 0;
				for ( final ComparePair pair : comparePairs )
					if ( pair.getIsValidOverlap() )
						++validPairs;

				final GlobalOptimizationPerformer optimizationPerformer = new GlobalOptimizationPerformer();
				optimizationPerformer.optimize( comparePairs, broadcastedStitchingParameters.value() );
				final OptimizationResult optimizationResult = new OptimizationResult(
						maxAllowedError,
						optimizationParameters,
						job.getTiles( 0 ).length,
						optimizationPerformer.remainingGraphSize,
						validPairs,
						optimizationPerformer.avgDisplacement,
						optimizationPerformer.maxDisplacement );
				return optimizationResult;
			}
		).collect() );

		GlobalOptimizationPerformer.restoreOutput();
		broadcastedStitchingParameters.destroy();

		if ( noLeaves )
		{
			final int sizeBefore = optimizationResultList.size();
			optimizationResultList.removeAll( Collections.singleton( null ) );
			final int sizeAfter = optimizationResultList.size();
			System.out.println( "" );
			System.out.println( "-----------" );
			System.out.println( "no leaves mode ON" );
			System.out.println( "" );
			System.out.println( "pairs before: " + sizeBefore );
			System.out.println( "pairs after: " + sizeAfter );
			System.out.println( "-----------" );
			System.out.println( "" );
		}

		Collections.sort( optimizationResultList );

		if ( logWriter != null )
		{
			logWriter.println();
			logWriter.println( "Scanning parameter space for the optimizer: min.cross.correlation and min.variance:" );
			logWriter.println();
			for ( final OptimizationResult optimizationResult : optimizationResultList )
				logWriter.println(
						"score=" + String.format( "%.2f", optimizationResult.score ) +
						", graph=" + optimizationResult.remainingGraphSize +
						", pairs=" + optimizationResult.remainingPairs +
						", avg.error=" + String.format( "%.2f", optimizationResult.avgDisplacement ) +
						", max.error=" + String.format( "%.2f", optimizationResult.maxDisplacement ) +
						";  cross.corr=" + String.format( "%.2f", optimizationResult.optimizationParameters.minCrossCorrelation ) +
						", variance=" + String.format( "%.2f", optimizationResult.optimizationParameters.minVariance )
					);
		}

		return optimizationResultList.get( 0 ).optimizationParameters;
	}

	private Vector< ComparePair > createComparePairs( final List< SerializablePairWiseStitchingResult[] > shifts, final OptimizationParameters optimizationParameters )
	{
		// Create fake tile objects so that they don't hold any image data
		// required by the GlobalOptimization
		final TreeMap< Integer, Tile< ? > > fakeTileImagesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult[] pairMulti : shifts ) {
			final SerializablePairWiseStitchingResult pair = pairMulti[ 0 ];
			for ( final TileInfo tileInfo : pair.getTilePair().toArray() ) {
				if ( !fakeTileImagesMap.containsKey( tileInfo.getIndex() ) ) {
					try {
						final ImageCollectionElement el = Utils.createElement( job, tileInfo );
						final ImagePlus fakeImage = new ImagePlus( tileInfo.getIndex().toString(), (java.awt.Image)null );
						final Tile< ? > fakeTile = new ImagePlusTimePoint( fakeImage, el.getIndex(), 1, el.getModel(), el );
						fakeTileImagesMap.put( tileInfo.getIndex(), fakeTile );
					} catch ( final Exception e ) {
						e.printStackTrace();
					}
				}
			}
		}

		final Vector< ComparePair > comparePairs = new Vector<>();
		for ( final SerializablePairWiseStitchingResult[] pairMulti : shifts )
		{
			for ( final SerializablePairWiseStitchingResult pair : pairMulti )
			{
				final ComparePair comparePair = new ComparePair(
						(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().getA().getIndex() ),
						(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().getB().getIndex() ) );

				comparePair.setRelativeShift( pair.getOffset() == null ? null : pair.getOffset().clone() );
				comparePair.setCrossCorrelation( pair.getCrossCorrelation() );
				comparePair.setIsValidOverlap(
						pair.getIsValidOverlap()
						&& pair.getCrossCorrelation() > optimizationParameters.minCrossCorrelation
						&& pair.getVariance() != null && pair.getVariance().doubleValue() > optimizationParameters.minVariance
					);

				comparePairs.addElement( comparePair );
			}
		}

		return comparePairs;
	}

	private boolean hasLeaves( final Vector< ComparePair > comparePairs )
	{
		final Map< Integer, Set< Integer > > connections = new TreeMap<>();
		for ( final ComparePair comparePair : comparePairs )
		{
			if ( !comparePair.getIsValidOverlap() )
				continue;

			final int[] ind = new int[] { comparePair.getTile1().getImpId(), comparePair.getTile2().getImpId() };
			for ( int i = 0; i < 2; ++i )
			{
				if ( !connections.containsKey( ind[ i ] ) )
					connections.put( ind[ i ], new TreeSet<>() );
				connections.get( ind[ i ] ).add( ind[ ( i + 1 ) % 2 ] );
			}
		}
		for ( final Set< Integer > value : connections.values() )
			if ( value.size() <= 1 )
				return true;
		return false;
	}
}
