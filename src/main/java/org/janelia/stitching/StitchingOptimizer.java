package org.janelia.stitching;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import ij.ImagePlus;
import mpicbg.models.Affine3D;
import mpicbg.models.IllDefinedDataPointsException;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.Tile;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
import mpicbg.stitching.StitchingParameters;
import net.imglib2.realtransform.AffineTransform3D;

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
		final String basePath = Paths.get( job.getArgs().inputTileConfigurations().get( 0 ) ).getParent().toString();
		final String iterationDirname = "iter" + iteration;
		final String pairwiseShiftsPath = Paths.get( basePath, iterationDirname, "pairwise.json" ).toString();

		// FIXME: skip if solution already exists?
//		if ( Files.exists( Paths.get( Utils.addFilenameSuffix( pairwiseShiftsPath, "-used" ) ) ) )
//			return;

		final List< SerializablePairWiseStitchingResult[] > shifts = TileInfoJSONProvider.loadPairwiseShiftsMulti( pairwiseShiftsPath );

		try ( final PrintWriter logWriter = new PrintWriter( Paths.get( basePath, iterationDirname, "optimizer.txt" ).toString() ) )
		{
			logWriter.println( "Tiles total per channel: " + job.getTiles( 0 ).length );
			System.out.println( "Stitching iteration " + iteration + ": Tiles total per channel: " + job.getTiles( 0 ).length );

			final double maxAllowedError = getMaxAllowedError( iteration );
			logWriter.println( "Set max allowed error to " + maxAllowedError + "px" );

			final OptimizationParameters bestOptimizationParameters = findBestOptimizationParameters( shifts, maxAllowedError, logWriter );

			logWriter.println();
			logWriter.println( "Determined optimization parameters:  min.cross.correlation=" + bestOptimizationParameters.minCrossCorrelation + ", min.variance=" + bestOptimizationParameters.minVariance );
			System.out.println( "Stitching iteration " + iteration + ": Determined optimization parameters:  min.cross.correlation=" + bestOptimizationParameters.minCrossCorrelation + ", min.variance=" + bestOptimizationParameters.minVariance );

			final Vector< ComparePointPair > comparePointPairs = createComparePointPairs( shifts, bestOptimizationParameters );
			final GlobalOptimizationPerformer optimizationPerformer = new GlobalOptimizationPerformer();
			final List< ImagePlusTimePoint > optimized = optimizationPerformer.optimize( comparePointPairs, job.getParams(), logWriter );

			System.out.println();
			System.out.println("*********");
			System.out.println("tiles replaced to TranslationModel: " + optimizationPerformer.replacedTiles );
			System.out.println("*********");
			System.out.println();

			// Update tile transforms
			for ( int channel = 0; channel < job.getChannels(); channel++ )
			{
				final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( job.getTiles( channel ) );

				final List< TileInfo > newTiles = new ArrayList<>();
				for ( final ImagePlusTimePoint optimizedTile : optimized )
				{
					final Affine3D< ? > affineModel = ( Affine3D< ? > ) optimizedTile.getModel();
					final double[][] matrix = new double[ 3 ][ 4 ];
					affineModel.toMatrix( matrix );

					final AffineTransform3D tileTransform = new AffineTransform3D();
					tileTransform.set( matrix );

					if ( tilesMap.containsKey( optimizedTile.getImpId() ) )
					{
						final TileInfo newTile = tilesMap.get( optimizedTile.getImpId() ).clone();
						newTile.setTransform( tileTransform );
						newTiles.add( newTile );
					}
					else
					{
						throw new RuntimeException("tile is not in input set");
					}
				}

//				if ( newTiles.size() + optimizationPerformer.lostTiles.size() != tilesMap.size() )
//					throw new RuntimeException( "Number of stitched tiles does not match" );

				// sort the tiles by their index
				final TileInfo[] tilesToSave = Utils.createTilesMap( newTiles.toArray( new TileInfo[ 0 ] ) ).values().toArray( new TileInfo[ 0 ] );
//				TileOperations.translateTilesToOriginReal( tilesToSave );
				// save final tiles configuration
				TileInfoJSONProvider.saveTilesConfiguration(
						tilesToSave,
						Paths.get(
								basePath,
								iterationDirname,
								Utils.addFilenameSuffix( Paths.get( job.getArgs().inputTileConfigurations().get( channel ) ).getFileName().toString(), "-stitched" )
							).toString()
					);
			}

			// save final pairwise shifts configuration (pairs that have been used on the final step of the optimization routine)
			final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( job.getTiles( 0 ) );
			final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult[] > > initialShiftsMap = Utils.createPairwiseShiftsMultiMap( shifts, false );
			final List< SerializablePairWiseStitchingResult[] > usedPairwiseShifts = new ArrayList<>();
			final List< SerializablePairWiseStitchingResult[] > finalPairwiseShifts = new ArrayList<>();
			for ( final ComparePointPair comparePointPairFinal : comparePointPairs )
			{
				final TilePair tilePair = new TilePair( tilesMap.get( comparePointPairFinal.getTile1().getImpId() ), tilesMap.get( comparePointPairFinal.getTile2().getImpId() ) );
				if ( tilePair.getA() != null && tilePair.getB() != null )
				{
					final SerializablePairWiseStitchingResult finalShift = new SerializablePairWiseStitchingResult( tilePair, comparePointPairFinal.getRelativeShift(), comparePointPairFinal.getCrossCorrelation() );
					finalShift.setIsValidOverlap( comparePointPairFinal.getIsValidOverlap() );

					final int ind1 = Math.min( tilePair.getA().getIndex(), tilePair.getB().getIndex() );
					final int ind2 = Math.max( tilePair.getA().getIndex(), tilePair.getB().getIndex() );
					final SerializablePairWiseStitchingResult initialShift = initialShiftsMap.get( ind1 ).get( ind2 )[ 0 ];
					final double[] displacement = new double[ initialShift.getNumDimensions() ];
					for ( int d = 0; d < displacement.length; ++d )
						displacement[ d ] = ( tilePair.getB().getPosition( d ) - tilePair.getA().getPosition( d ) ) - initialShift.getOffset( d );
					finalShift.setDisplacement( displacement );

					finalShift.setVariance( initialShift.getVariance().doubleValue() );

					finalPairwiseShifts.add( new SerializablePairWiseStitchingResult[] { finalShift } );

					if ( comparePointPairFinal.getIsValidOverlap() )
						usedPairwiseShifts.add( new SerializablePairWiseStitchingResult[] { initialShift } );
				}
			}

			final String pairwiseInputFile = Paths.get( basePath, iterationDirname, "pairwise.json" ).toString();
			TileInfoJSONProvider.savePairwiseShiftsMulti( finalPairwiseShifts, Utils.addFilenameSuffix( pairwiseInputFile, "-stitched" ) );
			TileInfoJSONProvider.savePairwiseShiftsMulti( usedPairwiseShifts, Utils.addFilenameSuffix( pairwiseInputFile, "-used" ) );
		}
		catch ( final NotEnoughDataPointsException | IllDefinedDataPointsException e )
		{
			System.out.println( "Optimization failed:" );
			e.printStackTrace();
			throw new RuntimeException( e );
		}
	}

	private OptimizationParameters findBestOptimizationParameters( final List< SerializablePairWiseStitchingResult[] > shifts, final double maxAllowedError, final PrintWriter logWriter )
	{
		final List< OptimizationParameters > optimizationParametersList = new ArrayList<>();
		for ( double testMinCrossCorrelation = 0.1; testMinCrossCorrelation <= 1; testMinCrossCorrelation += 0.05 )
			for ( double testMinVariance = 0; testMinVariance <= 300; testMinVariance += 1 + ( int ) testMinVariance / 10 )
				optimizationParametersList.add( new OptimizationParameters( testMinCrossCorrelation, testMinVariance ) );

		final Broadcast< StitchingParameters > broadcastedStitchingParameters = sparkContext.broadcast( job.getParams() );

		final List< OptimizationResult > optimizationResultList = new ArrayList<>( sparkContext.parallelize( optimizationParametersList, optimizationParametersList.size() ).map( optimizationParameters ->
			{
				final Vector< ComparePointPair > comparePointPairs = createComparePointPairs( shifts, optimizationParameters );

				int validPairs = 0;
				for ( final ComparePointPair comparePointPair : comparePointPairs )
					if ( comparePointPair.getIsValidOverlap() )
						++validPairs;

				final GlobalOptimizationPerformer optimizationPerformer = new GlobalOptimizationPerformer();
				GlobalOptimizationPerformer.suppressOutput();
				optimizationPerformer.optimize( comparePointPairs, broadcastedStitchingParameters.value() );
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

		Collections.sort( optimizationResultList );

		if ( logWriter != null )
		{
			logWriter.println();
			logWriter.println( "Scanning parameter space for the optimizer: min.cross.correlation and min.variance:" );
			logWriter.println();
			for ( final OptimizationResult optimizationResult : optimizationResultList )
				logWriter.println( "score=" + optimizationResult.score + ", graph=" + optimizationResult.remainingGraphSize + ", pairs=" + optimizationResult.remainingPairs + ", avg.error=" + optimizationResult.avgDisplacement + ", max.error=" + optimizationResult.maxDisplacement + ";  cross.corr=" + optimizationResult.optimizationParameters.minCrossCorrelation + ", variance=" + optimizationResult.optimizationParameters.minVariance );
		}

		return optimizationResultList.get( 0 ).optimizationParameters;
	}

	private Vector< ComparePointPair > createComparePointPairs( final List< SerializablePairWiseStitchingResult[] > shifts, final OptimizationParameters optimizationParameters )
	{
		// validate points
		for ( final SerializablePairWiseStitchingResult[] pairMulti : shifts )
		{
			for ( final SerializablePairWiseStitchingResult pair : pairMulti )
			{
				final double[] movingAdjustedPos = new double[ pair.getNumDimensions() ];
				for ( int d = 0; d < movingAdjustedPos.length; ++d )
					movingAdjustedPos[ d ] = pair.getPointPair().getB().getL()[ d ] - pair.getPointPair().getA().getL()[ d ] + pair.getOffset( d );
				for ( int d = 0; d < movingAdjustedPos.length; ++d )
					if ( Math.abs( movingAdjustedPos[ d ] ) > 1e-10 )
						throw new RuntimeException( "movingAdjustedPos = " + Arrays.toString( movingAdjustedPos ) );
			}
		}

		// Create fake tile objects so that they don't hold any image data
		// required by the GlobalOptimization
		final TreeMap< Integer, Tile< ? > > fakeTileImagesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult[] pairMulti : shifts ) {
			final SerializablePairWiseStitchingResult pair = pairMulti[ 0 ];
			for ( final TileInfo tileInfo : pair.getTilePair().toArray() ) {
				if ( !fakeTileImagesMap.containsKey( tileInfo.getIndex() ) ) {
					try {
						final ImageCollectionElement el = Utils.createElementSimilarityModel( job, tileInfo );
						final ImagePlus fakeImage = new ImagePlus( tileInfo.getIndex().toString(), (java.awt.Image)null );
						final Tile< ? > fakeTile = new ImagePlusTimePoint( fakeImage, el.getIndex(), 1, el.getModel(), el );
						fakeTileImagesMap.put( tileInfo.getIndex(), fakeTile );
					} catch ( final Exception e ) {
						e.printStackTrace();
					}
				}
			}
		}

		// create pairs
		final Vector< ComparePointPair > comparePairs = new Vector<>();
		for ( final SerializablePairWiseStitchingResult[] pairMulti : shifts )
		{
			for ( final SerializablePairWiseStitchingResult pair : pairMulti )
			{
				final ComparePointPair comparePointPair = new ComparePointPair(
						(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().getA().getIndex() ),
						(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().getB().getIndex() ) );

				comparePointPair.setPointPair( pair.getPointPair() );
				comparePointPair.setRelativeShift( pair.getOffset() == null ? null : pair.getOffset().clone() );
				comparePointPair.setCrossCorrelation( pair.getCrossCorrelation() );
				comparePointPair.setIsValidOverlap(
						pair.getIsValidOverlap()
						&& pair.getCrossCorrelation() > optimizationParameters.minCrossCorrelation
						&& pair.getVariance() != null && pair.getVariance().doubleValue() > optimizationParameters.minVariance
					);

				comparePairs.addElement( comparePointPair );
			}
		}

		return comparePairs;
	}
}
