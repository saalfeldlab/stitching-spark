package org.janelia.stitching;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;

import com.google.gson.GsonBuilder;

import mpicbg.models.IndexedTile;
import mpicbg.models.Model;
import net.imglib2.realtransform.AffineTransform3D;

public class StitchingOptimizer implements Serializable
{
	private static final long serialVersionUID = -3873876669757549452L;

	public static enum OptimizerMode implements Serializable
	{
		AFFINE,
		SIMILARITY,
		RIGID,
		TRANSLATION
	}

	public static enum RegularizerType implements Serializable
	{
		SIMILARITY,
		RIGID,
		TRANSLATION,
		NONE
	}

	private static final class OptimizationParameters implements Serializable
	{
		private static final long serialVersionUID = -8853813205822652603L;

		public final double minCrossCorrelation;
		public final double minVariance;

		public OptimizationParameters( final double minCrossCorrelation, final double minVariance )
		{
			this.minCrossCorrelation = minCrossCorrelation;
			this.minVariance = minVariance;
		}
	}

	private static final class OptimizationResult implements Comparable< OptimizationResult >, Serializable
	{
		private static final long serialVersionUID = -3606758225118852918L;

		public final OptimizationParameters optimizationParameters;
		public final double retainedGraphRatio;
		public final double maxAllowedError;

		public final int remainingGraphSize;
		public final int remainingPairs;
		public final double maxDisplacement;
		public final double avgDisplacement;

		public final boolean translationOnlyStitching;
		public final int replacedTilesTranslation;

		public List< String > newTileConfigurationPaths;
		public String usedPairwiseIndexesPath;

		public OptimizationResult(
				final double maxAllowedError,
				final OptimizationParameters optimizationParameters,
				final int fullGraphSize,
				final int remainingGraphSize,
				final int remainingPairs,
				final double avgDisplacement,
				final double maxDisplacement,
				final boolean translationOnlyStitching,
				final int replacedTilesTranslation )
		{
			this.maxAllowedError = maxAllowedError;
			this.optimizationParameters = optimizationParameters;
			this.remainingGraphSize = remainingGraphSize;
			this.remainingPairs = remainingPairs;
			this.avgDisplacement = avgDisplacement;
			this.maxDisplacement = maxDisplacement;
			this.translationOnlyStitching = translationOnlyStitching;
			this.replacedTilesTranslation = replacedTilesTranslation;

			retainedGraphRatio = ( double ) remainingGraphSize / fullGraphSize;
		}

		/**
		 * Defines descending sorting order.
		 */
		@Override
		public int compareTo( final OptimizationResult other )
		{
			// if both are above the error threshold, the order is determined by the resulting graph size and max.error
			if ( maxDisplacement > maxAllowedError && other.maxDisplacement > other.maxAllowedError )
			{
				if ( remainingGraphSize != other.remainingGraphSize )
					return -Integer.compare( remainingGraphSize, other.remainingGraphSize );

				return Double.compare( maxDisplacement, other.maxDisplacement );
			}
			// otherwise, the one that is above the error threshold goes last
			else if ( maxDisplacement > maxAllowedError )
			{
				return 1;
			}
			else if ( other.maxDisplacement > other.maxAllowedError )
			{
				return -1;
			}

			// both are within the accepted error range

			// better if the resulting graph is larger
			if ( remainingGraphSize != other.remainingGraphSize )
				return -Integer.compare( remainingGraphSize, other.remainingGraphSize );

			/*
			// better when the resulting graph has more edges
			if ( remainingPairs != other.remainingPairs )
				return -Integer.compare( remainingPairs, other.remainingPairs );
			*/

			/*
			// for the same graph characteristics, it is better when fewer tiles have simplified model
			if ( replacedTilesTranslation != other.replacedTilesTranslation )
				return Integer.compare( replacedTilesTranslation, other.replacedTilesTranslation );
			*/

			// if everything above is the same, the order is determined by smaller or higher error
			return Double.compare( maxDisplacement, other.maxDisplacement );
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
		optimize( iteration, null );
	}

	public void optimize( final int iteration, final PrintWriter logWriter ) throws IOException
	{
		final OptimizerMode optimizerMode = job.getArgs().optimizerMode();
		final DataProvider dataProvider = job.getDataProvider();

		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String iterationDirPath = PathResolver.get( basePath, "iter" + iteration );
		final String pairwiseFilename = "pairwise.json";
		final String pairwiseShiftsPath = PathResolver.get( iterationDirPath, pairwiseFilename );

		final List< SerializablePairWiseStitchingResult > subTilePairwiseResults = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwiseShiftsPath ) ) );
		final double maxAllowedError = getMaxAllowedError( iteration );

		if ( logWriter != null )
			printStats( logWriter, optimizerMode, maxAllowedError );

		final OptimizationResult bestOptimization = findBestOptimization(
				subTilePairwiseResults,
				maxAllowedError,
				logWriter,
				optimizerMode,
				iterationDirPath
			);

		// copy best resulting configuration to the base output path
		for ( final String bestTileConfigurationPath : bestOptimization.newTileConfigurationPaths )
		{
			dataProvider.copyFile(
					URI.create( bestTileConfigurationPath ),
					URI.create( PathResolver.get( iterationDirPath, PathResolver.getFileName( bestTileConfigurationPath ) ) )
				);
		}

		// save resulting used pairwise shifts configuration (load the file with indexes and extract the actual shifts from the existing pairwise file)
		final PairwiseShiftsIndexFilter usedPairwiseShiftsIndexes = loadPairwiseShiftsIndexesFromFile( dataProvider, bestOptimization.usedPairwiseIndexesPath );
		final List< SerializablePairWiseStitchingResult > usedPairwiseShifts = usedPairwiseShiftsIndexes.filterPairwiseShifts( subTilePairwiseResults );
		final String usedPairwiseShiftsPath = PathResolver.get( iterationDirPath, Utils.addFilenameSuffix( pairwiseFilename, "-used" ) );
		TileInfoJSONProvider.savePairwiseShifts( usedPairwiseShifts, dataProvider.getJsonWriter( URI.create( usedPairwiseShiftsPath ) ) );
	}

	private double getMaxAllowedError( final int iteration )
	{
		return Math.min( job.getArgs().initialMaxAllowedError() + iteration * job.getArgs().maxAllowedErrorStep(), job.getArgs().maxAllowedErrorLimit() );
	}

	private void printStats( final PrintWriter logWriter, final OptimizerMode optimizerMode, final double maxAllowedError )
	{
		logWriter.println( "Tiles total per channel: " + job.getTiles( 0 ).length );
		logWriter.println( "Optimizer mode: " + optimizerMode );
		if ( optimizerMode != OptimizerMode.TRANSLATION )
		{
			logWriter.println( "Regularizer type: " + job.getArgs().regularizerType() );
			logWriter.println( "Regularizer lambda: " + job.getArgs().regularizerLambda() );
		}
		logWriter.println( "Set max allowed error to " + maxAllowedError + "px" );
	}

	private OptimizationResult findBestOptimization(
			final List< SerializablePairWiseStitchingResult > subTilePairwiseResults,
			final double maxAllowedError,
			final PrintWriter logWriter,
			final OptimizerMode optimizerMode,
			final String iterationDirPath ) throws IOException
	{
		final List< OptimizationParameters > optimizationParametersList = new ArrayList<>();
		for ( double testMinCrossCorrelation = 0.1; testMinCrossCorrelation <= 1; testMinCrossCorrelation += 0.05 )
			for ( double testMinVariance = 0; testMinVariance <= 300; testMinVariance += 1 + ( int ) testMinVariance / 10 )
				optimizationParametersList.add( new OptimizationParameters( testMinCrossCorrelation, testMinVariance ) );

		final SerializableStitchingParameters stitchingParameters = job.getParams();
		final int tilesCount = job.getTiles( 0 ).length;

		final String resultingConfigsDirPath = PathResolver.get( iterationDirPath, "resulting-tile-configurations" );
		job.getDataProvider().createFolder( URI.create( resultingConfigsDirPath ) );

		final Broadcast< List< SerializablePairWiseStitchingResult > > broadcastedSubTilePairwiseResults = sparkContext.broadcast( subTilePairwiseResults );

		final List< TileInfo[] > inputTileChannels = new ArrayList<>();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
			inputTileChannels.add( job.getTiles( channel ) );
		final Broadcast< List< TileInfo[] > > broadcastedInputTileChannels = sparkContext.broadcast( inputTileChannels );

		final List< OptimizationResult > optimizationResults = new ArrayList<>( sparkContext
				.parallelize( optimizationParametersList, optimizationParametersList.size() )
				.map( optimizationParameters ->
					{
						final List< SubTilePairwiseMatch > subTilePairwiseMatches = createSubTilePairwiseMatches(
								broadcastedSubTilePairwiseResults.value(),
								optimizationParameters,
								optimizerMode
							);

						final GlobalOptimizationPerformer optimizationPerformer = new GlobalOptimizationPerformer();
						final List< IndexedTile< ? > > optimizedTileConfiguration = optimizationPerformer.optimize( subTilePairwiseMatches, stitchingParameters );

						final OptimizationResult optimizationResult = new OptimizationResult(
								maxAllowedError,
								optimizationParameters,
								tilesCount,
								optimizationPerformer.remainingGraphSize,
								optimizationPerformer.remainingPairs,
								optimizationPerformer.avgDisplacement,
								optimizationPerformer.maxDisplacement,
								optimizationPerformer.translationOnlyStitching,
								optimizationPerformer.replacedTilesTranslation
							);

						final String optimizedTileConfigurationFolder = PathResolver.get(
								resultingConfigsDirPath,
								String.format(
										"cross.corr=%.2f,variance=%.2f",
										optimizationResult.optimizationParameters.minCrossCorrelation,
										optimizationResult.optimizationParameters.minVariance
									)
							);
						final DataProvider dataProviderLocal = job.getDataProvider();
						dataProviderLocal.createFolder( URI.create( optimizedTileConfigurationFolder ) );

						// save resulting tile configuration for each channel
						optimizationResult.newTileConfigurationPaths = new ArrayList<>();
						for ( int channel = 0; channel < job.getChannels(); channel++ )
						{
							final TileInfo[] newChannelTiles = updateTileTransformations( broadcastedInputTileChannels.value().get( channel ), optimizedTileConfiguration );
							final String newTileConfigurationPath = PathResolver.get(
									optimizedTileConfigurationFolder,
									Utils.addFilenameSuffix(
											PathResolver.getFileName( job.getArgs().inputTileConfigurations().get( channel ) ),
											"-stitched"
										)
								);
							TileInfoJSONProvider.saveTilesConfiguration( newChannelTiles, dataProviderLocal.getJsonWriter( URI.create( newTileConfigurationPath ) ) );
							optimizationResult.newTileConfigurationPaths.add( newTileConfigurationPath );
						}

						// save used pairwise indexes (saving the actual used pairwise config for each threshold combination might be too expensive, just save the indexes and then extract the actual shifts only for the best result)
						final PairwiseShiftsIndexFilter usedPairwiseShiftsIndexes = getUsedPairwiseShiftsIndexFilter(
								optimizedTileConfiguration,
								broadcastedSubTilePairwiseResults.value(),
								subTilePairwiseMatches
							);
						optimizationResult.usedPairwiseIndexesPath = PathResolver.get( optimizedTileConfigurationFolder, "pairwise-used-indexes.json" );
						savePairwiseShiftsIndexesToFile( usedPairwiseShiftsIndexes, dataProviderLocal, optimizationResult.usedPairwiseIndexesPath );

						return optimizationResult;
					} )
				.collect()
			);

		broadcastedSubTilePairwiseResults.destroy();
		broadcastedInputTileChannels.destroy();

		Collections.sort( optimizationResults );

		if ( logWriter != null )
			logOptimizationResults( logWriter, optimizationResults );

		return optimizationResults.get( 0 );
	}

	private < M extends Model< M > > List< SubTilePairwiseMatch > createSubTilePairwiseMatches(
			final List< SerializablePairWiseStitchingResult > subTilePairwiseResults,
			final OptimizationParameters optimizationParameters,
			final OptimizerMode optimizerMode )
	{
		// create tile models
		final TreeMap< Integer, IndexedTile< M > > tileModelsMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult subTilePairwiseResult : subTilePairwiseResults )
		{
			if ( subTilePairwiseResult != null )
			{
				for ( final TileInfo originalTileInfo : subTilePairwiseResult.getSubTilePair().getFullTilePair().toArray() )
				{
					if ( !tileModelsMap.containsKey( originalTileInfo.getIndex() ) )
					{
						final M model = createTileModel(
								originalTileInfo.numDimensions(),
								optimizerMode,
								job.getArgs().regularizerType(),
								job.getArgs().regularizerLambda()
							);
						tileModelsMap.put( originalTileInfo.getIndex(), new IndexedTile<>( model, originalTileInfo.getIndex() ) );
					}
				}
			}
		}

		// create pairwise matches
		final List< SubTilePairwiseMatch > subTilePairwiseMatches = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult subTilePairwiseResult : subTilePairwiseResults )
		{
			if ( subTilePairwiseResult != null && subTilePairwiseResult.getIsValidOverlap() )
			{
				// add pair only if above the specified thresholds for this configuration
				if ( subTilePairwiseResult.getCrossCorrelation() >= optimizationParameters.minCrossCorrelation && subTilePairwiseResult.getVariance().doubleValue() >= optimizationParameters.minVariance )
				{
					final SubTilePairwiseMatch subTilePairwiseMatch = new SubTilePairwiseMatch(
							tileModelsMap.get( subTilePairwiseResult.getSubTilePair().getA().getFullTile().getIndex() ),
							tileModelsMap.get( subTilePairwiseResult.getSubTilePair().getB().getFullTile().getIndex() ),
							subTilePairwiseResult
						);
					subTilePairwiseMatches.add( subTilePairwiseMatch );
				}
			}
		}

		return subTilePairwiseMatches;
	}

	private < M extends Model< M > > M createTileModel(
			final int numDimensions,
			final OptimizerMode optimizerMode,
			final RegularizerType regularizerType,
			final double regularizerLambda )
	{
		final M model;
		switch ( optimizerMode )
		{
		case AFFINE:
			model = TileModelFactory.createAffineModel( numDimensions );
			break;
		case SIMILARITY:
			model = TileModelFactory.createSimilarityModel( numDimensions );
			break;
		case RIGID:
			model = TileModelFactory.createRigidModel( numDimensions );
			break;
		case TRANSLATION:
			model = TileModelFactory.createTranslationModel( numDimensions );
			break;
		default:
			throw new IllegalArgumentException( "optimizer mode not supported: " + optimizerMode );
		}

		final M regularizer;
		switch ( regularizerType )
		{
		case SIMILARITY:
			regularizer = TileModelFactory.createSimilarityModel( numDimensions );
			break;
		case RIGID:
			regularizer = TileModelFactory.createRigidModel( numDimensions );
			break;
		case TRANSLATION:
			regularizer = TileModelFactory.createTranslationModel( numDimensions );
			break;
		case NONE:
			regularizer = null;
			break;
		default:
			throw new IllegalArgumentException( "regularizer type not supported: " + regularizerType );
		}

		return ( regularizer != null ? TileModelFactory.createInterpolatedModel( numDimensions, model, regularizer, regularizerLambda ) : model );
	}

	private TileInfo[] updateTileTransformations( final TileInfo[] tiles, final List< IndexedTile< ? > > newTransformations )
	{
		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( tiles );
		final List< TileInfo > newTiles = new ArrayList<>();
		for ( final IndexedTile< ? > resultingTile : newTransformations )
		{
			if ( tilesMap.containsKey( resultingTile.getIndex() ) )
			{
				final AffineTransform3D tileTransform = TransformUtils.getModelTransform( resultingTile.getModel() );
				final TileInfo newTile = tilesMap.get( resultingTile.getIndex() ).clone();
				newTile.setTransform( tileTransform );
				newTiles.add( newTile );
			}
			else
			{
				throw new RuntimeException( "tile is not in the input set" );
			}
		}
		// sort the tiles by their index
		return Utils.createTilesMap( newTiles.toArray( new TileInfo[ 0 ] ) ).values().toArray( new TileInfo[ 0 ] );
	}

	private PairwiseShiftsIndexFilter getUsedPairwiseShiftsIndexFilter(
			final List< IndexedTile< ? > > resultingTiles,
			final List< SerializablePairWiseStitchingResult > subTilePairwiseResults,
			final List< SubTilePairwiseMatch > subTilePairwiseMatches )
	{
		final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > initialShiftsMap = Utils.createSubTilePairwiseResultsMap( subTilePairwiseResults, false );
		final Set< Integer > resultingTileIndexes = new HashSet<>();
		for ( final IndexedTile< ? > resultingTile : resultingTiles )
			resultingTileIndexes.add( resultingTile.getIndex() );

		final List< SerializablePairWiseStitchingResult > filteredPairwiseShifts = new ArrayList<>();
		for ( final SubTilePairwiseMatch finalPair : subTilePairwiseMatches )
		{
			if ( resultingTileIndexes.contains( finalPair.getFixedTile().getIndex() ) && resultingTileIndexes.contains( finalPair.getMovingTile().getIndex() ) )
			{
				final SubTilePair subTilePair = finalPair.getPairwiseResult().getSubTilePair();
				final int ind1 = Math.min( subTilePair.getA().getIndex(), subTilePair.getB().getIndex() );
				final int ind2 = Math.max( subTilePair.getA().getIndex(), subTilePair.getB().getIndex() );
				final SerializablePairWiseStitchingResult initialShift = initialShiftsMap.get( ind1 ).get( ind2 );
				filteredPairwiseShifts.add( initialShift );
			}
		}

		return new PairwiseShiftsIndexFilter( filteredPairwiseShifts );
	}

	private void savePairwiseShiftsIndexesToFile( final PairwiseShiftsIndexFilter pairwiseShiftsIndexes, final DataProvider dataProvider, final String filePath ) throws IOException
	{
		try ( final Writer gsonWriter = dataProvider.getJsonWriter( URI.create( filePath ) ) )
		{
			gsonWriter.write( new GsonBuilder().create().toJson( pairwiseShiftsIndexes ) );
		}
	}

	private PairwiseShiftsIndexFilter loadPairwiseShiftsIndexesFromFile( final DataProvider dataProvider, final String filePath ) throws IOException
	{
		try ( final Reader gsonReader = dataProvider.getJsonReader( URI.create( filePath ) ) )
		{
			return new GsonBuilder().create().fromJson( gsonReader, PairwiseShiftsIndexFilter.class );
		}
	}

	private void logOptimizationResults( final PrintWriter logWriter, final List< OptimizationResult > optimizationResultList )
	{
		logWriter.println();
		logWriter.println( "Scanning parameter space for the optimizer: min.cross.correlation and min.variance:" );
		logWriter.println();
		boolean aboveErrorThreshold = false;
		for ( final OptimizationResult optimizationResult : optimizationResultList )
		{
			if ( optimizationResult.maxDisplacement > optimizationResult.maxAllowedError && !aboveErrorThreshold )
			{
				// first item that is above the specified error threshold
				aboveErrorThreshold = true;
				logWriter.println();
				logWriter.println( "--------------- Max.error above threshold ---------------" );
			}

			final String modelSpecificationLog;
			if ( optimizationResult.remainingGraphSize != 0 )
			{
				if ( optimizationResult.translationOnlyStitching )
				{
					modelSpecificationLog = ";  translation-only stitching";
				}
				else
				{
					modelSpecificationLog =
							";  higher-order model stitching" +
							"; tiles replaced to Translation model=" + optimizationResult.replacedTilesTranslation;
				}
			}
			else
			{
				modelSpecificationLog = "";
			}

			logWriter.println(
					"retainedGraphRatio=" + String.format( "%.2f", optimizationResult.retainedGraphRatio ) +
					", graph=" + optimizationResult.remainingGraphSize +
					", pairs=" + optimizationResult.remainingPairs +
					",  avg.error=" + String.format( "%.2f", optimizationResult.avgDisplacement ) +
					", max.error=" + String.format( "%.2f", optimizationResult.maxDisplacement ) +
					";  cross.corr=" + String.format( "%.2f", optimizationResult.optimizationParameters.minCrossCorrelation ) +
					", variance=" + String.format( "%.2f", optimizationResult.optimizationParameters.minVariance ) +
					modelSpecificationLog
				);
		}
	}
}
