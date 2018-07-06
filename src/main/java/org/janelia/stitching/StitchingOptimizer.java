package org.janelia.stitching;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;

import ij.ImagePlus;
import mpicbg.models.Affine3D;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
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

	public static enum OptimizerMode implements Serializable
	{
		Translation,
		Affine
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
		public String usedPairwiseConfigurationPath;

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

	public void optimize( final int iteration, final OptimizerMode mode ) throws IOException
	{
		optimize( iteration, mode, null );
	}

	public void optimize( final int iteration, final OptimizerMode mode, final PrintWriter logWriter ) throws IOException
	{
		final DataProvider dataProvider = job.getDataProvider();

		final String basePath = PathResolver.getParent( job.getArgs().inputTileConfigurations().get( 0 ) );
		final String iterationDirPath = PathResolver.get( basePath, "iter" + iteration );
		final String pairwiseShiftsPath = PathResolver.get( iterationDirPath, "pairwise.json" );

		final List< SerializablePairWiseStitchingResult > tileBoxShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwiseShiftsPath ) ) );
		final double maxAllowedError = getMaxAllowedError( iteration );

		if ( logWriter != null )
		{
			logWriter.println( "Tiles total per channel: " + job.getTiles( 0 ).length );
			logWriter.println( "Stitching mode: " + mode );
			logWriter.println( "Set max allowed error to " + maxAllowedError + "px" );
		}

		final OptimizationResult bestOptimization = findBestOptimization(
				tileBoxShifts,
				maxAllowedError,
				logWriter,
				mode,
				iterationDirPath
			);

		// copy best resulting configuration to the base output path
		for ( final String bestTileConfigurationPath : bestOptimization.newTileConfigurationPaths )
		{
			job.getDataProvider().copyFile(
					URI.create( bestTileConfigurationPath ),
					URI.create( PathResolver.get( iterationDirPath, PathResolver.getFileName( bestTileConfigurationPath ) ) )
				);
		}

		// copy resulting used pairwise shifts configuration
		job.getDataProvider().copyFile(
				URI.create( bestOptimization.usedPairwiseConfigurationPath ),
				URI.create( PathResolver.get( iterationDirPath, PathResolver.getFileName( bestOptimization.usedPairwiseConfigurationPath ) ) )
			);
	}

	private OptimizationResult findBestOptimization(
			final List< SerializablePairWiseStitchingResult > tileBoxShifts,
			final double maxAllowedError,
			final PrintWriter logWriter,
			final OptimizerMode mode,
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

		final Broadcast< List< SerializablePairWiseStitchingResult > > broadcastedTileBoxShifts = sparkContext.broadcast( tileBoxShifts );

		final List< TileInfo[] > inputTileChannels = new ArrayList<>();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
			inputTileChannels.add( job.getTiles( channel ) );
		final Broadcast< List< TileInfo[] > > broadcastedInputTileChannels = sparkContext.broadcast( inputTileChannels );

		GlobalOptimizationPerformer.suppressOutput();

		final List< OptimizationResult > optimizationResults = new ArrayList<>( sparkContext
				.parallelize( optimizationParametersList, optimizationParametersList.size() )
				.map( optimizationParameters ->
					{
						final Vector< ComparePointPair > comparePointPairs = createComparePointPairs( broadcastedTileBoxShifts.value(), optimizationParameters, mode );

						GlobalOptimizationPerformer.suppressOutput();
						final GlobalOptimizationPerformer optimizationPerformer = new GlobalOptimizationPerformer();
						final List< ImagePlusTimePoint > optimizedTileConfiguration = optimizationPerformer.optimize( comparePointPairs, stitchingParameters );

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

						// save used pairwise configuration
						final List< SerializablePairWiseStitchingResult > usedPairwiseShifts = getUsedPairwiseConfiguration(
								optimizedTileConfiguration,
								broadcastedTileBoxShifts.value(),
								comparePointPairs
							);
						final String usedPairwiseShiftsPath = PathResolver.get(
								optimizedTileConfigurationFolder,
								Utils.addFilenameSuffix( "pairwise.json", "-used" )
							);
						TileInfoJSONProvider.savePairwiseShifts( usedPairwiseShifts, dataProviderLocal.getJsonWriter( URI.create( usedPairwiseShiftsPath ) ) );
						optimizationResult.usedPairwiseConfigurationPath = usedPairwiseShiftsPath;

						return optimizationResult;
					} )
				.collect()
			);

		GlobalOptimizationPerformer.restoreOutput();
		broadcastedTileBoxShifts.destroy();
		broadcastedInputTileChannels.destroy();

		Collections.sort( optimizationResults );

		if ( logWriter != null )
			logOptimizationResults( logWriter, optimizationResults );

		return optimizationResults.get( 0 );
	}

	private Vector< ComparePointPair > createComparePointPairs(
			final List< SerializablePairWiseStitchingResult > tileBoxShifts,
			final OptimizationParameters optimizationParameters,
			final OptimizerMode mode )
	{
		// create tile models
		final TreeMap< Integer, ImagePlusTimePoint > fakeTileImagesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult tileBoxShift : tileBoxShifts )
		{
			if ( tileBoxShift != null )
			{
				for ( final TileInfo originalTileInfo : tileBoxShift.getTileBoxPair().getOriginalTilePair().toArray() )
				{
					if ( !fakeTileImagesMap.containsKey( originalTileInfo.getIndex() ) )
					{
						try
						{
							final ImageCollectionElement el;
							switch ( mode )
							{
							case Translation:
								el = Utils.createElementTranslationModel( originalTileInfo );
								break;
							case Affine:
								el = Utils.createElementAffineModel( originalTileInfo );
								break;
							default:
								throw new UnsupportedOperationException( "stitching mode is not supported: " + mode );
							}

							final ImagePlus fakeImage = new ImagePlus( originalTileInfo.getIndex().toString(), ( java.awt.Image ) null );
							final ImagePlusTimePoint fakeTile = new ImagePlusTimePoint( fakeImage, el.getIndex(), 1, el.getModel(), el );
							fakeTileImagesMap.put( originalTileInfo.getIndex(), fakeTile );
						}
						catch ( final Exception e )
						{
							e.printStackTrace();
						}
					}
				}
			}
		}

		// create pairs
		final Vector< ComparePointPair > comparePairs = new Vector<>();
		for ( final SerializablePairWiseStitchingResult tileBoxShift : tileBoxShifts )
		{
			if ( tileBoxShift != null )
			{
				final ComparePointPair comparePointPair = new ComparePointPair(
						fakeTileImagesMap.get( tileBoxShift.getTileBoxPair().getOriginalTilePair().getA().getIndex() ),
						fakeTileImagesMap.get( tileBoxShift.getTileBoxPair().getOriginalTilePair().getB().getIndex() )
					);

				comparePointPair.setTileBoxPair( tileBoxShift.getTileBoxPair() );
				comparePointPair.setRelativeShift( tileBoxShift.getOffset() );
				comparePointPair.setCrossCorrelation( tileBoxShift.getCrossCorrelation() );
				comparePointPair.setIsValidOverlap(
						tileBoxShift.getIsValidOverlap()
						&& tileBoxShift.getCrossCorrelation() > optimizationParameters.minCrossCorrelation
						&& tileBoxShift.getVariance() != null && tileBoxShift.getVariance().doubleValue() > optimizationParameters.minVariance
					);

				comparePairs.addElement( comparePointPair );
			}
		}

		return comparePairs;
	}

	private TileInfo[] updateTileTransformations( final TileInfo[] tiles, final List< ImagePlusTimePoint > newTransformations )
	{
		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( tiles );
		final List< TileInfo > newTiles = new ArrayList<>();
		for ( final ImagePlusTimePoint optimizedTile : newTransformations )
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
				throw new RuntimeException( "tile is not in the input set" );
			}
		}

		// sort the tiles by their index
		return Utils.createTilesMap( newTiles.toArray( new TileInfo[ 0 ] ) ).values().toArray( new TileInfo[ 0 ] );
	}

	private List< SerializablePairWiseStitchingResult > getUsedPairwiseConfiguration(
			final List< ImagePlusTimePoint > resultingTiles,
			final List< SerializablePairWiseStitchingResult > tileBoxShifts,
			final Vector< ComparePointPair > comparePointPairs )
	{
		final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > initialShiftsMap = Utils.createTileBoxPairwiseShiftsMap( tileBoxShifts, false );
		final Set< ImagePlusTimePoint > resultingTileIndexes = new HashSet<>( resultingTiles );

		final List< SerializablePairWiseStitchingResult > usedPairwiseShifts = new ArrayList<>();
		for ( final ComparePointPair finalPair : comparePointPairs )
		{
			if ( finalPair.getIsValidOverlap() && resultingTileIndexes.contains( finalPair.getTile1() ) && resultingTileIndexes.contains( finalPair.getTile2() ) )
			{
				final SubdividedTileBoxPair tileBoxPair = finalPair.getTileBoxPair();

				final int ind1 = Math.min( tileBoxPair.getA().getIndex(), tileBoxPair.getB().getIndex() );
				final int ind2 = Math.max( tileBoxPair.getA().getIndex(), tileBoxPair.getB().getIndex() );
				final SerializablePairWiseStitchingResult initialShift = initialShiftsMap.get( ind1 ).get( ind2 );

				usedPairwiseShifts.add( initialShift );
			}
		}

		return usedPairwiseShifts;
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
