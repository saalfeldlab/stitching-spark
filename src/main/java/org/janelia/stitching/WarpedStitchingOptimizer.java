package org.janelia.stitching;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

public class WarpedStitchingOptimizer implements Serializable
{
	private static final long serialVersionUID = -3873876669757549452L;

	private static final int MAX_PARTITIONS = 15000;

	private static final class OptimizationParameters implements Serializable
	{
		private static final long serialVersionUID = 1283476717226257206L;

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
		private static final long serialVersionUID = 3867586355408111480L;

		private final C1WarpedStitchingArguments args;

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

		public OptimizationResult(
				final C1WarpedStitchingArguments args,
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
			this.args = args;
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

		/*
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

			// better when the resulting graph has more edges
			if ( remainingPairs != other.remainingPairs )
				return -Integer.compare( remainingPairs, other.remainingPairs );

			// for the same graph characteristics, it is better when fewer tiles have simplified model
			if ( args.affineOnlyStitching() && replacedTilesTranslation != other.replacedTilesTranslation )
				return Integer.compare( replacedTilesTranslation, other.replacedTilesTranslation );

			// if everything above is the same, the order is determined by smaller or higher error
			return Double.compare( maxDisplacement, other.maxDisplacement );
		}
	}

	private final transient JavaSparkContext sparkContext;
	private final C1WarpedStitchingJob job;

	public WarpedStitchingOptimizer( final JavaSparkContext sparkContext, final C1WarpedStitchingJob job )
	{
		this.sparkContext = sparkContext;
		this.job = job;
	}

	public void optimize() throws IOException
	{
		final DataProvider dataProvider = job.getDataProvider();

		final String basePath = job.getBasePath();
		final String pairwiseShiftsPath = PathResolver.get( basePath, "pairwise.json" );

		final List< SerializablePairWiseStitchingResult > tileBoxShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwiseShiftsPath ) ) );

		try ( final OutputStream logOut = dataProvider.getOutputStream( URI.create( PathResolver.get( basePath, "optimizer.txt" ) ) ) )
		{
			try ( final PrintWriter logWriter = new PrintWriter( logOut ) )
			{
				logWriter.println( "Tiles total per channel: " + job.getTiles( 0 ).length );
				System.out.println( "Tiles total per channel: " + job.getTiles( 0 ).length );

				final double maxAllowedError = job.getArgs().maxStitchingError();
				logWriter.println( "Set max allowed error to " + maxAllowedError + "px" );

				final OptimizationResult bestOptimization = findBestOptimization( tileBoxShifts, maxAllowedError, logWriter );

				// copy best resulting configuration to the base output path
				for ( final String bestTileConfigurationPath : bestOptimization.newTileConfigurationPaths )
				{
					job.getDataProvider().copyFile(
							URI.create( bestTileConfigurationPath ),
							URI.create( PathResolver.get( basePath, PathResolver.getFileName( bestTileConfigurationPath ) ) )
						);
				}
			}
		}
	}

	private OptimizationResult findBestOptimization(
			final List< SerializablePairWiseStitchingResult > tileBoxShifts,
			final double maxAllowedError,
			final PrintWriter logWriter ) throws IOException
	{
		if ( job.getArgs().translationOnlyStitching() && job.getArgs().affineOnlyStitching() )
			throw new IllegalArgumentException( "translationOnly & affineOnly are not allowed at the same time" );

		final List< OptimizationParameters > optimizationParametersList = new ArrayList<>();
		for ( double testMinCrossCorrelation = 0.1; testMinCrossCorrelation <= 1; testMinCrossCorrelation += 0.05 )
			for ( double testMinVariance = 0; testMinVariance <= 300; testMinVariance += 1 + ( int ) testMinVariance / 10 )
				optimizationParametersList.add( new OptimizationParameters( testMinCrossCorrelation, testMinVariance ) );

		final SerializableStitchingParameters stitchingParameters = job.getParams();
		final int tilesCount = job.getTiles( 0 ).length;

		final String resultingConfigBasePath = PathResolver.get( job.getBasePath(), "resulting-tile-configurations" );
		job.getDataProvider().createFolder( URI.create( resultingConfigBasePath ) );

		final Broadcast< List< SerializablePairWiseStitchingResult > > broadcastedTileBoxShifts = sparkContext.broadcast( tileBoxShifts );
		GlobalOptimizationPerformer.suppressOutput();

		final List< TileInfo[] > inputTileChannels = new ArrayList<>();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
			inputTileChannels.add( job.getTiles( channel ) );
		final Broadcast< List< TileInfo[] > > broadcastedInputTileChannels = sparkContext.broadcast( inputTileChannels );

		final List< OptimizationResult > optimizationResults = new ArrayList<>( sparkContext
			.parallelize( optimizationParametersList, optimizationParametersList.size() )
			.map( optimizationParameters ->
				{
					final Vector< ComparePointPair > comparePointPairs = createComparePointPairs( broadcastedTileBoxShifts.value(), optimizationParameters );

					GlobalOptimizationPerformer.suppressOutput();
					final GlobalOptimizationPerformer optimizationPerformer = new GlobalOptimizationPerformer();
					final List< ImagePlusTimePoint > optimizedTileConfiguration = optimizationPerformer.optimize( comparePointPairs, stitchingParameters );

					final OptimizationResult optimizationResult = new OptimizationResult(
							job.getArgs(),
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
							resultingConfigBasePath,
							String.format(
									"cross.corr=%.2f,variance=%.2f",
									optimizationResult.optimizationParameters.minCrossCorrelation,
									optimizationResult.optimizationParameters.minVariance
								)
						);
					final DataProvider dataProviderLocal = job.getDataProvider();
					dataProviderLocal.createFolder( URI.create( optimizedTileConfigurationFolder ) );

					optimizationResult.newTileConfigurationPaths = new ArrayList<>();
					for ( int channel = 0; channel < job.getChannels(); channel++ )
					{
						final TileInfo[] newChannelTiles = updateTileTransformations( broadcastedInputTileChannels.value().get( channel ), optimizedTileConfiguration );
						final String newTileConfigurationPath = PathResolver.get(
								optimizedTileConfigurationFolder,
								String.format( "ch%d-%s-stitched.json", channel, optimizationResult.translationOnlyStitching ? "translation" : "affine" )
							);
						TileInfoJSONProvider.saveTilesConfiguration( newChannelTiles, dataProviderLocal.getJsonWriter( URI.create( newTileConfigurationPath ) ) );
						optimizationResult.newTileConfigurationPaths.add( newTileConfigurationPath );
					}

					return optimizationResult;
				}
			)
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

	private Vector< ComparePointPair > createComparePointPairs( final List< SerializablePairWiseStitchingResult > tileBoxShifts, final OptimizationParameters optimizationParameters )
	{
		// create tile models
		final TreeMap< Integer, ImagePlusTimePoint > fakeTileImagesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult tileBoxShift : tileBoxShifts )
		{
			for ( final TileInfo originalTileInfo : new TilePair( tileBoxShift.getTilePair().getA().getOriginalTile(), tileBoxShift.getTilePair().getB().getOriginalTile() ).toArray() )
			{
				if ( !fakeTileImagesMap.containsKey( originalTileInfo.getIndex() ) )
				{
					try
					{
						final ImageCollectionElement el;

						if ( job.getArgs().translationOnlyStitching() )
							el = Utils.createElementTranslationModel( originalTileInfo );
						else
							el = Utils.createElementAffineModel( originalTileInfo );

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

		// create pairs
		final Vector< ComparePointPair > comparePairs = new Vector<>();
		for ( final SerializablePairWiseStitchingResult tileBoxShift : tileBoxShifts )
		{
			final ComparePointPair comparePointPair = new ComparePointPair(
					fakeTileImagesMap.get( tileBoxShift.getTilePair().getA().getOriginalTile().getIndex() ),
					fakeTileImagesMap.get( tileBoxShift.getTilePair().getB().getOriginalTile().getIndex() )
				);

			comparePointPair.setTileBoxPair( tileBoxShift.getTilePair() );
			comparePointPair.setRelativeShift( tileBoxShift.getOffset() == null ? null : tileBoxShift.getOffset().clone() );
			comparePointPair.setCrossCorrelation( tileBoxShift.getCrossCorrelation() );
			comparePointPair.setIsValidOverlap(
					tileBoxShift.getIsValidOverlap()
					&& tileBoxShift.getCrossCorrelation() > optimizationParameters.minCrossCorrelation
					&& tileBoxShift.getVariance() != null && tileBoxShift.getVariance().doubleValue() > optimizationParameters.minVariance
				);

			comparePairs.addElement( comparePointPair );
		}

		return comparePairs;
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
}
