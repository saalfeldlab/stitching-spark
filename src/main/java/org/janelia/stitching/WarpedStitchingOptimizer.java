package org.janelia.stitching;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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

	private static final double INITIAL_MAX_ALLOWED_ERROR = 20;

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
		public final List< ImagePlusTimePoint > optimized;

		public final OptimizationParameters optimizationParameters;
		public final double retainedGraphRatio;
		public final double maxAllowedError;

		public final int remainingGraphSize;
		public final int remainingPairs;
		public final double maxDisplacement;
		public final double avgDisplacement;

		public final boolean translationOnlyStitching;
		public final int replacedTilesTranslation;

		public OptimizationResult(
				final List< ImagePlusTimePoint > optimized,
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
			this.optimized = optimized;
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
			if ( translationOnlyStitching != other.translationOnlyStitching && Math.min( remainingGraphSize, other.remainingGraphSize ) > 0 )
				throw new RuntimeException( "some tiles have translation model while some others have affine model" );

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

			// for the same graph characteristics, it is better when fewer tiles have simplified model
			if ( replacedTilesTranslation != other.replacedTilesTranslation )
				return Integer.compare( replacedTilesTranslation, other.replacedTilesTranslation );

			// if everything above is the same, the order is determined by smaller or higher error
			return Double.compare( maxDisplacement, other.maxDisplacement );
		}
	}

	private final C1WarpedStitchingJob job;
	private transient final JavaSparkContext sparkContext;

	public WarpedStitchingOptimizer( final C1WarpedStitchingJob job, final JavaSparkContext sparkContext )
	{
		this.job = job;
		this.sparkContext = sparkContext;
	}

	public void optimize() throws IOException
	{
		final DataProvider dataProvider = job.getDataProvider();

		final String basePath = job.getBasePath();
		final String pairwiseShiftsPath = PathResolver.get( basePath, "pairwise.json" );

		// FIXME: skip if solution already exists?
//		if ( Files.exists( Paths.get( Utils.addFilenameSuffix( pairwiseShiftsPath, "-used" ) ) ) )
//			return;

		final List< SerializablePairWiseStitchingResult > tileBoxShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwiseShiftsPath ) ) );

		try ( final OutputStream logOut = dataProvider.getOutputStream( URI.create( PathResolver.get( basePath, "optimizer.txt" ) ) ) )
		{
			try ( final PrintWriter logWriter = new PrintWriter( logOut ) )
			{
				logWriter.println( "Tiles total per channel: " + job.getTiles( 0 ).length );
				System.out.println( "Tiles total per channel: " + job.getTiles( 0 ).length );

				final double maxAllowedError = INITIAL_MAX_ALLOWED_ERROR;
				logWriter.println( "Set max allowed error to " + maxAllowedError + "px" );

				final OptimizationResult bestOptimization = findBestOptimization( tileBoxShifts, maxAllowedError, logWriter );

				// Update tile transforms
				for ( int channel = 0; channel < job.getChannels(); channel++ )
				{
					final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( job.getTiles( channel ) );

					final List< TileInfo > newTiles = new ArrayList<>();
					for ( final ImagePlusTimePoint optimizedTile : bestOptimization.optimized )
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

//					if ( newTiles.size() + optimizationPerformer.lostTiles.size() != tilesMap.size() )
//						throw new RuntimeException( "Number of stitched tiles does not match" );

					// sort the tiles by their index
					final TileInfo[] tilesToSave = Utils.createTilesMap( newTiles.toArray( new TileInfo[ 0 ] ) ).values().toArray( new TileInfo[ 0 ] );
//					TileOperations.translateTilesToOriginReal( tilesToSave );

					// save final tiles configuration
					TileInfoJSONProvider.saveTilesConfiguration(
							tilesToSave,
							dataProvider.getJsonWriter( URI.create( PathResolver.get(
									basePath,
									"ch" + channel + "-" + ( bestOptimization.translationOnlyStitching ? "translation" : "affine" ) + "-stitched.json"
								) ) )
						);
				}
			}
		}
	}

	private OptimizationResult findBestOptimization( final List< SerializablePairWiseStitchingResult > tileBoxShifts, final double maxAllowedError, final PrintWriter logWriter ) throws IOException
	{
		final List< OptimizationParameters > optimizationParametersList = new ArrayList<>();
		for ( double testMinCrossCorrelation = 0.1; testMinCrossCorrelation <= 1; testMinCrossCorrelation += 0.05 )
			for ( double testMinVariance = 0; testMinVariance <= 300; testMinVariance += 1 + ( int ) testMinVariance / 10 )
				optimizationParametersList.add( new OptimizationParameters( testMinCrossCorrelation, testMinVariance ) );

		final SerializableStitchingParameters stitchingParameters = job.getParams();
		final int tilesCount = job.getTiles( 0 ).length;

		final Broadcast< List< SerializablePairWiseStitchingResult > > broadcastedTileBoxShifts = sparkContext.broadcast( tileBoxShifts );

		GlobalOptimizationPerformer.suppressOutput();

		final List< OptimizationResult > optimizationResultList = new ArrayList<>( sparkContext.parallelize( optimizationParametersList, Math.min( optimizationParametersList.size(), MAX_PARTITIONS ) ).map( optimizationParameters ->
			{
				final Vector< ComparePointPair > comparePointPairs = createComparePointPairs( broadcastedTileBoxShifts.value(), optimizationParameters );

				final GlobalOptimizationPerformer optimizationPerformer = new GlobalOptimizationPerformer();
				GlobalOptimizationPerformer.suppressOutput();
				final List< ImagePlusTimePoint > optimized;

				optimized = optimizationPerformer.optimize( comparePointPairs, stitchingParameters );

				if ( optimizationPerformer.translationOnlyStitching && optimizationPerformer.replacedTilesTranslation != 0 )
					throw new RuntimeException( "some tiles have their models replaced while translation-only stitching mode was reported" );

				final OptimizationResult optimizationResult = new OptimizationResult(
						optimized,
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
				return optimizationResult;
			}
		).collect() );

		broadcastedTileBoxShifts.destroy();
		GlobalOptimizationPerformer.restoreOutput();

		optimizationResultList.removeAll( Collections.singleton( null ) );
		Collections.sort( optimizationResultList );

		if ( optimizationResultList.isEmpty() )
			throw new RuntimeException( "No results available" );

		if ( logWriter != null )
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

		return optimizationResultList.get( 0 );
	}

	private Vector< ComparePointPair > createComparePointPairs( final List< SerializablePairWiseStitchingResult > tileBoxShifts, final OptimizationParameters optimizationParameters )
	{
		// validate points
		for ( final SerializablePairWiseStitchingResult tileBoxShift : tileBoxShifts )
		{
			final double[] movingAdjustedPos = new double[ tileBoxShift.getNumDimensions() ];
			for ( int d = 0; d < movingAdjustedPos.length; ++d )
				movingAdjustedPos[ d ] = tileBoxShift.getPointPair().getB().getL()[ d ] - tileBoxShift.getPointPair().getA().getL()[ d ] + tileBoxShift.getOffset( d );
			for ( int d = 0; d < movingAdjustedPos.length; ++d )
				if ( Math.abs( movingAdjustedPos[ d ] ) > 1e-3 )
					throw new RuntimeException( "movingAdjustedPos = " + Arrays.toString( movingAdjustedPos ) );
		}

		// Create fake tile objects so that they don't hold any image data
		// required by the GlobalOptimization
		final TreeMap< Integer, ImagePlusTimePoint > fakeTileImagesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult tileBoxShift : tileBoxShifts )
		{
			for ( final TileInfo originalTileInfo : new TilePair( tileBoxShift.getTilePair().getA().getOriginalTile(), tileBoxShift.getTilePair().getB().getOriginalTile() ).toArray() )
			{
				if ( !fakeTileImagesMap.containsKey( originalTileInfo.getIndex() ) )
				{
					try
					{
						// --- FIXME: test translation-only solution
						final ImageCollectionElement el = Utils.createElementAffineModel( originalTileInfo );
//						final ImageCollectionElement el = Utils.createElementTranslationModel( originalTileInfo );
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

			comparePointPair.setPointPair( tileBoxShift.getPointPair().clone() );
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
}
