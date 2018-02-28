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

	private static final double INITIAL_MAX_ALLOWED_ERROR = 15;

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
		public final double score;

		public final int remainingGraphSize;
		public final int remainingPairs;
		public final double maxDisplacement;
		public final double avgDisplacement;

		public final int replacedTilesSimilarity;
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
				final int replacedTilesSimilarity,
				final int replacedTilesTranslation )
		{
			this.optimized = optimized;
			this.optimizationParameters = optimizationParameters;
			this.remainingGraphSize = remainingGraphSize;
			this.remainingPairs = remainingPairs;
			this.avgDisplacement = avgDisplacement;
			this.maxDisplacement = maxDisplacement;
			this.replacedTilesSimilarity = replacedTilesSimilarity;
			this.replacedTilesTranslation = replacedTilesTranslation;

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
									"ch" + channel + "-affine-stitched.json"
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

		final List< OptimizationResult > optimizationResultList = new ArrayList<>( sparkContext.parallelize( optimizationParametersList, Math.min( optimizationParametersList.size(), MAX_PARTITIONS ) ).map( optimizationParameters ->
			{
				final Vector< ComparePointPair > comparePointPairs = createComparePointPairs( broadcastedTileBoxShifts.value(), optimizationParameters );

				final GlobalOptimizationPerformer optimizationPerformer = new GlobalOptimizationPerformer();
				GlobalOptimizationPerformer.suppressOutput();
				final List< ImagePlusTimePoint > optimized;

				optimized = optimizationPerformer.optimize( comparePointPairs, stitchingParameters );

				// ignore this configuration if some tiles do not have enough matches
//				if ( optimizationPerformer.replacedTilesTranslation != 0 )
//					return null;

				final OptimizationResult optimizationResult = new OptimizationResult(
						optimized,
						maxAllowedError,
						optimizationParameters,
						tilesCount,
						optimizationPerformer.remainingGraphSize,
						optimizationPerformer.remainingPairs,
						optimizationPerformer.avgDisplacement,
						optimizationPerformer.maxDisplacement,
						optimizationPerformer.replacedTilesSimilarity,
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
			for ( final OptimizationResult optimizationResult : optimizationResultList )
				logWriter.println( "score=" + optimizationResult.score + ", graph=" + optimizationResult.remainingGraphSize + ", pairs=" + optimizationResult.remainingPairs + ", avg.error=" + optimizationResult.avgDisplacement + ", max.error=" + optimizationResult.maxDisplacement + ";  cross.corr=" + optimizationResult.optimizationParameters.minCrossCorrelation + ", variance=" + optimizationResult.optimizationParameters.minVariance + ";  tiles replaced to Similarity model=" + optimizationResult.replacedTilesSimilarity + ";  tiles replaced to Translation model=" + optimizationResult.replacedTilesTranslation );
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
