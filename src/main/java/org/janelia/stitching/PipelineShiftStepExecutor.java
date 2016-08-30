package org.janelia.stitching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.janelia.stitching.analysis.ThresholdEstimation;

import ij.IJ;
import ij.ImagePlus;
import ij.gui.Roi;
import mpicbg.models.Tile;
import mpicbg.stitching.ComparePair;
import mpicbg.stitching.GlobalOptimization;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
import mpicbg.stitching.PairWiseStitchingImgLib;
import mpicbg.stitching.PairWiseStitchingResult;
import stitching.utils.Log;

public class PipelineShiftStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -7152174064553332061L;

	public PipelineShiftStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run()
	{
		final ArrayList< TilePair > overlappingTiles = TileOperations.findOverlappingTiles( job.getTiles() );
		System.out.println( "Overlapping pairs count = " + overlappingTiles.size() );

		final List< SerializablePairWiseStitchingResult > pairwiseShifts = preparePairwiseShifts( overlappingTiles );

		double threshold = job.getArgs().crossCorrelationThreshold();
		if ( threshold < 0 )
			threshold = ThresholdEstimation.findOptimalThreshold( pairwiseShifts );
		System.out.println( "Cross correlation threshold value: " + threshold + ( job.getArgs().crossCorrelationThreshold() < 0 ? " (determined automatically)" : " (custom value)" ) );

		optimizeShifts( pairwiseShifts, threshold );
	}

	/**
	 * Tries to load precalculated pairwise shifts from disk to save computational time.
	 */
	private List< SerializablePairWiseStitchingResult > preparePairwiseShifts( final ArrayList< TilePair > overlappingTiles )
	{
		// Try to load precalculated shifts for some pairs of tiles
		final String pairwiseResultsFile = Utils.addFilenameSuffix( Utils.removeFilenameSuffix( job.getArgs().inputFilePath(), "_full" ), "_pairwise" );
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = new ArrayList<>();
		try {
			System.out.println( "try to load pairwise results from disk" );
			pairwiseShifts.addAll( TileInfoJSONProvider.loadPairwiseShifts( pairwiseResultsFile ) );
		} catch ( final FileNotFoundException e ) {
			System.out.println( "Pairwise results file not found" );
		} catch ( final NullPointerException e ) {
			System.out.println( "Pairwise results file is malformed" );
		}  catch ( final IOException e ) {
			e.printStackTrace();
		}

		// Create a cache to efficiently lookup the existing pairs of tiles loaded from disk
		final TreeMap< Integer, TreeSet< Integer > > cache = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult result : pairwiseShifts ) {
			final int firstIndex  =  Math.min( result.getTilePair().first().getIndex(), result.getTilePair().second().getIndex() ),
					secondIndex =  Math.max( result.getTilePair().first().getIndex(), result.getTilePair().second().getIndex() );
			if ( !cache.containsKey( firstIndex ) )
				cache.put( firstIndex, new TreeSet< Integer >() );
			cache.get( firstIndex ).add( secondIndex );
		}

		// Remove pending pairs of tiles which were already processed
		for ( final Iterator< TilePair > it = overlappingTiles.iterator(); it.hasNext(); )
		{
			final TilePair pair = it.next();
			final int firstIndex  =  Math.min( pair.first().getIndex(), pair.second().getIndex() ),
					secondIndex =  Math.max( pair.first().getIndex(), pair.second().getIndex() );
			if ( cache.containsKey( firstIndex ) && cache.get( firstIndex ).contains( secondIndex ) )
				it.remove();
		}

		if ( overlappingTiles.isEmpty() && !pairwiseShifts.isEmpty() )
		{
			// If we're able to load precalculated pairwise results, save some time skipping this step and jump to the global optimization
			System.out.println( "Successfully loaded all pairwise results from disk!" );
		}
		else
		{
			pairwiseShifts.addAll( computePairwiseShifts( overlappingTiles ) );

			try {
				System.out.println( "Stitched all tiles pairwise, store this information on disk.." );
				TileInfoJSONProvider.savePairwiseShifts( pairwiseShifts, pairwiseResultsFile );
			} catch ( final IOException e ) {
				e.printStackTrace();
			}
		}

		return pairwiseShifts;
	}

	/**
	 * Computes the best possible pairwise shifts between every pair of tiles on a Spark cluster.
	 * It uses phase correlation for measuring similarity between two images.
	 */
	private List< SerializablePairWiseStitchingResult > computePairwiseShifts( final ArrayList< TilePair > overlappingTiles )
	{
		if ( !job.getArgs().noRoi() )
			System.out.println( "*** Use ROIs as usual ***" );
		else
			System.out.println( "*** Compute phase correlation between full tile images instead of their ROIs ***" );

		final JavaRDD< TilePair > rdd = sparkContext.parallelize( overlappingTiles );
		final JavaRDD< SerializablePairWiseStitchingResult > pairwiseStitching = rdd.map(
				new Function< TilePair, SerializablePairWiseStitchingResult >()
				{
					private static final long serialVersionUID = -2907426581991906327L;

					@Override
					public SerializablePairWiseStitchingResult call( final TilePair pairOfTiles ) throws Exception
					{
						final ImageCollectionElement el1 = Utils.createElement( job, pairOfTiles.first() );
						final ImageCollectionElement el2 = Utils.createElement( job, pairOfTiles.second() );

						final int timepoint = 1;
						final ComparePair pair = new ComparePair(
								new ImagePlusTimePoint( IJ.openImage( Utils.getAbsoluteImagePath( job, pairOfTiles.first() ) ), el1.getIndex(), timepoint, el1.getModel(), el1 ),
								new ImagePlusTimePoint( IJ.openImage( Utils.getAbsoluteImagePath( job, pairOfTiles.second() ) ), el2.getIndex(), timepoint, el2.getModel(), el2 ) );

						Roi roi1 = null, roi2 = null;
						if ( !job.getArgs().noRoi() )
						{
							final Boundaries overlap1 = TileOperations.getOverlappingRegion( pairOfTiles.first(), pairOfTiles.second() );
							final Boundaries overlap2 = TileOperations.getOverlappingRegion( pairOfTiles.second(), pairOfTiles.first() );

							// mpicbg accepts only 2d rectangular ROIs
							roi1 = new Roi( overlap1.min( 0 ), overlap1.min( 1 ), overlap1.dimension( 0 ), overlap1.dimension( 1 ) );
							roi2 = new Roi( overlap2.min( 0 ), overlap2.min( 1 ), overlap2.dimension( 0 ), overlap2.dimension( 1 ) );
						}

						final double[] initialOffset = new double[ job.getDimensionality() ];
						for ( int d = 0; d < initialOffset.length; d++ )
							initialOffset[ d ] = pairOfTiles.second().getPosition( d ) - pairOfTiles.first().getPosition( d );

						final PairWiseStitchingResult result = PairWiseStitchingImgLib.stitchPairwise(
								pair.getImagePlus1(), pair.getImagePlus2(), roi1, roi2, pair.getTimePoint1(), pair.getTimePoint2(), job.getParams(), initialOffset, pairOfTiles.first().getSize() );

						System.out.println( "Stitched tiles " + pairOfTiles.first().getIndex() + " and " + pairOfTiles.second().getIndex() + System.lineSeparator() +
								"   CrossCorr=" + result.getCrossCorrelation() + ", PhaseCorr=" + result.getPhaseCorrelation() + ", RelShift=" + Arrays.toString( result.getOffset() ) );

						pair.getImagePlus1().close();
						pair.getImagePlus2().close();

						return new SerializablePairWiseStitchingResult( pairOfTiles, result );
					}
				});
		return pairwiseStitching.collect();
	}

	/**
	 * Finds final tile positions by globally optimizing pairwise shifts and stores updated tile configuration on the disk
	 */
	private void optimizeShifts( final List< SerializablePairWiseStitchingResult > pairwiseShifts, final double threshold )
	{
		job.getParams().regThreshold = threshold;

		// Create fake tile objects so that they don't hold any image data
		// required by the GlobalOptimization
		final TreeMap< Integer, Tile< ? > > fakeTileImagesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult pair : pairwiseShifts ) {
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
		for ( final SerializablePairWiseStitchingResult pair : pairwiseShifts ) {
			final ComparePair comparePair = new ComparePair(
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().first().getIndex() ),
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().second().getIndex() ) );

			comparePair.setRelativeShift( pair.getOffset() );
			comparePair.setCrossCorrelation( pair.getCrossCorrelation() );

			comparePairs.addElement( comparePair );
		}

		System.out.println( "Perform global optimization" );
		final ArrayList< ImagePlusTimePoint > optimized = new ArrayList<>();
		if ( !comparePairs.isEmpty() )
			optimized.addAll( GlobalOptimization.optimize( comparePairs, comparePairs.get( 0 ).getTile1(), job.getParams() ) );

		System.out.println( "Global optimization done" );
		for ( final ImagePlusTimePoint imt : optimized )
			Log.info( imt.getImpId() + ": " + imt.getModel() );

		// Update tile positions
		final Map< Integer, TileInfo > tilesMap = job.getTilesMap();
		for ( final ImagePlusTimePoint optimizedTile : optimized )
		{
			final double[] pos = new double[ job.getDimensionality() ];
			optimizedTile.getModel().applyInPlace( pos );
			tilesMap.get( optimizedTile.getImpId() ).setPosition( pos );
		}

		try {
			TileInfoJSONProvider.saveTilesConfiguration( job.getTiles(), Utils.addFilenameSuffix( Utils.removeFilenameSuffix( job.getArgs().inputFilePath(), "_full" ), "_shifted" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
}
