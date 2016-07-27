package org.janelia.stitching;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

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
import mpicbg.stitching.StitchingParameters;
import scala.Tuple2;
import stitching.utils.Log;

/**
 * @author pisarevi
 *
 */

public class StitchingSpark implements Runnable, Serializable {

	public static void main( String[] args ) {
		final StitchingArguments stitchingArgs = new StitchingArguments( args );
		if ( !stitchingArgs.parsedSuccessfully() )
			System.exit( 1 );
		
		StitchingSpark st = new StitchingSpark( stitchingArgs );
		st.run();
	}
	
	private static final long serialVersionUID = 6006962943789087537L;
	
	private StitchingArguments args;
	private StitchingJob job;
	private transient JavaSparkContext sparkContext;
	
	public StitchingSpark( final StitchingArguments args ) {
		this.args = args;
	}
	
	@Override
	public void run() {
		job = new StitchingJob( args );
		try {
			job.prepareTiles();
		} catch ( final Exception e ) {
			System.out.println( "Aborted: " + e.getMessage() );
			e.printStackTrace();
			System.exit( 2 );
		}
		
		sparkContext = new JavaSparkContext( new SparkConf().setAppName( "Stitching" ) );
		
		// Query metadata
		final ArrayList< TileInfo > tilesWithoutMetadata = new ArrayList<>();
		for ( final TileInfo tile : job.getTiles() )
			if ( tile.getSize() == null || tile.getType() == null )
				tilesWithoutMetadata.add( tile );
		
		if ( !tilesWithoutMetadata.isEmpty() )
			queryMetadata( tilesWithoutMetadata );
		
		if ( job.getMode() != StitchingJob.Mode.Metadata )
		{
			job.validateTiles();
			
			final StitchingParameters params = new StitchingParameters();
			params.dimensionality = job.getDimensionality();
			params.channel1 = 0;
			params.channel2 = 0;
			params.timeSelect = 0;
			params.checkPeaks = 5;
			params.computeOverlap = true;
			params.subpixelAccuracy = false;
			params.virtual = true;
			job.setParams( params );
			
			// Compute shifts
			if ( job.getMode() != StitchingJob.Mode.FuseOnly ) {
				final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles = TileHelper.findOverlappingTiles( job.getTiles() );
				computeShifts( overlappingTiles );
			}

			// Fuse
			if ( job.getMode() != StitchingJob.Mode.NoFuse ) {
				
				final Boundaries boundaries = TileHelper.getCollectionBoundaries( job.getTiles() );
				final ArrayList< TileInfo > subregions = TileHelper.divideSpace( boundaries, job.getSubregionSize() );
				fuse( subregions );
			}
		}
		
		sparkContext.close();
		System.out.println( "done" );
	}
	
	
	private void queryMetadata( final ArrayList< TileInfo > tilesWithoutMetadata ) 
	{
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( tilesWithoutMetadata );
		final JavaRDD< TileInfo > task = rdd.map(
				new Function< TileInfo, TileInfo >() 
				{	
					private static final long serialVersionUID = -4991255417353136684L;

					@Override
					public TileInfo call( final TileInfo tile ) throws Exception 
					{
						final ImageCollectionElement el = Utils.createElement( job, tile );
						final ImagePlus imp = el.open( true );
						
						// FIXME: workaround for misinterpreting slices as timepoints when no metadata is present 
						long[] size = Conversions.toLongArray( el.getDimensions() );
						if ( size.length == 2 && imp.getNFrames() > 1 )
							size = new long[] { size[ 0 ], size[ 1 ], imp.getNFrames() };
						
						tile.setType( ImageType.valueOf( imp.getType() ) );
						tile.setSize( size );
						
						el.close();
						return tile;
					}
				});
		
		final List< TileInfo > tilesMetadata = task.collect();
		
		System.out.println( "Obtained metadata for all tiles" );
		
		final TileInfo[] tiles = job.getTiles();
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tilesMap.put( tile.getIndex(), tile );
		
		for ( final TileInfo tileMetadata : tilesMetadata ) {
			final TileInfo tile = tilesMap.get( tileMetadata.getIndex() ); 
			tile.setType( tileMetadata.getType() );
			tile.setSize( tileMetadata.getSize() );
		}
		
		try {
			job.saveTiles( Utils.addFilenameSuffix( args.getInput(), "_full" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}

	
	private void computeShifts( final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles )
	{
		System.out.println( "Overlapping pairs count = " + overlappingTiles.size() );
		
		// Try to load precalculated shifts for some pairs of tiles
		final String pairwiseResultsFile = Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args.getInput(), "_full" ), "_pairwise" );
		final List< SerializablePairWiseStitchingResult > stitchedPairs = new ArrayList<>(); 
		try {
			System.out.println( "try to load pairwise results from disk" );
			stitchedPairs.addAll( job.loadPairwiseShifts( pairwiseResultsFile ) );
		} catch ( final FileNotFoundException e ) {
			System.out.println( "Pairwise results file not found" );
		} catch ( final NullPointerException e ) {
			System.out.println( "Pairwise results file is malformed" );
		}
		
		// Create a cache to efficiently lookup the existing pairs of tiles loaded from disk
		final TreeMap< Integer, TreeSet< Integer > > cache = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult result : stitchedPairs ) {
			final int firstIndex  =  Math.min( result.getPairOfTiles()._1.getIndex(), result.getPairOfTiles()._2.getIndex() ),
					  secondIndex =  Math.max( result.getPairOfTiles()._1.getIndex(), result.getPairOfTiles()._2.getIndex() );
			if ( !cache.containsKey( firstIndex ) )
				cache.put( firstIndex, new TreeSet< Integer >() );
			cache.get( firstIndex ).add( secondIndex );
		}
		
		// Remove pending pairs of tiles which were already processed
		for ( final Iterator< Tuple2< TileInfo, TileInfo > > it = overlappingTiles.iterator(); it.hasNext(); ) 
		{
			final Tuple2< TileInfo, TileInfo > pair = it.next();
			final int firstIndex  =  Math.min( pair._1.getIndex(), pair._2.getIndex() ),
					  secondIndex =  Math.max( pair._1.getIndex(), pair._2.getIndex() );
			if ( cache.containsKey( firstIndex ) && cache.get( firstIndex ).contains( secondIndex ) )
				it.remove();
		}

		if ( overlappingTiles.isEmpty() && !stitchedPairs.isEmpty() )
		{
			// If we're able to load precalculated pairwise results, save a lot of time by skipping this step and jump to the global optimization
			System.out.println( "Successfully loaded all pairwise results from disk!" );
		}
		else
		{
			stitchedPairs.addAll( computePairwiseShifts( overlappingTiles ) );
			
			System.out.println( "Stitched all tiles pairwise, perform global optimization on them" );
			
			try {
				System.out.println( "but first store this information on disk.." );
				job.savePairwiseShifts( stitchedPairs, pairwiseResultsFile );
			} catch ( final IOException e ) {
				e.printStackTrace();
			}
		}
		
		// Create fake tile objects so that they don't hold any image data
		// required by the GlobalOptimization
		final TreeMap< Integer, Tile< ? > > fakeTileImagesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult pair : stitchedPairs ) {
			for ( final TileInfo tileInfo : new TileInfo[] { pair.getPairOfTiles()._1, pair.getPairOfTiles()._2 } ) {
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
		for ( final SerializablePairWiseStitchingResult pair : stitchedPairs ) {
			final ComparePair comparePair = new ComparePair( 
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getPairOfTiles()._1.getIndex() ), 
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getPairOfTiles()._2.getIndex() ) );

			comparePair.setRelativeShift( pair.getOffset() );
			comparePair.setCrossCorrelation( pair.getCrossCorrelation() );
			
			comparePairs.addElement( comparePair );
		}
		final ArrayList< ImagePlusTimePoint > optimized = GlobalOptimization.optimize( comparePairs, comparePairs.get( 0 ).getTile1(), job.getParams() );
		
		System.out.println( "Global optimization done" );
		for ( final ImagePlusTimePoint imt : optimized )
			Log.info( imt.getImpId() + ": " + imt.getModel() );
		
		// Process the result updating the tiles position within the job object
		final TileInfo[] tiles = job.getTiles();
		assert tiles.length == optimized.size();
		
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tilesMap.put( tile.getIndex(), tile );
		
		for ( final ImagePlusTimePoint optimizedTile : optimized ) {
			final double[] pos = new double[ job.getDimensionality() ];
			optimizedTile.getModel().applyInPlace( pos );
			tilesMap.get( optimizedTile.getImpId() ).setPosition( pos );
		}
		
		try {
			job.saveTiles( Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args.getInput(), "_full" ), "_shifted" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
	
	private List< SerializablePairWiseStitchingResult > computePairwiseShifts( final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles )
	{
		final JavaRDD< Tuple2< TileInfo, TileInfo > > rdd = sparkContext.parallelize( overlappingTiles );
		final JavaRDD< SerializablePairWiseStitchingResult > pairwiseStitching = rdd.map(
				new Function< Tuple2< TileInfo, TileInfo >, SerializablePairWiseStitchingResult >() 
				{	
					private static final long serialVersionUID = -2907426581991906327L;

					@Override
					public SerializablePairWiseStitchingResult call( final Tuple2< TileInfo, TileInfo > pairOfTiles ) throws Exception 
					{
						final ImageCollectionElement el1 = Utils.createElement( job, pairOfTiles._1 );
						final ImageCollectionElement el2 = Utils.createElement( job, pairOfTiles._2 );
						
						final ComparePair pair = new ComparePair(
								new ImagePlusTimePoint( IJ.openImage( Utils.getAbsoluteImagePath( job, pairOfTiles._1 ) ), el1.getIndex(), 1, el1.getModel(), el1 ),
								new ImagePlusTimePoint( IJ.openImage( Utils.getAbsoluteImagePath( job, pairOfTiles._2 ) ), el2.getIndex(), 1, el2.getModel(), el2 ) );
						
            			final Roi roi1 = new Roi( TileHelper.getROI( pairOfTiles._1, pairOfTiles._2 ) );
            			final Roi roi2 = new Roi( TileHelper.getROI( pairOfTiles._2, pairOfTiles._1 ) );
						
        				final PairWiseStitchingResult result = PairWiseStitchingImgLib.stitchPairwise(
        						pair.getImagePlus1(), pair.getImagePlus2(), roi1, roi2, pair.getTimePoint1(), pair.getTimePoint2(), job.getParams() );
        				
        				System.out.println( "Stitched tiles " + pairOfTiles._1.getIndex() + " and " + pairOfTiles._2.getIndex() + System.lineSeparator() +
        								"   CrossCorr=" + result.getCrossCorrelation() + ", PhaseCorr=" + result.getPhaseCorrelation() + ", RelShift=" + Arrays.toString( result.getOffset() ) );
        				
        				pair.getImagePlus1().close();
        				pair.getImagePlus2().close();
        				
        				return new SerializablePairWiseStitchingResult( pairOfTiles, result );
					}
				});
		return pairwiseStitching.collect();
	}
	
	
	private void fuse( final ArrayList< TileInfo > subregions )
	{
		System.out.println( "There are " + subregions.size() + " subregions in total" );
		final String fusedFolder = job.getBaseFolder() + "/fused";
		new File( fusedFolder ).mkdirs();
		
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( subregions );
		final JavaRDD< TileInfo > fused = rdd.map(
				new Function< TileInfo, TileInfo >() 
				{
					private static final long serialVersionUID = 8324712817942470416L;

					@Override
					public TileInfo call( final TileInfo subregion ) throws Exception 
					{
						// TODO: optimize with KD interval tree or smth similar
						final ArrayList< TileInfo > tilesWithinSubregion = new ArrayList<>();
						for ( final TileInfo tile : job.getTiles() )
							if ( TileHelper.overlap( tile, subregion ) )
								tilesWithinSubregion.add( tile );
						
						if ( tilesWithinSubregion.isEmpty() )
							return null;
						
						subregion.setFile( fusedFolder + "/tile" + subregion.getIndex() + ".tif" );
						System.out.println( "Starting to fuse tiles within subregion " + subregion.getIndex() );
						
						final FusionPerformer fusion = new FusionPerformer( job );
						fusion.fuseTilesWithinSubregion( tilesWithinSubregion, subregion );
						
						System.out.println( "Completed for subregion " + subregion.getIndex() );
						
						return subregion;
					}
				});
		
		final List< TileInfo > output = fused.collect();
		output.removeAll( Collections.singleton( null ) );
		System.out.println( "Obtained " + output.size() + " tiles" );
		
		job.setTiles( output.toArray( new TileInfo[ 0 ] ) );
		
		try {
			job.saveTiles( Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args.getInput(), "_full" ), "_fused" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
}
