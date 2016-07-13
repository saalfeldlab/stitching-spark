package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

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
		
		final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles = Utils.findOverlappingTiles( job.getTiles() );
		System.out.println( "Overlapping pairs count = " + overlappingTiles.size() );
		
		final StitchingParameters params = new StitchingParameters();
		params.dimensionality = job.getDimensionality();
		params.channel1 = 0;
		params.channel2 = 0;
		params.timeSelect = 0;
		params.checkPeaks = 5;
		params.computeOverlap = true;
		params.subpixelAccuracy = true;
		params.virtual = true;
		job.setParams( params );
		
		final SparkConf conf = new SparkConf().setAppName( "StitchingSpark" );
		final JavaSparkContext sparkContext = new JavaSparkContext( conf );
		stitchWithSpark( sparkContext, overlappingTiles );
		sparkContext.close();
		
		System.out.println( "done" );
	}

	private void stitchWithSpark( final JavaSparkContext sparkContext, final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles ) {
		
		final JavaRDD< Tuple2< TileInfo, TileInfo > > rdd = sparkContext.parallelize( overlappingTiles );
		final JavaRDD< SerializablePairWiseStitchingResult > pairwiseStitching = rdd.map(
				new Function< Tuple2< TileInfo, TileInfo >, SerializablePairWiseStitchingResult >() {
					
					private static final long serialVersionUID = -2907426581991906327L;

					@Override
					public SerializablePairWiseStitchingResult call( final Tuple2< TileInfo, TileInfo > pairOfTiles ) throws Exception {
						
						System.out.println( "Stitching tiles " + pairOfTiles._1.getIndex() + " and " + pairOfTiles._2.getIndex() + "..." );
						
						final ImageCollectionElement el1 = Utils.createElement( job, pairOfTiles._1 );
						final ImageCollectionElement el2 = Utils.createElement( job, pairOfTiles._2 );
						
						final ComparePair pair = new ComparePair(
								new ImagePlusTimePoint( el1.open( job.getParams().virtual ), el1.getIndex(), 1, el1.getModel(), el1 ),
								new ImagePlusTimePoint( el2.open( job.getParams().virtual ), el2.getIndex(), 1, el2.getModel(), el2 ) );
						
            			final Roi roi1 = Utils.getROI( pair.getTile1().getElement(), pair.getTile2().getElement() );
            			final Roi roi2 = Utils.getROI( pair.getTile2().getElement(), pair.getTile1().getElement() ); 
						
        				final PairWiseStitchingResult result = PairWiseStitchingImgLib.stitchPairwise(
        						pair.getImagePlus1(), pair.getImagePlus2(), roi1, roi2, pair.getTimePoint1(), pair.getTimePoint1(), job.getParams() );
        				
        				System.out.println( "Stitched tiles " + pairOfTiles._1.getIndex() + " and " + pairOfTiles._2.getIndex() + System.lineSeparator() +
        								"   CrossCorr=" + result.getCrossCorrelation() + ", PhaseCorr=" + result.getPhaseCorrelation() + ", RelShift=" + Arrays.toString( result.getOffset() ) );
        				
        		    	el1.close();
        		    	el2.close();
        				
        				return new SerializablePairWiseStitchingResult( pairOfTiles, result );
					}
				});
		
		final List< SerializablePairWiseStitchingResult > stitchedPairs = pairwiseStitching.collect();
		
		System.out.println( "Stitched all tiles pairwise, perform global optimization on them..." );
		
		// Create fake tile objects so that they don't hold any image data
		// required by the GlobalOptimization
		final TreeMap< Integer, Tile< ? > > tiles = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult pair : stitchedPairs ) {
			for ( final TileInfo tileInfo : new TileInfo[] { pair.getPairOfTiles()._1, pair.getPairOfTiles()._2 } ) {
				if ( !tiles.containsKey( tileInfo.getIndex() ) ) {
					try {
						final ImageCollectionElement el = Utils.createElement( job, tileInfo );
						final Tile< ? > tile = new ImagePlusTimePoint( null, el.getIndex(), 1, el.getModel(), el );
						tiles.put( tileInfo.getIndex(), tile );
					} catch ( final Exception e ) {
						e.printStackTrace();
					}
				}
			}
		}
	
		final Vector< ComparePair > comparePairs = new Vector<>();
		for ( final SerializablePairWiseStitchingResult pair : stitchedPairs ) {
			final ComparePair comparePair = new ComparePair( 
					(ImagePlusTimePoint)tiles.get( pair.getPairOfTiles()._1.getIndex() ), 
					(ImagePlusTimePoint)tiles.get( pair.getPairOfTiles()._2.getIndex() ) );

			comparePair.setRelativeShift( pair.getOffset() );
			comparePair.setCrossCorrelation( pair.getCrossCorrelation() );
			
			comparePairs.addElement( comparePair );
		}
		final ArrayList< ImagePlusTimePoint > optimized = GlobalOptimization.optimize( comparePairs, comparePairs.get( 0 ).getTile1(), job.getParams() );
		
		System.out.println( "Global optimization done" );
		
		// output the result
		System.out.println( "Results: " );
		for ( final ImagePlusTimePoint imt : optimized )
			System.out.println( imt.getImpId() + ": " + imt.getModel() );
		
		outputResults( optimized );
	}
	
	private void outputResults( final ArrayList< ImagePlusTimePoint > stitched ) {
		
		final TileInfo[] inputs = job.getTiles();
		final TileInfo[] results = new TileInfo[ inputs.length ];
		assert results.length == stitched.size();
		
		for ( int i = 0; i < stitched.size(); i++ ) {
			results[ i ] = new TileInfo();
			results[ i ].setFile( inputs[ i ].getFile() );
			results[ i ].setSize( inputs[ i ].getSize() );
			results[ i ].setIndex( i );
			
			final double[] pos = new double[ job.getDimensionality() ];
			stitched.get( i ).getModel().applyInPlace( pos );
			
			// TODO: avoid this stupid conversion
			final float[] posFloat = new float[ pos.length ];
			for ( int j = 0; j < pos.length; j++ )
				posFloat[ j ] = (float)pos[j];
			
			results[ i ].setPosition( posFloat );
		}
		
		try {
			job.saveTiles( results );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
}
