package org.janelia.stitching;

import java.awt.Rectangle;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import ij.gui.Roi;
import mpicbg.models.Model;
import mpicbg.models.Tile;
import mpicbg.models.TranslationModel2D;
import mpicbg.models.TranslationModel3D;
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

public class StitchingSpark {
	
	static StitchingParameters params;
	
	public static void stitchWithSpark( 
			final JavaSparkContext sc, 
			final StitchingJob job, 
			final StitchingParameters paramsLocal, 
			final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles ) {
		
		params = paramsLocal;
		
		final JavaRDD< Tuple2< TileInfo, TileInfo > > rdd = sc.parallelize( overlappingTiles );
		final JavaRDD< SerializablePairWiseStitchingResult > pairwiseStitching = rdd.map(
				new Function< Tuple2< TileInfo, TileInfo >, SerializablePairWiseStitchingResult >() {
					
					private static final long serialVersionUID = -2907426581991906327L;

					@Override
					public SerializablePairWiseStitchingResult call( Tuple2< TileInfo, TileInfo > pairOfTiles ) throws Exception {
						
						System.out.println( "Stitching tiles " + pairOfTiles._1.getIndex() + " and " + pairOfTiles._2.getIndex() + "...");
						
						final ImageCollectionElement el1 = IJUtil.createElement( job, pairOfTiles._1 );
						final ImageCollectionElement el2 = IJUtil.createElement( job, pairOfTiles._2 );
						
						final ComparePair pair = new ComparePair(
								new ImagePlusTimePoint( el1.open( params.virtual ), el1.getIndex(), 1, el1.getModel(), el1 ),
								new ImagePlusTimePoint( el2.open( params.virtual ), el2.getIndex(), 1, el2.getModel(), el2 ) );
						
            			final Roi roi1 = IJUtil.getROI( pair.getTile1().getElement(), pair.getTile2().getElement() );
            			final Roi roi2 = IJUtil.getROI( pair.getTile2().getElement(), pair.getTile1().getElement() ); 
						
        				final PairWiseStitchingResult result = PairWiseStitchingImgLib.stitchPairwise(
        						pair.getImagePlus1(), pair.getImagePlus2(), roi1, roi2, pair.getTimePoint1(), pair.getTimePoint1(), params );
        				
        				String logstr = "Stitched tiles " + pairOfTiles._1.getIndex() + " and " + pairOfTiles._2.getIndex() + System.lineSeparator() +
        								"   CrossCorr=" + result.getCrossCorrelation() + ", PhaseCorr=" + result.getPhaseCorrelation() + ", RelShift=" + Arrays.toString( result.getOffset() );
        				System.out.println( logstr );
        				
        		    	el1.close();
        		    	el2.close();
        				
        				return new SerializablePairWiseStitchingResult( pairOfTiles, result );
					}
				});
		
		final List< SerializablePairWiseStitchingResult > stitchedPairs = pairwiseStitching.collect();
		
		System.out.println( "Stitched all tiles pairwise, perform global optimization on them..." );
		
		final TreeMap< Integer, Tile< ? > > tiles = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult pair : stitchedPairs ) {
			for ( final TileInfo tileInfo : new TileInfo[] { pair.getPairOfTiles()._1, pair.getPairOfTiles()._2 } ) {
				if ( !tiles.containsKey( tileInfo.getIndex() ) ) {
					try {
						final ImageCollectionElement el = IJUtil.createElement( job, tileInfo );
						final Tile< ? > tile = new ImagePlusTimePoint( null, el.getIndex(), 1, el.getModel(), el );
						tiles.put( tileInfo.getIndex(), tile );
					} catch ( Exception e ) {
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
		final ArrayList< ImagePlusTimePoint > optimized = GlobalOptimization.optimize( comparePairs, comparePairs.get( 0 ).getTile1(), params );
		
		System.out.println( "Global optimization done" );
		
		
		// output the result
		System.out.println( "Results: " );
		for ( final ImagePlusTimePoint imt : optimized )
			Log.info( imt.getImpId() + ": " + imt.getModel() );
		
		writeResultsToFile( job, optimized );
	}
	
	private static void writeResultsToFile( final StitchingJob job, final ArrayList< ImagePlusTimePoint > stitched ) {
		
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
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void performStitching( final StitchingJob job ) {
		
		final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles = findOverlappingTiles( job.getTiles() );
		System.out.println( "Overlapping pairs count = " + overlappingTiles.size() );
		
		final StitchingParameters params = new StitchingParameters();
		params.dimensionality = job.getDimensionality();
		params.fusionMethod = 7; // this means "Don't fuse images". Why is this not a enum?
		params.channel1 = 0;
		params.channel2 = 0;
		params.timeSelect = 0;
		params.checkPeaks = 5;
		params.computeOverlap = true;
		params.subpixelAccuracy = false;
		params.virtual = true;
		
		final SparkConf conf = new SparkConf()
				.setAppName( "StitchingSpark" )
				.setMaster( "local[7]" )
				.set( "spark.executor.memory", "10g" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		stitchWithSpark( sc, job, params, overlappingTiles );
		sc.close();
	}

	public static void main( String[] args ) {
		
		final StitchingArguments arguments = new StitchingArguments( args );
		if ( !arguments.parsedSuccessfully() )
			System.exit( 1 );
		
		final StitchingJob job = new StitchingJob( arguments );
		try {
			job.prepareTiles();
		} catch ( final Exception e ) {
			System.out.println( "Aborted: " + e.getMessage() );
			System.exit( 2 );
		}
		
		performStitching( job );
		
		System.out.println( "done" );
	}
	
	
	private static ArrayList< Tuple2< TileInfo, TileInfo > > findOverlappingTiles( final TileInfo[] tiles ) {
		
		final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles = new ArrayList<>();
		for ( int i = 0; i < tiles.length; i++ )
			for ( int j = i + 1; j < tiles.length; j++ )
				if ( overlap( tiles[ i ], tiles[ j ] ) )
					overlappingTiles.add( new Tuple2< TileInfo, TileInfo >( tiles[ i ], tiles[ j ] ) );
		return overlappingTiles;
	}
	
	private static boolean overlap( TileInfo t1, TileInfo t2 ) {
		assert t1.getDimensionality() == t2.getDimensionality();
		
		for ( int d = 0; d < t1.getDimensionality(); d++ ) {
			
			final float p1 = t1.getPosition()[ d ], p2 = t2.getPosition()[ d ];
			final int s1 = t1.getSize()[ d ], s2 = t2.getSize()[ d ];
			
			if ( !( ( p2 >= p1 && p2 <= p1 + s1 ) || 
					( p1 >= p2 && p1 <= p2 + s2 ) ) )
				return false;
		}
		return true;
	}
}
