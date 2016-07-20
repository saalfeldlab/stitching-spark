package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import ij.IJ;
import ij.ImagePlus;
import ij.gui.Roi;
import mpicbg.models.InvertibleBoundable;
import mpicbg.models.Tile;
import mpicbg.stitching.ComparePair;
import mpicbg.stitching.GlobalOptimization;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
import mpicbg.stitching.PairWiseStitchingImgLib;
import mpicbg.stitching.PairWiseStitchingResult;
import mpicbg.stitching.StitchingParameters;
import net.imglib2.Cursor;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
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
		
		final ArrayList< TileInfo > unknownSizeTiles = new ArrayList<>();
		for ( final TileInfo tile : job.getTiles() )
			if ( tile.getSize() == null )
				unknownSizeTiles.add( tile );
		
		final SparkConf conf = new SparkConf().setAppName( "Stitching" );
		final JavaSparkContext sparkContext = new JavaSparkContext( conf );
		
		if ( !unknownSizeTiles.isEmpty() )
			queryTileSize( sparkContext, unknownSizeTiles );
		
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
			final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles = Utils.findOverlappingTiles( job.getTiles() );
			System.out.println( "Overlapping pairs count = " + overlappingTiles.size() );
			
			computeShifts( sparkContext, overlappingTiles );
		}

		// Fuse
		if ( job.getMode() != StitchingJob.Mode.NoFuse ) {
			
			final Boundaries boundaries = Utils.findBoundaries( job.getTiles() );
			final ArrayList< TileInfo > subregions = Utils.divideSpace( boundaries, job.getSubregionSize() );
			
			final String fusedFolder = job.getBaseFolder() + "/fused";
			new File( fusedFolder ).mkdirs();
			
			fuse( sparkContext, subregions, fusedFolder );
		}
		
		sparkContext.close();
		System.out.println( "done" );
	}
	
	
	private void queryTileSize( final JavaSparkContext sparkContext, final ArrayList< TileInfo > unknownSizeTiles ) {
		
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( unknownSizeTiles );
		final JavaRDD< TileInfo > tileSize = rdd.map(
				new Function< TileInfo, TileInfo >() {
					
					private static final long serialVersionUID = -4991255417353136684L;

					@Override
					public TileInfo call( final TileInfo tile ) throws Exception {
						final ImageCollectionElement el = Utils.createElement( job, tile );
						final ImagePlus imp = el.open( true );
						
						// FIXME: workaround for misinterpreting slices as timepoints when no metadata is present 
						long[] size = Conversions.toLongArray( el.getDimensions() );
						if ( size.length == 2 && imp.getNFrames() > 1 )
							size = new long[] { size[ 0 ], size[ 1 ], imp.getNFrames() };
						
						tile.setSize( size );
						el.close();
						return tile;
					}
				});
		
		final List< TileInfo > knownSizeTiles = tileSize.collect();
		
		System.out.println( "Obtained image size for all tiles" );
		
		final TileInfo[] tiles = job.getTiles();
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tilesMap.put( tile.getIndex(), tile );
		
		for ( final TileInfo knownSizeTile : knownSizeTiles )
			tilesMap.get( knownSizeTile.getIndex() ).setSize( knownSizeTile.getSize() );
		
		try {
			job.saveTiles( Utils.addFilenameSuffix( args.getInput(), "_sized" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}

	
	private void computeShifts( final JavaSparkContext sparkContext, final ArrayList< Tuple2< TileInfo, TileInfo > > overlappingTiles ) {
		
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
		final TreeMap< Integer, Tile< ? > > fakeTileImagesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult pair : stitchedPairs ) {
			for ( final TileInfo tileInfo : new TileInfo[] { pair.getPairOfTiles()._1, pair.getPairOfTiles()._2 } ) {
				if ( !fakeTileImagesMap.containsKey( tileInfo.getIndex() ) ) {
					try {
						final ImageCollectionElement el = Utils.createElement( job, tileInfo );
						final Tile< ? > tile = new ImagePlusTimePoint( null, el.getIndex(), 1, el.getModel(), el );
						fakeTileImagesMap.put( tileInfo.getIndex(), tile );
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
			job.saveTiles( Utils.addFilenameSuffix( args.getInput(), "_shifted" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
	
	
	private void fuse( final JavaSparkContext sparkContext, final ArrayList< TileInfo > subregions, final String fusedFolder ) {

		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( subregions );
		final JavaRDD< TileInfo > fused = rdd.map(
				new Function< TileInfo, TileInfo >() {

					private static final long serialVersionUID = 8324712817942470416L;

					@Override
					public TileInfo call( final TileInfo subregion ) throws Exception {
						// TODO: optimize with KD interval tree or smth similar
						final ArrayList< TileInfo > tilesWithinSubregion = new ArrayList<>();
						for ( final TileInfo tile : job.getTiles() )
							if ( Utils.overlap( tile, subregion ) )
								tilesWithinSubregion.add( tile );
						
						if ( tilesWithinSubregion.isEmpty() )
							return null;
						
						subregion.setFile( fusedFolder + "/tile" + subregion.getIndex() + ".tif" );
						System.out.println( "Starting to fuse tiles within subregion " + subregion.getIndex() );
						
						// TODO: support different kinds of RealType
						
						final Boundaries subregionBoundaries = subregion.getBoundaries();
						final ArrayList< IntervalView< UnsignedShortType > > intervals = new ArrayList<>();
						final ArrayList< InvertibleBoundable > models = new ArrayList<>();
						
						for ( final TileInfo tile : tilesWithinSubregion ) {
							System.out.println( "[Subregion " + subregion.getIndex() + "] Loading image " + (intervals.size()+1) + " of " + tilesWithinSubregion.size() );
							
							final ImagePlus imp = IJ.openImage( tile.getFile() );
							
							final Boundaries tileBoundariesWithinSubregion = tile.getBoundaries();
							final long[] tileImageOffset = new long[ tile.getDimensionality() ];
							for ( int d = 0; d < tileImageOffset.length; d++ )
								tileImageOffset[ d ] = Math.max( 0, subregionBoundaries.getMin(d) - tileBoundariesWithinSubregion.getMin(d) ) / 10;
							
							for ( int d = 0; d < subregion.getDimensionality(); d++ ) {
								tileBoundariesWithinSubregion.setMin( d, Math.max( tileBoundariesWithinSubregion.getMin(d), subregionBoundaries.getMin(d) ) );
								tileBoundariesWithinSubregion.setMax( d, Math.min( tileBoundariesWithinSubregion.getMax(d), subregionBoundaries.getMax(d) ) );
								
								// Set relative coordinates
								tileBoundariesWithinSubregion.setMin( d, tileBoundariesWithinSubregion.getMin(d) - subregionBoundaries.getMin(d) );
								tileBoundariesWithinSubregion.setMax( d, tileBoundariesWithinSubregion.getMax(d) - subregionBoundaries.getMin(d) );
							}
							
							final long[] tileImageDimensions = tileBoundariesWithinSubregion.getDimensions();
							final IntervalView< UnsignedShortType > interval = Views.offsetInterval( 
									ImageJFunctions.wrapShort( imp ), tileImageOffset, tileImageDimensions );
							intervals.add( interval );

							final double[] tileSubregionOffset = new double[ tile.getDimensionality() ];
							for ( int d = 0; d < tileSubregionOffset.length; d++ )
								tileSubregionOffset[ d ] = Math.max( 0, tile.getPosition(d) - subregionBoundaries.getMin(d) );
							models.add( (InvertibleBoundable) TileModelFactory.createOffsetModel( tileSubregionOffset ) );
						}
						
						// Create output image
						final ImgFactory< UnsignedShortType > f = new ImagePlusImgFactory<>();
						System.out.println( "subregion: " + Arrays.toString( subregionBoundaries.getDimensions() ) );
						final Img< UnsignedShortType > out = f.create( subregionBoundaries.getDimensions(), new UnsignedShortType() );
						System.out.println( "out interval size = " + out.size() );
						
						// Draw all intervals onto it one by one
						final RandomAccess< UnsignedShortType > randomAccess = out.randomAccess();
						for ( int i = 0; i < intervals.size(); i++ ) {
							final IntervalView< UnsignedShortType > interval = intervals.get( i );
							
							// Prepare offset to map the input image to the output image
							final double[] pos = new double[ interval.numDimensions() ];
				  			models.get( i ).applyInPlace( pos );
				  			final Point origin = new Point( pos.length );
				  			for ( int d = 0; d < pos.length; d++ )
				  				origin.setPosition( (long)Math.floor( pos[d] ), d );
				  			
							System.out.println( "  Processing tile " + (i+1) + " of " + intervals.size() + ".." );
							String debug = "";
							for ( int d = 0; d < interval.numDimensions(); d++ )
								debug += "("+(interval.min(d)+origin.getLongPosition(d))+","+(interval.max(d)+origin.getLongPosition(d))+"),";
							System.out.println( "  interval: size=" + interval.size()+";   " + debug );
				  			
				  			// Copy input to output
							final Cursor< UnsignedShortType > cursorInput = interval.localizingCursor();
							while ( cursorInput.hasNext()) {
								cursorInput.fwd();
								
								final Point p = new Point( origin );
								p.move( cursorInput );
								randomAccess.setPosition( p );
								
								randomAccess.get().set( cursorInput.get() );
						    }
						}
						
						System.out.println( "Saving the resulting file  for subregion " + subregion.getIndex() );
						
						final ImagePlus outImp = ImageJFunctions.wrap( out, "" );
						IJ.saveAsTiff( outImp, subregion.getFile() );
						
						System.out.println( "Completed for subregion " + subregion.getIndex() );
						
						outImp.close();
						
						return subregion;
					}
				});
		
		final List< TileInfo > output = fused.collect();
		output.removeAll( Collections.singleton( null ) );
		System.out.println( "Obtained " + output.size() + " tiles" );
		
		job.setTiles( output.toArray( new TileInfo[ 0 ] ) );
		
		try {
			job.saveTiles( Utils.addFilenameSuffix( args.getInput(), "_fused" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
}
