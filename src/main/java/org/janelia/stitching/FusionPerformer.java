package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

import ij.IJ;
import ij.ImagePlus;
import mpicbg.models.InvertibleBoundable;
import net.imglib2.Cursor;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * @author pisarevi
 *
 */

public class FusionPerformer {
	
	private StitchingJob job;
	private ArrayList< TileInfo > tiles;
	private TreeMap< Integer, ImagePlus > images;
	
	public FusionPerformer( final StitchingJob job, final ArrayList< TileInfo > tiles ) {
		this.job = job;
		this.tiles = tiles;
		this.images = new TreeMap<>();
	}
	
	public void fuseTilesWithinSubregion( final TileInfo subregion ) throws Exception {
		int imageType = -1;
		for ( final TileInfo tile : tiles ) {
			System.out.println( "[Subregion " + subregion.getIndex() + "] Loading image " + (images.size()+1) + " of " + tiles.size() );
			final ImagePlus img = IJ.openImage( Utils.getAbsoluteImagePath( job, tile ) );
			images.put( tile.getIndex(), img );
			
			if ( imageType == -1 )
				imageType = img.getType();
			else if ( imageType != img.getType() )
				throw new Exception( "Can't fuse images of different types" );
		}
		
		ImagePlus outImg = null;
		if ( imageType == ImagePlus.GRAY8 )
			outImg = fuseSubregion( subregion, new UnsignedByteType() );
		else if ( imageType == ImagePlus.GRAY16 )
			outImg = fuseSubregion( subregion, new UnsignedShortType() );
		else if ( imageType == ImagePlus.GRAY32 )
			outImg = fuseSubregion( subregion, new FloatType() );
		
		System.out.println( "Saving the resulting file for subregion " + subregion.getIndex() );
		IJ.saveAsTiff( outImg, subregion.getFile() );
		
		outImg.close();
		for ( final ImagePlus img : images.values() )
			img.close();
	}
	
	private < T extends NumericType<T> & NativeType<T> > ImagePlus fuseSubregion( final TileInfo subregion, final T type ) throws Exception {
		final Boundaries subregionBoundaries = subregion.getBoundaries();
		final ArrayList< IntervalView< T > > intervals = new ArrayList<>();
		final ArrayList< InvertibleBoundable > models = new ArrayList<>();
		
		for ( final TileInfo tile : tiles ) {
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
			final Img< T > inImg = ImageJFunctions.wrap( images.get( tile.getIndex() ) );
			final IntervalView< T > interval = Views.offsetInterval( inImg, tileImageOffset, tileImageDimensions );
			intervals.add( interval );

			final double[] tileSubregionOffset = new double[ tile.getDimensionality() ];
			for ( int d = 0; d < tileSubregionOffset.length; d++ )
				tileSubregionOffset[ d ] = Math.max( 0, tile.getPosition(d) - subregionBoundaries.getMin(d) );
			models.add( (InvertibleBoundable) TileModelFactory.createOffsetModel( tileSubregionOffset ) );
		}
		
		// Create output image
		System.out.println( "subregion: " + Arrays.toString( subregionBoundaries.getDimensions() ) );
		final Img< T > out = new ImagePlusImgFactory< T >().create( subregionBoundaries.getDimensions(), type.createVariable() );
		System.out.println( "out interval size = " + out.size() );
		
		// Draw all intervals onto it one by one
		final RandomAccess< T > randomAccess = out.randomAccess();
		for ( int i = 0; i < intervals.size(); i++ ) {
			final IntervalView< T > interval = intervals.get( i );
			
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
			final Cursor< T > cursorInput = interval.localizingCursor();
			while ( cursorInput.hasNext()) {
				cursorInput.fwd();
				
				final Point p = new Point( origin );
				p.move( cursorInput );
				randomAccess.setPosition( p );
				
				randomAccess.get().set( cursorInput.get() );
		    }
		}
		
		return ImageJFunctions.wrap( out, "" );
	}
}
