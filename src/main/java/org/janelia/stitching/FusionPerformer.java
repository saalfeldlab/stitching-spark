package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * @author pisarevi
 *
 */

public class FusionPerformer
{

	private final StitchingJob job;

	public FusionPerformer( final StitchingJob job )
	{
		this.job = job;
	}

	@SuppressWarnings( "unchecked" )
	public < T extends NumericType< T > & NativeType< T > > void fuseTilesWithinSubregion( final ArrayList< TileInfo > tiles, final TileInfo subregion ) throws Exception
	{
		ImageType imageType = null;
		for ( final TileInfo tile : tiles )
		{
			if ( imageType == null )
				imageType = tile.getType();
			else if ( imageType != tile.getType() )
				throw new Exception( "Can't fuse images of different types" );
		}

		fuseSubregion( tiles, subregion, ( T ) imageType.getType() );
	}

	private < T extends NumericType< T > & NativeType< T > > void fuseSubregion( final ArrayList< TileInfo > tiles, final TileInfo subregion, final T type ) throws Exception
	{
		final Boundaries subregionBoundaries = subregion.getBoundaries();

		// Create output image
		System.out.println( "subregion: " + Arrays.toString( subregionBoundaries.getDimensions() ) );
		final Img< T > out = new ImagePlusImgFactory< T >().create( subregionBoundaries.getDimensions(), type.createVariable() );
		final RandomAccess< T > randomAccess = out.randomAccess();

		// Draw all intervals onto it one by one
		for ( int i = 0; i < tiles.size(); i++ )
		{
			final TileInfo tile = tiles.get( i );
			System.out.println( "[Subregion " + subregion.getIndex() + "] Loading image " + ( i + 1 ) + " of " + tiles.size() );
			final ImagePlus img = IJ.openImage( Utils.getAbsoluteImagePath( job, tile ) );

			final Boundaries tileBoundariesWithinSubregion = tile.getBoundaries();
			final long[] tileImageOffset = new long[ tile.numDimensions() ];
			for ( int d = 0; d < tileImageOffset.length; d++ )
				tileImageOffset[ d ] = Math.max( 0, subregionBoundaries.min( d ) - tileBoundariesWithinSubregion.min( d ) ) / 10;

			for ( int d = 0; d < subregion.numDimensions(); d++ )
			{
				tileBoundariesWithinSubregion.setMin( d, Math.max( tileBoundariesWithinSubregion.min( d ), subregionBoundaries.min( d ) ) );
				tileBoundariesWithinSubregion.setMax( d, Math.min( tileBoundariesWithinSubregion.max( d ), subregionBoundaries.max( d ) ) );

				// Set relative coordinates
				tileBoundariesWithinSubregion.setMin( d, tileBoundariesWithinSubregion.min( d ) - subregionBoundaries.min( d ) );
				tileBoundariesWithinSubregion.setMax( d, tileBoundariesWithinSubregion.max( d ) - subregionBoundaries.min( d ) );
			}

			// Prepare offset to map the input image to the output image
			final long[] tileSubregionOffset = new long[ tile.numDimensions() ];
			for ( int d = 0; d < tileSubregionOffset.length; d++ )
				tileSubregionOffset[ d ] = Math.max( 0, ( long ) Math.floor( tile.getPosition( d ) ) - subregionBoundaries.min( d ) );

			final long[] tileImageDimensions = tileBoundariesWithinSubregion.getDimensions();
			final Img< T > in = ImageJFunctions.wrap( img );
			final IntervalView< T > interval = Views.offsetInterval( in, tileImageOffset, tileImageDimensions );

			String debug = "Processing tile " + ( i + 1 ) + " of " + tiles.size() + " ";
			for ( int d = 0; d < interval.numDimensions(); d++ )
				debug += "(" + ( interval.min( d ) + tileSubregionOffset[ d ] ) + "," + ( interval.max( d ) + tileSubregionOffset[ d ] ) + "),";
			System.out.println( debug );

			// Copy input to output
			final Cursor< T > cursorInput = interval.localizingCursor();
			while ( cursorInput.hasNext() )
			{
				cursorInput.fwd();

				final Point p = new Point( cursorInput );
				p.move( tileSubregionOffset );
				randomAccess.setPosition( p );

				randomAccess.get().set( cursorInput.get() );
			}

			img.close();
		}

		final ImagePlus outImg = ImageJFunctions.wrap( out, "" );
		subregion.setType( ImageType.valueOf( outImg.getType() ) );
		System.out.println( "Saving the resulting file for subregion " + subregion.getIndex() );
		IJ.saveAsTiff( outImg, subregion.getFile() );
		outImg.close();
	}
}
