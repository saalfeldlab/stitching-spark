package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;

import ij.IJ;
import ij.ImagePlus;
import mpicbg.stitching.fusion.AveragePixelFusion;
import mpicbg.stitching.fusion.PixelFusion;
import net.imglib2.Cursor;
import net.imglib2.Point;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
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
	public < T extends RealType< T > & NativeType< T > > void fuseTilesWithinSubregion( final ArrayList< TileInfo > tiles, final TileInfo subregion ) throws Exception
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

	private < T extends RealType< T > & NativeType< T > > void fuseSubregion( final ArrayList< TileInfo > tiles, final TileInfo subregion, final T type ) throws Exception
	{
		final Boundaries subregionBoundaries = subregion.getBoundaries();
		System.out.println( "subregion: " + Arrays.toString( subregionBoundaries.getDimensions() ) + ", tiles within subregion = " + tiles.size() );
		
		// TODO: improve flexibility by making this array n-dimensional
		final PixelFusion[][][] pixels = new PixelFusion[ ( int ) subregionBoundaries.getDimensions( 0 ) ]
		                                                [ ( int ) subregionBoundaries.getDimensions( 1 ) ]
		                                                [ ( int ) subregionBoundaries.getDimensions( 2 ) ];
		for ( int x = 0; x < pixels.length; x++ )
			for ( int y = 0; y < pixels[ x ].length; y++ )
				for ( int z = 0; z < pixels[ y ].length; z++ )
					pixels[ x ][ y ][ z ] = new AveragePixelFusion(); // TODO: support different kinds of fusion
		
		// Fill the pixels
		for ( int i = 0; i < tiles.size(); i++ )
		{
			final TileInfo tile = tiles.get( i );
			System.out.println( "[Subregion " + subregion.getIndex() + "] Loading image " + ( i + 1 ) + " of " + tiles.size() );
			final ImagePlus img = IJ.openImage( Utils.getAbsoluteImagePath( job, tile ) );

			final Boundaries tileBoundariesWithinSubregion = tile.getBoundaries();
			final long[] tileImageOffset = new long[ tile.getDimensionality() ];
			for ( int d = 0; d < tileImageOffset.length; d++ )
				tileImageOffset[ d ] = Math.max( 0, subregionBoundaries.getMin( d ) - tileBoundariesWithinSubregion.getMin( d ) ) / 10;

			for ( int d = 0; d < subregion.getDimensionality(); d++ )
			{
				tileBoundariesWithinSubregion.setMin( d, Math.max( tileBoundariesWithinSubregion.getMin( d ), subregionBoundaries.getMin( d ) ) );
				tileBoundariesWithinSubregion.setMax( d, Math.min( tileBoundariesWithinSubregion.getMax( d ), subregionBoundaries.getMax( d ) ) );

				// Set relative coordinates
				tileBoundariesWithinSubregion.setMin( d, tileBoundariesWithinSubregion.getMin( d ) - subregionBoundaries.getMin( d ) );
				tileBoundariesWithinSubregion.setMax( d, tileBoundariesWithinSubregion.getMax( d ) - subregionBoundaries.getMin( d ) );
			}

			// Prepare offset to map the input image to the output image
			final long[] tileSubregionOffset = new long[ tile.getDimensionality() ];
			for ( int d = 0; d < tileSubregionOffset.length; d++ )
				tileSubregionOffset[ d ] = Math.max( 0, ( long ) Math.floor( tile.getPosition( d ) ) - subregionBoundaries.getMin( d ) );

			final long[] tileImageDimensions = tileBoundariesWithinSubregion.getDimensions();
			final Img< T > in = ImageJFunctions.wrapReal( img );
			final IntervalView< T > interval = Views.offsetInterval( in, tileImageOffset, tileImageDimensions );

			String debug = "Processing tile " + ( i + 1 ) + " of " + tiles.size() + " ";
			for ( int d = 0; d < interval.numDimensions(); d++ )
				debug += "(" + ( interval.min( d ) + tileSubregionOffset[ d ] ) + "," + ( interval.max( d ) + tileSubregionOffset[ d ] ) + "),";
			System.out.println( debug );

			// Iterate through every pixel in the desired interval
			final Cursor< T > inputCursor = interval.localizingCursor();
			while ( inputCursor.hasNext() )
			{
				inputCursor.fwd();
				
				final double[] inputPosition = new double[ interval.numDimensions() ];
				inputCursor.localize( inputPosition );
				
				final Point outPosition = new Point( inputCursor );
				outPosition.move( tileSubregionOffset );
				
				pixels[ outPosition.getIntPosition( 0 ) ]
					  [ outPosition.getIntPosition( 1 ) ]
					  [ outPosition.getIntPosition( 2 ) ].addValue( inputCursor.get().getRealDouble(), tile.getIndex(), inputPosition );
			}
			
			img.close();
		}

		System.out.println( "Creating output image and filling it with data.." );
		
		final Img< T > out = new ImagePlusImgFactory< T >().create( subregionBoundaries.getDimensions(), type.createVariable() );
		final Cursor< T > outCursor = out.localizingCursor();
		while ( outCursor.hasNext() )
		{
			outCursor.fwd();
			final Point outPosition = new Point( outCursor );
			outCursor.get().setReal( pixels[ outPosition.getIntPosition( 0 ) ]
										   [ outPosition.getIntPosition( 1 ) ]
										   [ outPosition.getIntPosition( 2 ) ].getValue() );
		}
		
		final ImagePlus outImg = ImageJFunctions.wrap( out, "" );
		System.out.println( "Saving the resulting file for subregion " + subregion.getIndex() );
		IJ.saveAsTiff( outImg, subregion.getFile() );
		outImg.close();
	}
}
