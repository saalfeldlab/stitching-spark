package org.janelia.stitching;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.util.ImageImporter;
import org.janelia.util.concurrent.MultithreadedExecutor;

import bdv.img.TpsTransformWrapper;
import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class WarpedTileLoaderTest
{
	public static void main( final String[] args ) throws IOException
	{
		test();
	}

	private static < T extends NativeType< T > & RealType< T > > void test() throws IOException
	{
		new ImageJ();

		final String slab = "11z";
		final int channel = 0;
		final TileInfo[] slabTiles = C1WarpedMetadata.getSlabTiles( slab, channel );
		final TileInfo slabTile = slabTiles[ slabTiles.length / 2 ];
		final TpsTransformWrapper transform = C1WarpedMetadata.getSlabTransform( slab );
		final double[] slabMin = Intervals.minAsDoubleArray( TileOperations.getRealCollectionBoundaries( slabTiles ) );

		if ( slabTile.getPixelResolution() == null )
			slabTile.setPixelResolution( new double[] { 0.097, 0.097, 0.18 } );

		final ImagePlus imp = ImageImporter.openImage( slabTile.getFilePath() );
		imp.show();

		final RandomAccessibleInterval< T > warpedTile = WarpedTileLoader.loadTile( DataProviderFactory.createFSDataProvider(), slabMin, slabTile, transform );

		final Interval estimatedBoundingBox = WarpedTileLoader.getBoundingBox( slabMin, slabTile, transform );
		for ( int d = 0; d < Math.max( warpedTile.numDimensions(), estimatedBoundingBox.numDimensions() ); ++d )
			if ( warpedTile.min( d ) != estimatedBoundingBox.min( d ) || warpedTile.max( d ) != estimatedBoundingBox.max( d ) )
				throw new RuntimeException( "warped image dimensions != estimated bounding box" );

		System.out.println( String.format( "Warped tile: min=%s, dimensions=%s", Arrays.toString( Intervals.minAsLongArray( warpedTile ) ), Arrays.toString( Intervals.dimensionsAsLongArray( warpedTile ) ) ) );

		final ImagePlusImg< T, ? > warpedImg = new ImagePlusImgFactory< T >().create( warpedTile, Util.getTypeFromInterval( warpedTile ) );

		try ( final MultithreadedExecutor threadPool = new MultithreadedExecutor() )
		{
			threadPool.run(
					z -> {
						final Cursor< T > src = Views.flatIterable( Views.hyperSlice( Views.zeroMin( warpedTile ), 2, z ) ).cursor();
						final Cursor< T > dst = Views.flatIterable( Views.hyperSlice( warpedImg, 2, z ) ).cursor();
						while ( dst.hasNext() || src.hasNext() )
							dst.next().set( src.next() );
					},
					( int ) warpedTile.dimension( 2 )
				);
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ImagePlus warpedImp = null;
		try {
			warpedImp = warpedImg.getImagePlus();
		} catch (final ImgLibException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Utils.workaroundImagePlusNSlices( warpedImp );
		warpedImp.show();
	}
}
