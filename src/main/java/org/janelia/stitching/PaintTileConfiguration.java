package org.janelia.stitching;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ByteImagePlus;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.util.Intervals;

public class PaintTileConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );

		TileOperations.translateTilesToOriginReal( tiles );
		final Interval boundingBox = TileOperations.getCollectionBoundaries( tiles );

		final int sliceDim = 2;

		final List< Integer > displayProjectionDims = new ArrayList<>();
		for ( int d = 0; d < boundingBox.numDimensions(); ++d )
			if ( d != sliceDim )
				displayProjectionDims.add( d );

		final long middlePoint = boundingBox.min( sliceDim ) + boundingBox.dimension( sliceDim ) / 2;
		final long[] middleSliceIntervalMin = Intervals.minAsLongArray( boundingBox ).clone();
		final long[] middleSliceIntervalMax = Intervals.maxAsLongArray( boundingBox ).clone();
		middleSliceIntervalMin[ sliceDim ] = middlePoint;
		middleSliceIntervalMax[ sliceDim ] = middlePoint;
		final Interval middleSliceInterval = new FinalInterval( middleSliceIntervalMin, middleSliceIntervalMax );

		final ByteImagePlus< ByteType > img = ImagePlusImgs.bytes(
				boundingBox.dimension( displayProjectionDims.get( 0 ) ),
				boundingBox.dimension( displayProjectionDims.get( 1 ) )
			);

		final ByteType val = new ByteType( Byte.MAX_VALUE );
		int tilesPainted = 0;
		for ( final TileInfo tile : tiles )
		{
			final Interval tileInterval = tile.getBoundaries();
			if ( TileOperations.overlap( tileInterval, middleSliceInterval ) )
			{
				System.out.println( "Drawing tile" );
				++tilesPainted;
				final long[] tileIntervalProjectionMin = new long[] {
						tileInterval.min( displayProjectionDims.get( 0 ) ),
						tileInterval.min( displayProjectionDims.get( 1 ) )
					};
				final long[] tileIntervalProjectionMax = new long[] {
						tileInterval.max( displayProjectionDims.get( 0 ) ),
						tileInterval.max( displayProjectionDims.get( 1 ) )
					};
				final Interval tileIntervalProjection = new FinalInterval( tileIntervalProjectionMin, tileIntervalProjectionMax );
				drawBox( img, tileIntervalProjection, val );
			}
		}

		System.out.println( "Tiles painted: " + tilesPainted + " out of " + tiles.length );

		final ImagePlus imp = img.getImagePlus();
		new ImageJ();
		imp.show();
	}

	private static < T extends NativeType< T > & RealType< T > > void drawBox(
			final RandomAccessibleInterval< T > img,
			final Interval box,
			final T val )
	{
		final RandomAccess< T > imgRandomAccess = img.randomAccess();
		final int[][] boxCorners = new int[][] { Intervals.minAsIntArray( box ), Intervals.maxAsIntArray( box ) };
		final long[] displayPosition = new long[ 2 ];
		for ( int dFixed = 0; dFixed < 2; ++dFixed )
		{
			for ( int dMove = 0; dMove < 2; ++dMove )
			{
				if ( dFixed != dMove)
				{
					for ( int i = 0; i < 2; ++i )
					{
						displayPosition[ dFixed ] = boxCorners[ i ][ dFixed ];
						for ( int c = boxCorners[ 0 ][ dMove ]; c <= boxCorners[ 1 ][ dMove ]; ++c )
						{
							displayPosition[ dMove ] = c;
							if ( displayPosition[ 0 ] >= img.min( 0 ) && displayPosition[ 0 ] <= img.max( 0 ) && displayPosition[ 1 ] >= img.min( 1 ) && displayPosition[ 1 ] <= img.max( 1 ) )
							{
								imgRandomAccess.setPosition( displayPosition );
								imgRandomAccess.get().set( val );
							}
						}
					}
				}
			}
		}
	}
}
