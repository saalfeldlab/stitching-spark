package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.util.TiffSliceReader;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

public class GenerateSliceTiffExportSpark
{
	public static void main( final String[] args ) throws IOException
	{
		run( args[ 0 ] );
	}

	private static < T extends NativeType< T > & RealType< T > > void run( final String filepath ) throws IOException
	{
		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf().setAppName( "TiffSliceExport" ) ) )
		{
			final TileInfo[] cells = TileInfoJSONProvider.loadTilesConfiguration( filepath );
			final Boundaries box = TileOperations.getCollectionBoundaries( cells );
			final List< Long > zPositions = new ArrayList<>();
			for ( long z = box.min( 2 ); z <= box.max( 2 ); ++z )
				zPositions.add( z );

			final long[] sliceDimensions = new long[] { box.dimension( 0 ), box.dimension( 1 ) };

			final String basePath = Paths.get( filepath ).getParent().toString() + "/slice_tiff";
			new File( basePath ).mkdirs();

			sparkContext.parallelize( zPositions ).foreach( zPos ->
				{
					final ImagePlusImg< T, ? > imgSliceOut = new ImagePlusImgFactory< T >().create( sliceDimensions, ( T ) cells[ 0 ].getType().getType().createVariable() );
					for ( final TileInfo cell : cells )
					{
						final Boundaries cellBoundaries = cell.getBoundaries();
						if ( zPos >= cellBoundaries.min( 2 ) && zPos <= cellBoundaries.max( 2 ) )
						{
							final int cellSlice = ( int ) ( zPos - cellBoundaries.min( 2 ) + 1 );
							final ImagePlus impSlice = TiffSliceReader.readSlice( cell.getFilePath(), cellSlice );
							final RandomAccessibleInterval< T > imgSlice = ImagePlusImgs.from( impSlice );
							final RandomAccessibleInterval< T > imgSliceTranslated = Views.translate( imgSlice, new long[] { cellBoundaries.min( 0 ), cellBoundaries.min( 1 ) } );
							final Cursor< T > imgSliceTranslatedCursor = Views.flatIterable( imgSliceTranslated ).cursor();

							final RandomAccessibleInterval< T > imgSliceOutInterval = Views.interval( imgSliceOut, imgSliceTranslated );
							final Cursor< T > imgSliceOutIntervalCursor = Views.flatIterable( imgSliceOutInterval ).cursor();

							while ( imgSliceTranslatedCursor.hasNext() || imgSliceOutIntervalCursor.hasNext() )
								imgSliceOutIntervalCursor.next().set( imgSliceTranslatedCursor.next() );
						}
					}

					final ImagePlus impSliceOut = imgSliceOut.getImagePlus();
					IJ.saveAsTiff( impSliceOut, basePath + "/" + zPos + ".tif" );
				}
			);
		}
	}
}
