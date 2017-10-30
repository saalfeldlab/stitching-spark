package org.janelia.stitching;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.junit.Assert;
import org.junit.Test;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class TilesToN5Test
{
	@Test
	public void test() throws ImgLibException, IOException
	{
		final Random rnd = new Random();
		final long[] dimensions = new long[ 3 ];
		for ( int d = 0; d < dimensions.length; ++d )
			dimensions[ d ] = rnd.nextInt( 700 ) + 1;

		final int blockSize = rnd.nextInt( 100 ) + 30;

		System.out.println( "Generating random image of size " + Arrays.toString( dimensions ) + ", block size set to " + blockSize );

		final ImagePlusImg< UnsignedShortType, ? > img = ImagePlusImgs.unsignedShorts( dimensions );
		final Cursor< UnsignedShortType > cursor = img.cursor();
		while ( cursor.hasNext() )
			cursor.next().set( ( int ) Math.round( ( rnd.nextGaussian() + 1.0 ) * 100 ) );

		final ImagePlus imp = img.getImagePlus();
		Utils.workaroundImagePlusNSlices( imp );

		final Path tempDir = Files.createTempDirectory( "TilesToN5Test-" );
		final String impFilename = "test-imp.tif";
		final String impPath = tempDir.resolve( impFilename ).toString();

		System.out.println( "Saving test image to: " + impPath );
		IJ.saveAsTiff( imp, impPath );

		final TileInfo tile = new TileInfo( dimensions.length );
		tile.setIndex( 0 );
		tile.setSize( dimensions );
		tile.setPosition( new double[ dimensions.length ] );
		tile.setFilePath( impPath );
		tile.setType( ImageType.GRAY16 );
		final TileInfo[] tiles = new TileInfo[] { tile };

		final String n5Path = tempDir.resolve( "n5-test" ).toString();
		final String datasetPath = "test-channel";

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local" )
				.setAppName( "TilesToN5Test" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			final Map< String, TileInfo[] > newTiles = TilesToN5Converter.convertTiffToN5(
					sparkContext,
					n5Path,
					() -> N5.openFSWriter( n5Path ),
					Collections.singletonMap( datasetPath, tiles ),
					blockSize,
					CompressionType.GZIP
				);

			Assert.assertEquals( datasetPath, newTiles.keySet().iterator().next() );

			final TileInfo newTile = newTiles.values().iterator().next()[ 0 ];
			Assert.assertEquals( Paths.get( n5Path, datasetPath, impFilename ).toString(), newTile.getFilePath() );
		}

		System.out.println( "Loading N5 cell export from:" + Paths.get( n5Path, datasetPath ).toString() );

		final RandomAccessibleInterval< UnsignedShortType > rai = N5Utils.open( N5.openFSReader( n5Path ), Paths.get( datasetPath, impFilename ).toString() );
		final Cursor< UnsignedShortType > raiCursor = Views.iterable( rai ).localizingCursor();
		final RandomAccess< UnsignedShortType > imgRandomAccess = img.randomAccess();
		int pixelsProcessed = 0;
		while ( raiCursor.hasNext() )
		{
			++pixelsProcessed;
			raiCursor.fwd();
			imgRandomAccess.setPosition( raiCursor );
			if ( !raiCursor.get().valueEquals( imgRandomAccess.get() ) )
			{
				final long[] position = new long[ raiCursor.numDimensions() ];
				raiCursor.localize( position );
				fail( "pixel value differs at " + Arrays.toString( position ) );
			}
		}

		if ( pixelsProcessed != Intervals.numElements( dimensions ) )
			fail( "processed less pixels than contained in the image" );

		System.out.println( "All checks passed, removing temp data..." );
		N5.openFSWriter( tempDir.toString() ).remove();

		System.out.println( "OK" );
	}
}
