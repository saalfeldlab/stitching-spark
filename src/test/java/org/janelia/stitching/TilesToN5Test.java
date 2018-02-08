package org.janelia.stitching;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Writer;
import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;

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
	public void testFSMultithreaded() throws ImgLibException, IOException
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

		final Map< String, TileInfo[] > newTiles = TilesToN5ConverterMultithreaded.convertTiffToN5(
				n5Path,
				() -> new N5FSWriter( n5Path ),
				Collections.singletonMap( datasetPath, tiles ),
				blockSize,
				new GzipCompression()
			);

		Assert.assertEquals( datasetPath, newTiles.keySet().iterator().next() );

		final TileInfo newTile = newTiles.values().iterator().next()[ 0 ];
		Assert.assertEquals( Paths.get( n5Path, datasetPath, impFilename ).toString(), newTile.getFilePath() );

		System.out.println( "Loading N5 cell export from:" + Paths.get( n5Path, datasetPath ).toString() );

		final RandomAccessibleInterval< UnsignedShortType > rai = TileLoader.loadTile( newTile, DataProviderFactory.createByURI( URI.create( newTile.getFilePath() ) ) );
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
		Assert.assertTrue( new N5FSWriter( tempDir.toString() ).remove() );

		System.out.println( "OK" );
	}

	@Test
	public void testFSSpark() throws ImgLibException, IOException
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

		final Map< String, TileInfo[] > newTiles;
		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local[*]" )
				.setAppName( "TilesToN5Test" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ) ) )
		{
			newTiles = TilesToN5ConverterMultithreaded.convertTiffToN5(
					n5Path,
					() -> new N5FSWriter( n5Path ),
					Collections.singletonMap( datasetPath, tiles ),
					blockSize,
					new GzipCompression()
				);
		}

		Assert.assertEquals( datasetPath, newTiles.keySet().iterator().next() );

		final TileInfo newTile = newTiles.values().iterator().next()[ 0 ];
		Assert.assertEquals( Paths.get( n5Path, datasetPath, impFilename ).toString(), newTile.getFilePath() );

		System.out.println( "Loading N5 cell export from:" + Paths.get( n5Path, datasetPath ).toString() );

		final RandomAccessibleInterval< UnsignedShortType > rai = TileLoader.loadTile( newTile, DataProviderFactory.createByURI( URI.create( newTile.getFilePath() ) ) );
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
		Assert.assertTrue( new N5FSWriter( tempDir.toString() ).remove() );

		System.out.println( "OK" );
	}

//	@Test
	public void testS3Multithreaded() throws ImgLibException, IOException
	{
		final Random rnd = new Random();
		final long[] dimensions = new long[ 3 ];
		for ( int d = 0; d < dimensions.length; ++d )
			dimensions[ d ] = rnd.nextInt( 200 ) + 1;

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

		final String n5Bucket = "test-bucket-n5";

		// make sure old data are cleaned up if there was a failed run
		new N5AmazonS3Writer( AmazonS3ClientBuilder.standard().build(), n5Bucket ).remove();

		final String s3Link = new AmazonS3URI( "s3://" + n5Bucket + "/" ).toString();
		final String datasetPath = "test-channel";

		final Map< String, TileInfo[] > newTiles = TilesToN5ConverterMultithreaded.convertTiffToN5(
				s3Link,
				() -> new N5AmazonS3Writer( AmazonS3ClientBuilder.standard().build(), n5Bucket ),
				Collections.singletonMap( datasetPath, tiles ),
				blockSize,
				new GzipCompression()
			);

		Assert.assertEquals( datasetPath, newTiles.keySet().iterator().next() );

		final TileInfo newTile = newTiles.values().iterator().next()[ 0 ];
		Assert.assertEquals( PathResolver.get( s3Link, datasetPath, impFilename ), newTile.getFilePath() );

		System.out.println( "Loading N5 cell export from:" + PathResolver.get( s3Link, datasetPath ) );

		final RandomAccessibleInterval< UnsignedShortType > rai = TileLoader.loadTile( newTile, DataProviderFactory.createByURI( URI.create( newTile.getFilePath() ) ) );
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
		new N5FSWriter( tempDir.toString() ).remove();
		new N5AmazonS3Writer( AmazonS3ClientBuilder.standard().build(), n5Bucket ).remove();

		System.out.println( "OK" );
	}
}
