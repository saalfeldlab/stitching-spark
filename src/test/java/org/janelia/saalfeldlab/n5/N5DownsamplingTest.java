package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.junit.Assert;
import org.junit.Test;

import net.imglib2.img.array.ArrayImgs;
import net.imglib2.util.Intervals;

public class N5DownsamplingTest
{
	static private final String testDirPath = System.getProperty("user.home") + "/tmp/n5-downsampling-test";

	@Test
	public void testIsotropicDownsampling() throws IOException
	{
		final N5Writer n5 = N5.openFSWriter( testDirPath );
		final long[] dimensions = new long[] { 4, 4, 4 };
		final int[] cellSize = new int[] { 1, 1, 1 };

		final byte[] data = new byte[ ( int ) Intervals.numElements( dimensions ) ];
		for ( byte i = 0; i < data.length; ++i )
			data[ i ] = i;

		final String datasetPath = "c0";
		N5Utils.save( ArrayImgs.bytes( data, dimensions ), n5, Paths.get( datasetPath, "s0" ).toString(), cellSize, CompressionType.GZIP );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local" )
				.setAppName( "N5DownsamplingTest" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			new N5DownsamplingSpark<>( sparkContext ).downsampleIsotropic( testDirPath, datasetPath );

			Assert.assertTrue(
					Paths.get( testDirPath, datasetPath ).toFile().listFiles().length == 2 &&
					n5.datasetExists( Paths.get( datasetPath, "s0" ).toString() ) &&
					n5.datasetExists( Paths.get( datasetPath, "s1" ).toString() ) );

			final String s1Path = Paths.get( datasetPath, "s1" ).toString();
			final DatasetAttributes s1Attributes = n5.getDatasetAttributes( s1Path );
			for ( final byte zCoord : new byte[] { 0, 1 } )
			{
				final byte zOffset = ( byte ) ( zCoord * 32 );
				Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( ( Math.round( zOffset + ( 0  + 1  + 4  + 5  ) / 4. ) + Math.round( zOffset + ( 16 + 17 + 20 + 21 ) / 4. ) ) / 2. ) }, ( byte[] ) n5.readBlock( s1Path, s1Attributes, new long[] { 0, 0, zCoord } ).getData() );
				Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( ( Math.round( zOffset + ( 2  + 3  + 6  + 7  ) / 4. ) + Math.round( zOffset + ( 18 + 19 + 22 + 23 ) / 4. ) ) / 2. ) }, ( byte[] ) n5.readBlock( s1Path, s1Attributes, new long[] { 1, 0, zCoord } ).getData() );
				Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( ( Math.round( zOffset + ( 8  + 9  + 12 + 13 ) / 4. ) + Math.round( zOffset + ( 24 + 25 + 28 + 29 ) / 4. ) ) / 2. ) }, ( byte[] ) n5.readBlock( s1Path, s1Attributes, new long[] { 0, 1, zCoord } ).getData() );
				Assert.assertArrayEquals( new byte[] { ( byte ) Math.round( ( Math.round( zOffset + ( 10 + 11 + 14 + 15 ) / 4. ) + Math.round( zOffset + ( 26 + 27 + 30 + 31 ) / 4. ) ) / 2. ) }, ( byte[] ) n5.readBlock( s1Path, s1Attributes, new long[] { 1, 1, zCoord } ).getData() );
			}

			n5.remove( "" );
		}
	}
}
