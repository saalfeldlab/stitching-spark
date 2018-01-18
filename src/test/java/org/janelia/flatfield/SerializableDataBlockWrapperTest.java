package org.janelia.flatfield;

import java.io.IOException;
import java.util.Random;

import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.Lz4Compression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.XzCompression;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.list.ListCursor;
import net.imglib2.img.list.WrappedListImg;
import net.imglib2.view.Views;

public class SerializableDataBlockWrapperTest
{
	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-test";
	static private final String datasetName = "/test/group/dataset";
	static private final long[] dimensions = new long[]{100, 200, 300};
	static private final int[] blockSize = new int[]{44, 33, 22};

	static private Random rnd = new Random();

	protected static N5Writer n5;

	protected Compression[] getCompressions() {

		return new Compression[] {
				new RawCompression(),
				new Bzip2Compression(),
				new GzipCompression(),
				new Lz4Compression(),
				new XzCompression()
			};
	}

	@BeforeClass
	public static void setUpBeforeClass() throws IOException
	{
		n5 = new N5FSWriter( testDirPath );
	}

	@AfterClass
	public static void rampDownAfterClass() throws IOException
	{
		Assert.assertTrue( n5.remove() );
	}

	@Test
	public void testSerializableTypeString() throws IOException {

		for (final Compression compression : getCompressions()) {
			System.out.println("Testing " + compression.getType() + " serializable type with String");
			try {
				n5.createDataset(datasetName, dimensions, blockSize, DataType.SERIALIZABLE, compression);
				final SerializableDataBlockWrapper< String > dataBlockWrapper = new SerializableDataBlockWrapper<>( n5, datasetName, new long[]{0, 0, 0} );
				final WrappedListImg< String > dataBlockWrapped = dataBlockWrapper.wrap();
				final ListCursor< String > dataBlockCursor = dataBlockWrapped.cursor();
				while ( dataBlockCursor.hasNext() )
				{
					final int len = rnd.nextInt(20);
					final byte[] bytes = new byte[len];
					rnd.nextBytes(bytes);
					dataBlockCursor.fwd();
					dataBlockCursor.set( new String( bytes ) );
				}
				dataBlockWrapper.save();

				final SerializableDataBlockWrapper< String > loadedDataBlockWrapper = new SerializableDataBlockWrapper<>( n5, datasetName, new long[]{0, 0, 0} );
				final RandomAccessibleInterval< String > loadedDataBlockWrapped = loadedDataBlockWrapper.wrap();
				final Cursor< String > loadedDataBlockPlainCursor = Views.flatIterable( loadedDataBlockWrapped ).cursor();
				final Cursor< String > dataBlockPlainCursor = Views.flatIterable( dataBlockWrapped ).cursor();
				while ( dataBlockPlainCursor.hasNext() || loadedDataBlockPlainCursor.hasNext() )
					Assert.assertEquals( dataBlockPlainCursor.next(), loadedDataBlockPlainCursor.next() );

				Assert.assertTrue(n5.remove(datasetName));

			} catch (final IOException e) {
				Assert.fail(e.getMessage());
			}
		}
	}
}
