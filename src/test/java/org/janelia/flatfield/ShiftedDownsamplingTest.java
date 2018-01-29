package org.janelia.flatfield;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.stitching.Utils;
import org.janelia.util.ImageImporter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.AffineTransform;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class ShiftedDownsamplingTest
{
	private transient JavaSparkContext sparkContext;

	@Before
	public void setUp()
	{
		sparkContext = new JavaSparkContext( new SparkConf()
				.setMaster( "local" )
				.setAppName( "ShiftedDownsampingTest" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ) );
	}

	@After
	public void tearDown()
	{
		sparkContext.close();
	}

	@Test
	public void testPixelsMapping()
	{
		final Interval interval = Intervals.createMinSize( 0, 16 );
		final ShiftedDownsampling< AffineTransform > downsampler = new ShiftedDownsampling<>( sparkContext, interval );

		final long[] expectedDimensionsAtScales = new long[] { 16, 9, 5, 3, 2, 1 };
		final long[] expectedPixelSizeAtScales = new long[] { 1, 2, 4, 8, 16, 32 };
		final long[] expectedOffsetAtScales = new long[] { 0, 1, 2, 4, 8, 16 };

		final int[][] fullPixelToDownsampledPixelAtScales = new int[][] {
			null,
			new int[] { 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8 },
			new int[] { 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4 },
			new int[] { 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2 },
			new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1 },
			new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
		};

		final int[][] downsampledPixelToFullPixelsCountAtScales = new int[][] {
			null,
			new int[] { 1, 2, 2, 2, 2, 2, 2, 2, 1 },
			new int[] { 2, 4, 4, 4, 2 },
			new int[] { 4, 8, 4 },
			new int[] { 8, 8 },
			new int[] { 16 },
		};

		Assert.assertEquals( expectedDimensionsAtScales.length, downsampler.getNumScales() );

		for ( int scale = 0; scale < expectedDimensionsAtScales.length; ++scale )
		{
			try ( final ShiftedDownsampling< AffineTransform >.PixelsMapping pixelsMapping = downsampler.new PixelsMapping( scale ) )
			{
				Assert.assertArrayEquals( new long[] { expectedDimensionsAtScales[ scale ] }, pixelsMapping.getDimensions() );
				Assert.assertArrayEquals( new long[] { expectedPixelSizeAtScales[ scale ] }, pixelsMapping.getPixelSize() );
				Assert.assertArrayEquals( new long[] { expectedOffsetAtScales[ scale ] }, pixelsMapping.getOffset() );

				if ( scale == 0 )
				{
					Assert.assertNull( pixelsMapping.broadcastedFullPixelToDownsampledPixel );
					Assert.assertNull( pixelsMapping.broadcastedDownsampledPixelToFullPixelsCount );
				}
				else
				{
					Assert.assertArrayEquals( fullPixelToDownsampledPixelAtScales[ scale ], pixelsMapping.broadcastedFullPixelToDownsampledPixel.value() );
					Assert.assertArrayEquals( downsampledPixelToFullPixelsCountAtScales[ scale ], pixelsMapping.broadcastedDownsampledPixelToFullPixelsCount.value() );
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static < A extends AffineGet & AffineSet, T extends NativeType< T > & RealType< T > > void main( final String[] args )
	{
		final String filepath = args[ 0 ];

		final ImagePlus imp = ImageImporter.openImage( filepath );
		final RandomAccessibleInterval< T > img = ( RandomAccessibleInterval< T > ) ImagePlusImgs.from( imp );
		final ShiftedDownsampling< A > downsampler = new ShiftedDownsampling<>( img );

		final int restoredScale = 0;
		final List< RandomAccessibleInterval< T > > restoredImgs = new ArrayList<>();
		try ( final ShiftedDownsampling< A >.PixelsMapping pixelsMappingRestored = downsampler.new PixelsMapping( restoredScale ) )
		{
			for ( int lowScale = 0; lowScale < downsampler.getNumScales(); ++lowScale )
			{
				System.out.println( "Processing scale " + lowScale );
				try ( final ShiftedDownsampling< A >.PixelsMapping pixelsMapping = downsampler.new PixelsMapping( lowScale ) )
				{
					final RandomAccessibleInterval< T > downsampledImg = downsampler.downsampleSolutionComponent( img, pixelsMapping );
					final RealRandomAccessible< T > upsampledImg = downsampler.upsample( downsampledImg, restoredScale );
					final RandomAccessibleInterval< T > restoredImg = Views.interval( Views.raster( upsampledImg ), new FinalInterval( pixelsMappingRestored.getDimensions() ) );
					restoredImgs.add( restoredImg );
				}
			}
		}

		final RandomAccessibleInterval< T > restoredImgsStack = Views.stack( restoredImgs );
		final ImagePlus restoredStackImp = ImageJFunctions.wrap( restoredImgsStack, "" );
		Utils.workaroundImagePlusNSlices( restoredStackImp );

		new ImageJ();
		restoredStackImp.show();
	}
}
