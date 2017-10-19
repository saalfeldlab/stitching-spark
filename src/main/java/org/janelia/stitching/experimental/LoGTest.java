package org.janelia.stitching.experimental;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.algorithm.dog.DifferenceOfGaussian;
import net.imglib2.algorithm.gauss3.SeparableSymmetricConvolution;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.Views;

public class LoGTest {

	static String input;

	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		input = args[0];
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( input ) );
		run_dog_nd( tiles[ 0 ], 0.5 );
	}



	static < T extends RealType< T > & NativeType< T > > void run_dog_nd( final TileInfo tile, final double sigma ) throws Exception
	{
		System.out.println("Opening image..");

		final ImagePlus imp = IJ.openImage( tile.getFilePath() );
		Utils.workaroundImagePlusNSlices(imp);
		final Img< T > img = ImagePlusImgs.from( imp );
		final Cursor< T > imgCursor = Views.flatIterable( img ).cursor();

		System.out.println("Copying..");

		final ArrayImg< DoubleType, DoubleArray > vImg = ArrayImgs.doubles( tile.getSize() );
		final Cursor< DoubleType > vImgCursor = Views.flatIterable( vImg ).cursor();

		while ( vImgCursor.hasNext() || imgCursor.hasNext() )
			vImgCursor.next().set( imgCursor.next().getRealDouble() );

		System.out.println("Computing DoG..");
		final ExecutorService threadPool = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
		final double[] sigmaSmaller = new double[ tile.numDimensions() ];
		Arrays.fill( sigmaSmaller, sigma * 0.75 );
		final double[] sigmaLarger = new double[ tile.numDimensions() ];
		Arrays.fill( sigmaLarger, sigma * 1.25 );

		final ExtendedRandomAccessibleInterval< DoubleType, ArrayImg< DoubleType, DoubleArray > > vImgExtended = Views.extendMirrorSingle( vImg );
		final ArrayImg< DoubleType, DoubleArray > vDoGImg = ArrayImgs.doubles( tile.getSize() );
		DifferenceOfGaussian.DoG(sigmaSmaller, sigmaLarger, vImgExtended, vDoGImg, threadPool);

		System.out.println("Computing DoGDoG..");

		final ExtendedRandomAccessibleInterval< DoubleType, ArrayImg< DoubleType, DoubleArray > > vDoGImgExtended = Views.extendMirrorSingle( vDoGImg );
		final ArrayImg< DoubleType, DoubleArray > vDoGDoGImg = ArrayImgs.doubles( tile.getSize() );
		DifferenceOfGaussian.DoG(sigmaSmaller, sigmaLarger, vDoGImgExtended, vDoGDoGImg, threadPool);

		System.out.println("Saving..");

		final ImagePlus vDoGImp = ImageJFunctions.wrap( vDoGImg, "DoG" );
		Utils.workaroundImagePlusNSlices(vDoGImp);
		IJ.saveAsTiff(vDoGImp, Paths.get(input).getParent().toString()+"/DoG_"+sigma+".tif" );

		final ImagePlus vDoGDoGImp = ImageJFunctions.wrap( vDoGDoGImg, "DoGDoG" );
		Utils.workaroundImagePlusNSlices(vDoGDoGImp);
		IJ.saveAsTiff(vDoGDoGImp, Paths.get(input).getParent().toString()+"/DoGDoG_"+sigma+".tif" );

		System.out.println("Shutting thread pool down..");
		threadPool.shutdown();

		System.out.println("Done");
	}




	static < T extends RealType< T > & NativeType< T > > void run_3d( final TileInfo tile, final double sigma ) throws Exception
	{
		final int radius = 3 * (int)Math.ceil(sigma);
		final LaplacianOfGaussian kernelLoG = LaplacianOfGaussian.create( radius, sigma, tile.numDimensions() );

		final double[][][] kernel = new double[ 2*radius + 1 ][ 2*radius + 1 ][ 2*radius + 1 ];
		final double[] flatKernel = ((DoubleArray)kernelLoG.update(null)).getCurrentStorageArray();
		final int[] pos = new int[ tile.numDimensions() ];
		for ( int i = 0; i < flatKernel.length; i++ )
		{
			IntervalIndexer.indexToPosition(i, new int[] {2*radius + 1, 2*radius + 1, 2*radius + 1}, pos);
			kernel[ pos[0] ][ pos[1] ][ pos[2] ] = flatKernel[ i ];
		}

		System.out.println("Opening image..");

		final ImagePlus imp = IJ.openImage( tile.getFilePath() );
		Utils.workaroundImagePlusNSlices(imp);
		final Img< T > img = ImagePlusImgs.from( imp );
		final Cursor< T > imgCursor = Views.flatIterable( img ).cursor();

		System.out.println("Copying..");

		final ArrayImg< DoubleType, DoubleArray > vImg = ArrayImgs.doubles( tile.getSize() );
		final Cursor< DoubleType > vImgCursor = Views.flatIterable( vImg ).cursor();

		while ( vImgCursor.hasNext() || imgCursor.hasNext() )
			vImgCursor.next().set( imgCursor.next().getRealDouble() );

		final ExecutorService threadPool = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );

		System.out.println("Computing LoG..");
		final double[] v_LoG = imfilter_symmetric_3d( vImg.update(null).getCurrentStorageArray(), new int[]{(int) tile.getSize(0),(int) tile.getSize(1),(int) tile.getSize(2)}, kernel, threadPool );

	    for (int c = 0; c < v_LoG.length; c++)
		    v_LoG[c] *= 2;
	    final double[] v_LoG2 = imfilter_symmetric_3d(v_LoG, new int[]{(int) tile.getSize(0),(int) tile.getSize(1),(int) tile.getSize(2)}, kernel, threadPool );

		System.out.println("Saving..");

		final ImagePlus vLoGImp = ImageJFunctions.wrap( ArrayImgs.doubles(v_LoG, tile.getSize()), "vLoG" );
		Utils.workaroundImagePlusNSlices(vLoGImp);
		IJ.saveAsTiff(vLoGImp, Paths.get(input).getParent().toString()+"/3d-LoG_"+sigma+".tif" );

		final ImagePlus vLoGLoGImp = ImageJFunctions.wrap( ArrayImgs.doubles(v_LoG2, tile.getSize()), "vLoGLoG" );
		Utils.workaroundImagePlusNSlices(vLoGLoGImp);
		IJ.saveAsTiff(vLoGLoGImp, Paths.get(input).getParent().toString()+"/3d-LoGLoG_"+sigma+".tif" );

		System.out.println("Shutting thread pool down..");
		threadPool.shutdown();

		System.out.println("Done");
	}

	/*static < T extends RealType< T > & NativeType< T > > void run_nd( final TileInfo tile, final double sigma ) throws Exception
	{
		final int radius = 3 * (int)Math.ceil(sigma);
		final LaplacianOfGaussian kernelLoG = LaplacianOfGaussian.create( radius, sigma, tile.numDimensions() );

		final double[][] halfKernels = new double[ tile.numDimensions() ][ radius + 1 ];
		for ( int d = 0; d < halfKernels.length; d++ )
		{
			final long[] mins = new long[ kernelLoG.numDimensions() ], maxs = new long[ kernelLoG.numDimensions() ];
			Arrays.fill( mins, radius );
			Arrays.fill( maxs, radius );
			mins[ d ] = radius;
			maxs[ d ] = kernelLoG.max(d);
			final Cursor< DoubleType > kernel1DCursor = Views.flatIterable( Views.interval( kernelLoG, new FinalInterval( mins, maxs ) ) ).localizingCursor();
			while ( kernel1DCursor.hasNext() )
			{
				kernel1DCursor.fwd();
				halfKernels[ d ][ kernel1DCursor.getIntPosition(d) - radius ] = kernel1DCursor.get().get();
			}
		}

		System.out.println("Opening image..");

		final ImagePlus imp = IJ.openImage( tile.getFilePath() );
		Utils.workaroundImagePlusNSlices(imp);
		final Img< T > img = ImagePlusImgs.from( imp );
		final Cursor< T > imgCursor = Views.flatIterable( img ).cursor();

		System.out.println("Copying..");

		final ArrayImg< DoubleType, DoubleArray > vImg = ArrayImgs.doubles( tile.getSize() );
		final Cursor< DoubleType > vImgCursor = Views.flatIterable( vImg ).cursor();

		while ( vImgCursor.hasNext() || imgCursor.hasNext() )
			vImgCursor.next().set( imgCursor.next().getRealDouble() );

		System.out.println("Computing LoG..");
		final ExecutorService threadPool = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );

		final ExtendedRandomAccessibleInterval< DoubleType, ArrayImg< DoubleType, DoubleArray > > vImgExtended = Views.extendMirrorSingle( vImg );
		final ArrayImg< DoubleType, DoubleArray > vLoGImg = ArrayImgs.doubles( tile.getSize() );
		SeparableSymmetricConvolution.convolve( halfKernels, vImgExtended, vLoGImg, threadPool );

		System.out.println("Computing LoGLoG..");

		final ExtendedRandomAccessibleInterval< DoubleType, ArrayImg< DoubleType, DoubleArray > > vLoGImgExtended = Views.extendMirrorSingle( vLoGImg );
		final ArrayImg< DoubleType, DoubleArray > vLoGLoGImg = ArrayImgs.doubles( tile.getSize() );
		SeparableSymmetricConvolution.convolve( halfKernels, vLoGImgExtended, vLoGLoGImg, threadPool );

		System.out.println("Saving..");

		ImagePlus vLoGImp = ImageJFunctions.wrap( vLoGImg, "LoG" );
		Utils.workaroundImagePlusNSlices(vLoGImp);
		IJ.saveAsTiff(vLoGImp, Paths.get(input).getParent().toString()+"/LoG_"+sigma+".tif" );

		ImagePlus vLoGLoGImp = ImageJFunctions.wrap( vLoGLoGImg, "LoGLoG" );
		Utils.workaroundImagePlusNSlices(vLoGLoGImp);
		IJ.saveAsTiff(vLoGLoGImp, Paths.get(input).getParent().toString()+"/LoGLoG_"+sigma+".tif" );

		System.out.println("Shutting thread pool down..");
		threadPool.shutdown();

		System.out.println("Done");
	}*/

	static void run_2d_cidre( final TileInfo tile, final double sigma ) throws Exception
	{
		final int radius = 3 * (int)Math.ceil(sigma);

		System.out.println("Opening image..");
		final ImagePlus imp = IJ.openImage( tile.getFilePath() );
		Utils.workaroundImagePlusNSlices(imp);

		System.out.println("Copying..");
		final float[][] arr = imp.getProcessor().getFloatArray();
		final double[] flat = new double[ arr.length*arr[0].length ];
		for ( int x = 0; x < arr.length; x++ )
			for ( int y = 0; y < arr[x].length; y++ )
				flat[ IntervalIndexer.positionToIndex( new int[] {x,y}, new int[] {arr.length,arr[0].length} ) ] = arr[ x ][ y ];

		System.out.println("Computing..");
		// define the kernel size, make certain dimension is odd
	    int hsize = 6 * (int)Math.ceil(sigma);
	    if (hsize % 2 == 0)
	        hsize++;
	    final double std2 = sigma * sigma;

	    // h{n} = sigmas(n)^2 * fspecial('log', hsize, sigmas(n))
	    final double[][] h = new double[hsize][hsize];
	    final double[][] h1 = new double[hsize][hsize];
	    double sumh = 0.0;
	    for (int c = 0; c < hsize; c++) {
	    	for (int r = 0; r < hsize; r++) {
	    		final double arg = -1.0 * ((c-hsize/2)*(c-hsize/2) + (r-hsize/2)*(r-hsize/2)) / (2.0*std2);
	    		h[c][r] = Math.exp(arg);
	    		sumh += h[c][r];
	    	}
	    }
	    // calculate Laplacian
	    double sumh1 = 0.0;
	    for (int c = 0; c < hsize; c++) {
	    	for (int r = 0; r < hsize; r++) {
	    		h[c][r] /= sumh;
	    		h1[c][r] = h[c][r] * ((c-hsize/2)*(c-hsize/2) + (r-hsize/2)*(r-hsize/2) - 2 * std2) / (std2 * std2);
	    		sumh1 += h1[c][r];
	    	}
	    }
	    for (int c = 0; c < hsize; c++) {
	    	for (int r = 0; r < hsize; r++) {
	    		h[c][r] = (h1[c][r] - sumh1/(hsize*hsize)) * (sigma * sigma); // h{n} = sigmas(n)^2 * fspecial('log', hsize, sigmas(n));
	    	}
	    }


	    System.out.println("Kernel:");
		for (int c = 0; c < hsize; c++)
		{
			System.out.print("[");
			for (int r = 0; r < hsize; r++)
				System.out.print( String.format("%.3f  ", h[c][r] ) );
			System.out.println("]");
		}


	    // apply a LoG filter to v_img to penalize disagreements between neighbors
	    final double[] v_LoG = imfilter_symmetric_2d(flat, arr.length, arr[0].length, h);

	    //for (int c = 0; c < v_LoG.length; c++)
		//    v_LoG[c] *= 2;
	    final double[] v_LoG2 = imfilter_symmetric_2d(v_LoG, arr.length, arr[0].length, h);


		System.out.println("Saving..");
		final ImagePlus vLoGImp = ImageJFunctions.wrap( ArrayImgs.doubles(v_LoG, arr.length, arr[0].length), "vLoG" );
		Utils.workaroundImagePlusNSlices(vLoGImp);
		IJ.saveAsTiff(vLoGImp, Paths.get(input).getParent().toString()+"/2d-LoG_"+sigma+".tif" );

		final ImagePlus vLoGLoGImp = ImageJFunctions.wrap( ArrayImgs.doubles(v_LoG2, arr.length, arr[0].length), "vLoGLoG" );
		Utils.workaroundImagePlusNSlices(vLoGLoGImp);
		IJ.saveAsTiff(vLoGLoGImp, Paths.get(input).getParent().toString()+"/2d-LoGLoG_"+sigma+".tif" );

		System.out.println("Done");
	}

	private static double[] imfilter_symmetric_2d(final double[] pixels, final int width, final int height, final double[][] k)
	{
		final int kc = k.length / 2;
		final double[] kernel = new double[k.length * k.length];

		final double[] result = new double[width*height];

		for (int i = 0; i < k.length; i++)
			for (int j = 0; j < k.length; j++)
				kernel[i * k.length + j] = k[i][j];

		double sum;
		int offset, i;
		int edgeDiff;
		boolean edgePixel;
		final int xedge = width - kc;
		final int yedge = height - kc;
		int nx, ny;

		for (int x = 0; x < width; x++) {
			for (int y = 0; y < height; y++) {
				sum = 0;
				i = 0;
				edgePixel = x < kc || x >= xedge || y < kc || y >= yedge;
				for (int u = -kc; u <= kc; u++) {
					offset = (x+u)*height + y;
					for (int v = -kc; v <= kc; v++) {
						if (edgePixel) {
							nx = x + u;
							ny = y + v;
							edgeDiff = 0;
							if (nx < 0)
								edgeDiff = (-2*nx - 1) * height;
							else if (nx >= width)
								edgeDiff = (-2 * (nx - width) - 1) * height;

							if (ny < 0)
								edgeDiff += -2*ny - 1;
							else if (ny >= height)
								edgeDiff += -2 * (ny - height) - 1;

							sum += pixels[offset + v + edgeDiff] * kernel[i++];
						}
						else
						{
							sum += pixels[offset + v] * kernel[i++];
						}
					}
				}
				result[x * height + y] = sum;
			}
		}

		return result;
	}


	static < T extends RealType< T > & NativeType< T > > void run_2d_my( final TileInfo tile, final double sigma ) throws Exception
	{
		final int radius = 3 * (int)Math.ceil(sigma);
		final LaplacianOfGaussian kernelLoG = LaplacianOfGaussian.create( radius, sigma, 2 );


		final double[][] kernel = new double[ 2*radius + 1 ][ 2*radius + 1 ];
		final double[] flatKernel = ((DoubleArray)kernelLoG.update(null)).getCurrentStorageArray();
		final int[] pos = new int[ 2 ];
		for ( int i = 0; i < flatKernel.length; i++ )
		{
			IntervalIndexer.indexToPosition(i, new int[] {2*radius + 1, 2*radius + 1}, pos);
			kernel[ pos[0] ][ pos[1] ] = flatKernel[ i ];
		}
		System.out.println("Kernel:");
		for (int c = 0; c < kernel.length; c++)
		{
			System.out.print("[");
			for (int r = 0; r < kernel.length; r++)
				System.out.print( String.format("%.3f  ", kernel[c][r] ) );
			System.out.println("]");
		}



		final double[][] halfKernels = new double[ 2 ][ radius + 1 ];
		for ( int d = 0; d < halfKernels.length; d++ )
		{
			final long[] mins = new long[ kernelLoG.numDimensions() ], maxs = new long[ kernelLoG.numDimensions() ];
			Arrays.fill( mins, radius );
			Arrays.fill( maxs, radius );
			mins[ d ] = radius;
			maxs[ d ] = kernelLoG.max(d);
			final Cursor< DoubleType > kernel1DCursor = Views.flatIterable( Views.interval( kernelLoG, new FinalInterval( mins, maxs ) ) ).localizingCursor();
			while ( kernel1DCursor.hasNext() )
			{
				kernel1DCursor.fwd();
				halfKernels[ d ][ kernel1DCursor.getIntPosition(d) - radius ] = kernel1DCursor.get().get();
			}
		}

		System.out.println("Half kernel:");
		for (int c = 0; c < halfKernels.length; c++)
			System.out.println(Arrays.toString(halfKernels[c]));

		System.out.println("Opening image..");

		final ImagePlus imp = IJ.openImage( tile.getFilePath() );
		Utils.workaroundImagePlusNSlices(imp);
		final float[][] arr = imp.getProcessor().getFloatArray();

		System.out.println("Copying..");
		final double[] flat = new double[ arr.length*arr[0].length ];
		for ( int x = 0; x < arr.length; x++ )
			for ( int y = 0; y < arr[x].length; y++ )
				flat[ IntervalIndexer.positionToIndex( new int[] {x,y}, new int[] {arr.length,arr[0].length} ) ] = arr[ x ][ y ];

		final ArrayImg< DoubleType, DoubleArray > vImg = ArrayImgs.doubles( flat, new long[] { tile.getSize(0), tile.getSize(1) } );
		final ExecutorService threadPool = Executors.newSingleThreadExecutor();

		System.out.println("Computing LoG..");

		final ExtendedRandomAccessibleInterval< DoubleType, ArrayImg< DoubleType, DoubleArray > > vImgExtended = Views.extendMirrorSingle( vImg );
		final ArrayImg< DoubleType, DoubleArray > vLoGImg = ArrayImgs.doubles( new long[] { tile.getSize(0), tile.getSize(1) } );
		SeparableSymmetricConvolution.convolve( halfKernels, vImgExtended, vLoGImg, threadPool );

		System.out.println("Computing LoGLoG..");

		final ExtendedRandomAccessibleInterval< DoubleType, ArrayImg< DoubleType, DoubleArray > > vLoGImgExtended = Views.extendMirrorSingle( vLoGImg );
		final ArrayImg< DoubleType, DoubleArray > vLoGLoGImg = ArrayImgs.doubles( new long[] { tile.getSize(0), tile.getSize(1) } );
		SeparableSymmetricConvolution.convolve( halfKernels, vLoGImgExtended, vLoGLoGImg, threadPool );

		System.out.println("Saving..");

		final ImagePlus vLoGImp = ImageJFunctions.wrap( vLoGImg, "LoG" );
		Utils.workaroundImagePlusNSlices(vLoGImp);
		IJ.saveAsTiff(vLoGImp, Paths.get(input).getParent().toString()+"/2d-my-LoG_"+sigma+".tif" );

		final ImagePlus vLoGLoGImp = ImageJFunctions.wrap( vLoGLoGImg, "LoGLoG" );
		Utils.workaroundImagePlusNSlices(vLoGLoGImp);
		IJ.saveAsTiff(vLoGLoGImp, Paths.get(input).getParent().toString()+"/2d-my-LoGLoG_"+sigma+".tif" );

		System.out.println("Shutting thread pool down..");
		threadPool.shutdown();

		System.out.println("Done");
	}



	/*private static < T extends RealType< T > & NativeType< T > > Img< T > imfilter_symmetric( final Img< T > img, final Img< DoubleType > kernel ) throws Exception
	{
		for ( int d = 1; d < kernel.numDimensions(); d++ )
			if ( kernel.dimension(d) != kernel.dimension(d-1) )
				throw new Exception( "Expected square kernel" );
		for ( int d = 0; d < kernel.numDimensions(); d++ )
			if ( kernel.dimension(d) % 2 != 0 )
				throw new Exception( "Expected odd sided kernel" );

		int kc = (int) (kernel.dimension(0) / 2);

		final Img< T > result = new ImagePlusImgFactory< T >().create( Intervals.dimensionsAsLongArray(img), img.firstElement().createVariable() );


		final Cursor< T > imgCursor = Views.flatIterable( img ).localizingCursor();
		final Cursor< T > resultCursor = Views.flatIterable( result ).cursor();
		final int[] pos = new int[ kernel.numDimensions() ];

		while ( imgCursor.hasNext() || resultCursor.hasNext() )
		{
			imgCursor.fwd();
			resultCursor.fwd();

			imgCursor.localize(pos);

			boolean edgePixel = false;
			for ( int d = 0; d < kernel.numDimensions(); d++ )
				if ( pos[d] < kc || pos[d] >= img.dimension(d) - kc )
					edgePixel = true;

			double sum = 0;

		}


		int edgeDiff;
		boolean edgePixel;
		int xedge = width - kc;
		int yedge = height - kc;
		int nx, ny;

		for (int x = 0; x < width; x++) {
			for (int y = 0; y < height; y++) {
				sum = 0;
				i = 0;
				edgePixel = x < kc || x >= xedge || y < kc || y >= yedge;
				for (int u = -kc; u <= kc; u++) {
					offset = (x+u)*height + y;
					for (int v = -kc; v <= kc; v++) {
						if (edgePixel) {
							nx = x + u;
							ny = y + v;
							edgeDiff = 0;
							if (nx < 0)
								edgeDiff = (-2*nx - 1) * height;
							else if (nx >= width)
								edgeDiff = (-2 * (nx - width) - 1) * height;

							if (ny < 0)
								edgeDiff += -2*ny - 1;
							else if (ny >= height)
								edgeDiff += -2 * (ny - height) - 1;

							sum += pixels[offset + v + edgeDiff] * kernel[i++];
						}
						else
						{
							sum += pixels[offset + v] * kernel[i++];
						}
					}
				}
				result[x * height + y] = sum;
			}
		}

		return result;
	}*/

	private static double[] imfilter_symmetric_3d(final double[] pixels, final int[] dimensions, final double[][][] k, final ExecutorService threadPool ) throws Exception
	{
		final int kc = k.length / 2;
		final double[] kernel = new double[(int) Math.pow( k.length, 3 )];
		for (int x = 0; x < k.length; x++)
			for (int y = 0; y < k.length; y++)
				for (int z = 0; z < k.length; z++)
					kernel[ IntervalIndexer.positionToIndex(new int[] {x,y,z}, new int[] {k.length,k.length,k.length})] = k[x][y][z];

		final double[] result = new double[pixels.length];

		final int numThreads = Runtime.getRuntime().availableProcessors();
		final Future< ? >[] futures = new Future[ numThreads ];
		final AtomicInteger ai = new AtomicInteger();

		for ( int ithread = 0; ithread < numThreads; ++ithread )
			futures[ ithread ] = threadPool.submit(() -> {
				final int myNumber = ai.getAndIncrement();
				final int wholeSize = pixels.length;
				final int chunk = (wholeSize / numThreads) + (wholeSize % numThreads == 0 ? 0 : 1);
				final int startInd = chunk * myNumber;
				final int endInd = Math.min(startInd+chunk, wholeSize);

				final int[] pos = new int[ dimensions.length ], coords = new int[ dimensions.length ];
				for (int pixel = startInd; pixel < endInd; pixel++)
				{
					IntervalIndexer.indexToPosition(pixel, dimensions, pos);
					final int x = pos[0], y = pos[1], z = pos[2];

					double sum = 0;
					int counter = 0;

					for (int u = -kc; u <= kc; u++) {
						for (int v = -kc; v <= kc; v++) {
							for (int w = -kc; w <= kc; w++) {

								coords[0]=x+u;
								coords[1]=y+v;
								coords[2]=z+w;

								for( int d = 0; d < coords.length; d++ )
									if (coords[d] < 0)
										coords[d] *= -1;
									else
									if (coords[d] >= dimensions[d])
										coords[d] = 2 * (dimensions[d] - 1) - coords[d];

								sum += pixels[IntervalIndexer.positionToIndex(coords, dimensions)] * kernel[counter++];
							}
						}
					}
					result[pixel] = sum;
				}
			});
		for ( final Future< ? > future : futures )
			future.get();

		return result;
	}
}
