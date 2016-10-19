package org.janelia.stitching;

import java.awt.Dimension;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import bdv.export.Downsample;
import ij.IJ;
import ij.ImagePlus;
import mpicbg.stitching.ImageCollectionElement;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Scale;
import net.imglib2.realtransform.Scale3D;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.RandomAccessibleOnRealRandomAccessible;
import net.imglib2.view.Views;

public class IlluminationCorrectionSpark implements Runnable, Serializable
{
	private static final long serialVersionUID = 1920621620902670130L;
	
	private static final double WINDOW_POINTS_PERCENT = 0.125;
	
	private transient int numPixels;
	
	private transient int N;	// stack size
	private transient double[] Q;
	
	private enum Mestimator { LS, CAUCHY };
	private transient double CAUCHY_W;		// width of the Cauchy function used for robust regression 
	private transient double PivotShiftX;		// shift on the Q axis into the pivot space 
	private transient double[] PivotShiftY;	// shift on the q axis into the pivot space 
	private transient int ITER;				// iteration count for the optimization
	private transient Mestimator MESTIMATOR;	// specifies "CAUCHY" or "LS" (least squares) 
	private transient int TERMSFLAG;			// flag specifing which terms to include in the energy function 
	private transient double LAMBDA_VREG;		// coefficient for the v regularization 
	private transient double LAMBDA_ZERO = Math.pow(10, 0.5);		// coefficient for the zero-light term
	private transient double ZMIN;			// minimum possible value for Z
	private transient double ZMAX;			// maximum possible value for Z
	private transient double STACKMIN;
	private transient int maxLbgfsIterations = 500;
	
	
	private static final int maxIter = 500;			// max iterations for optimization
	private static final int MaxFunEvals = 1000;	// max evaluations of objective function
	private static final double progTol = 1e-5;		// progress tolerance
	private static final double optTol = 1e-5;		// optimality tolerance
	private static final int Corr = 100;			// number of corrections to store in memory
	
	
	private static int downsampleFactor = 4;
	private static int histSize = 256;
	private static double histMin = 100.0, histMax = 313.0;
	
	
	
	private static String inputFilepath;
	private transient JavaSparkContext sparkContext;
	private transient TileInfo[] tiles;
	private transient TileInfo[] correctedTiles;
	
	private static long[] originalSize;
	private transient long[] downsampledSize;
	
	private transient CompressedStack histograms;
	private transient double[] binsMean;
	
	// Solution
	private transient double[] vFinal;
	private transient double[] zFinal;
	
	
	public static void main( final String[] args )
	{
		final IlluminationCorrectionSpark driver = new IlluminationCorrectionSpark( args[ 0 ] );
		driver.run();
	}
	

	public IlluminationCorrectionSpark( final String inputFilepath ) 
	{
		this.inputFilepath = inputFilepath;
	}

	@Override
	public void run()
	{
		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "IlluminationCorrection" )
				.set( "spark.driver.maxResultSize", "8g" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryoserializer.buffer.max", "2000m" ));
		
		
		try {
			tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilepath );
			N = tiles.length;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		// check if all tiles have the same size
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				if ( tile.getSize(d) != tiles[0].getSize(d) )
				{
					System.out.println("Assumption failed: some tiles have different size");
					System.exit(1);
				}
		
		
		if ( !readHistogramsFromDisk() )
		{
			long elapsed = System.nanoTime();
			histograms = getHistograms();
			elapsed = System.nanoTime() - elapsed;
			
			System.out.println( "Size of the resulting array: " + histograms.hist.length + " x " + histograms.hist[ 0 ].length );
			System.out.println( "min=" + histograms.min + ", max=" + histograms.max );
			System.out.println( "Done" );
			System.out.println( "Elapsed time: " + (int)(elapsed / Math.pow(10, 9)) + "s" );
			
			numPixels = histograms.hist[0].length;
			
			if ( dumpHistogramsToDisk() )
				System.out.println("Saved");
				//return;
		}
		else
		{
			System.out.println("Loaded histograms from disk");
		}
		
		int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
		for ( int pixel = 0; pixel < histograms.hist[ 0 ].length; pixel++ )
		{			
			for ( int bin = 0; bin < histograms.hist.length; bin++ )
			{
				if (min > histograms.hist[ bin ][ pixel ])
					min = histograms.hist[ bin ][ pixel ];
				if (max < histograms.hist[ bin ][ pixel ])
					max = histograms.hist[ bin ][ pixel ];
			}	
		}
		System.out.println("Min freq=" + min + ", max freq=" + max);
		
		
		originalSize = getMinSize(tiles);
		downsampledSize = new long[ originalSize.length ];
		for ( int d = 0; d < originalSize.length; d++ )
			downsampledSize[ d ] = originalSize[ d ] / downsampleFactor;

		
		/*System.out.println("Average histogram");
		for ( int bin = 0; bin < histSize; bin++ )
		{
			double avBin = 0;
			for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
				avBin += (double)histograms.hist[bin][pixelIndex] / N;
			avBin /= numPixels;
			System.out.println(avBin);
		}*/
		System.out.println("--------------");
		
		
		
		/*A: for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
		{
			double[] x =  new double[histSize];
			boolean nonzero = false;
			for ( int bin = histSize - 1; bin >= 0; bin-- )
			{
				if ( !nonzero && histograms.hist[bin][pixelIndex] != 0 )
				{
					if ( bin < 200 )
						continue A;
					nonzero = true;
				}
				x[bin] = (double)histograms.hist[bin][pixelIndex] / N;
			}
			System.out.println("pixel="+pixelIndex+", h="+Arrays.toString(x));
		}*/
		
		// precalc middle values of every bin
		binsMean = new double[ histSize ];
		final double histBinSize = (histMax-histMin) / histSize;
		for ( int binIndex = 0; binIndex < histSize; binIndex++ )
			binsMean[ binIndex ] = histMin + binIndex*histBinSize + histBinSize/2;
		//System.out.println("binsMean="+Arrays.toString(binsMean));
		
		estimateQ();
		
		// initial guesses for the correction surfaces
		double[] v0 = new double[ numPixels ];
		double[] b0 = new double[ numPixels ];
		Arrays.fill( v0, 1.0 );
		
		
		
		PivotShiftY = new double[ numPixels ];
/*
		// Transform Q and S (which contains q) to the pivot space.
		// The pivot space is just a shift of the origin to the median datum. First, the shift for Q:
		final int mid_ind = (histSize-1) / 2;
		PivotShiftX = Q[mid_ind];
		
		for (int i = 0; i < N; i++)
			Q[i] -= PivotShiftX;

		// next, the shift for each location q
		PivotShiftY = new double[ numPixels ];
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
			PivotShiftY[ pixelIndex ] = sortedPixels[ pixelIndex ][ mid_ind ];
		
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
			for ( int i = 0; i < N; i++ ) 
				sortedPixels[ pixelIndex ][ i ] -= PivotShiftY[ pixelIndex ];
		
		// also, account for the pivot shift in b0
		//b0 = b0 + PivotShiftX*v0 - PivotShiftY;
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
			b0[ pixelIndex ] = PivotShiftX - PivotShiftY[ pixelIndex ];
*/
		
		
		// some parameters initialization
		STACKMIN = histograms.min;
		ZMAX = STACKMIN;
		final double zx0 = 0.85 * STACKMIN, zy0 = zx0;
		
		
		// vector containing initial values of the variables we want to estimate
		//x0 = [v0(:); b0(:); zx0; zy0];
		final double[] x0 = new double[2 * numPixels + 2];
		int pX = 0;
		for (int i = 0; i < v0.length; i++)
			x0[pX++] = v0[i];
		for (int i = 0; i < b0.length; i++)
			x0[pX++] = b0[i];
		x0[pX++] = zx0;
		x0[pX++] = zy0;
		
		final MinFuncOptions minFuncOptions = new MinFuncOptions();
		minFuncOptions.maxIter      = maxLbgfsIterations;			// max iterations for optimization
		minFuncOptions.MaxFunEvals  = 1000;							// max evaluations of objective function
		minFuncOptions.progTol      = 1e-5;							// progress tolerance
		minFuncOptions.optTol       = 1e-5;							// optimality tolerance
		minFuncOptions.Corr         = 100;							// number of corrections to store in memory (default: 100)*/
		
		// First call to roughly estimate our variables
		ITER = 1;
		MESTIMATOR = Mestimator.LS;
		TERMSFLAG = 0;
		MinFuncResult minFuncResult = minFunc( x0, minFuncOptions );
		
		double[] x  = minFuncResult.x;
		double fval = minFuncResult.f;

		// unpack
		final double[] v1 = Arrays.copyOfRange(x, 0, numPixels);
		final double[] b1 = Arrays.copyOfRange(x, numPixels, 2*numPixels);
		final double zx1 = x[2 * numPixels];
		final double zy1 = x[2 * numPixels + 1];
		
		
		
		
		
		System.out.println("-------------");
		System.out.println("-------------");
		// 2nd optimization using REGULARIZED ROBUST fitting
		// use the mean standard error of the LS fitting to set the width of the
		// CAUCHY function
		CAUCHY_W = computeStandardError(v1, b1);
		
		System.out.println("CAUCHY_W="+CAUCHY_W);

		// assign the remaining global variables needed in cdr_objective
		ITER = 1;
		MESTIMATOR = Mestimator.CAUCHY;
		TERMSFLAG = 1;

		// vector containing initial values of the variables we want to estimate
		double[] x1 = new double[2 * numPixels + 2];
		
		int pX1 = 0;
		for (int i = 0; i < v1.length; i++)
			x1[pX1++] = v1[i];
		for (int i = 0; i < b1.length; i++)
			x1[pX1++] = b1[i];
		x1[pX1++] = zx1;
		x1[pX1++] = zy1;		

		minFuncResult = minFunc(x1, minFuncOptions);		
		x = minFuncResult.x;
		fval = minFuncResult.f;		

		// unpack the optimized v surface, b surface, xc, and yc from the vector x
		double[] v = Arrays.copyOfRange(x, 0, numPixels);
		double[] b = Arrays.copyOfRange(x, numPixels, 2*numPixels);
		double zx = x[2 * numPixels];
		double zy = x[2 * numPixels + 1];

		// Build the final correction model 
/*
		// Unpivot b: move pivot point back to the original location
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
			b[pixelIndex] += PivotShiftY[pixelIndex] - PivotShiftX * v[pixelIndex];
*/		
		// shift the b surface to the zero-light surface
		double[] z = new double[numPixels];
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
			z[pixelIndex] = b[pixelIndex] + zx * v[pixelIndex];

		
		vFinal = rescaleHelper( v, "V" );
		zFinal = rescaleHelper( z, "Z" );
		
		correctImages();
		
		sparkContext.close();
		System.out.println("Done");
	}
	
	
	private < T extends RealType< T > & NativeType< T > > CompressedStack getHistograms()
	{
		final int treeDepth = (int) Math.ceil( Math.log( sparkContext.defaultParallelism() ) / Math.log( 2 ) );
		System.out.println( "default parallelism = " + sparkContext.defaultParallelism() + ",  tree depth = " + treeDepth );
		
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
		
		final CompressedStack result = rdd.treeAggregate( 
				null, // zero value 
				
				// generator
				new Function2< CompressedStack, TileInfo, CompressedStack >()
				{
					private static final long serialVersionUID = -4991255417353136684L;

					@Override
					public CompressedStack call( final CompressedStack intermediateHist, final TileInfo tile ) throws Exception
					{
						System.out.println( "Loading tile " + tile.getIndex() );
						final ImagePlus imp = IJ.openImage( tile.getFilePath() );
						Utils.workaroundImagePlusNSlices( imp );
						final Img< T > img = ImagePlusImgs.from( imp );
						final T type = ( T ) ImageType.valueOf( imp.getType() ).getType();
						
						// Downsample by some factor
						final int[] outDimensions = new int[ tile.numDimensions() ];
						for ( int d = 0; d < outDimensions.length; d++ )
							outDimensions[ d ] = ( int ) Math.floor( ( ( double ) tile.getSize( d ) / downsampleFactor ) );

						final int[] dimFactors = new int[ tile.numDimensions() ];
						Arrays.fill( dimFactors, downsampleFactor );

						System.out.println( "Downsampling" );
						final Img< T > downsampledImg = new ImagePlusImgFactory< T >().create( outDimensions, type.createVariable() );
						Downsample.downsample( img, downsampledImg, dimFactors );
						imp.close();
						
						// Fill the histogram
						final CompressedStack result;
						if ( intermediateHist == null )
						{
							System.out.println( "--- Create histogram from scratch" );
							result = new CompressedStack( histSize, (int)downsampledImg.size() );
						}
						else
						{
							System.out.println( "*** Add to the intermediate histogram" );
							result = intermediateHist;
						}
						
						System.out.println( "Filling the histogram" );
						final Cursor< T > cursor = Views.iterable( downsampledImg ).localizingCursor();
						final int[] position = new int[ downsampledImg.numDimensions() ];
						final double histBinSize = (histMax-histMin) / histSize;
						while ( cursor.hasNext() )
						{
							cursor.fwd();
							
							cursor.localize( position );
							final int pixelIndex = IntervalIndexer.positionToIndex( position, outDimensions );
							
							final double val = cursor.get().getRealDouble();
							int binIndex;
							if ( val <= histMin )
								binIndex = 0;
							else if ( val >= histMax )
								binIndex = histSize - 1;
							else
								binIndex = (int) ( (val-histMin) / histBinSize );
							
							result.hist[ binIndex ][ pixelIndex ]++;
							
							if (result.min > val)
								result.min = val;
							if (result.max < val)
								result.max = val;
						}
						
						System.out.println( "Ready" );
						return result;
					}
				},
				
				// reducer
				new Function2< CompressedStack, CompressedStack, CompressedStack >()
				{
					private static final long serialVersionUID = 3979781907633918053L;

					@Override
					public CompressedStack call( final CompressedStack a, final CompressedStack b ) throws Exception
					{
						System.out.println( "Combining intermediate results (a="+a+", b="+b+")" );
						
						if ( a == null )
							return b;
						else if ( b == null )
							return a;
						
						for ( int binIndex = 0; binIndex < a.hist.length; binIndex++ )
							for ( int pixelIndex = 0; pixelIndex < a.hist[ binIndex ].length; pixelIndex++ )
								a.hist[ binIndex ][ pixelIndex ] += b.hist[ binIndex ][ pixelIndex ];
						
						if (a.min > b.min)
							a.min = b.min;
						if (a.max < b.max)
							a.max = b.max;
						
						return a;
					}
				}
				
			, treeDepth );
		
		return result;
	}
	
	
	
	private < T extends RealType< T > & NativeType< T > > void rescale( final Img< T > srcImg, final Img< T > dstImg )
	{
		// Define scaling transform
		final double[] scalingCoeffs = new double[ srcImg.numDimensions() ];
		for ( int d = 0; d < scalingCoeffs.length; d++ )
			scalingCoeffs[ d ] = (double) dstImg.dimension( d ) / srcImg.dimension( d );
		final Scale scalingTransform = new Scale( scalingCoeffs );
		
		// Prepare source image
		final ExtendedRandomAccessibleInterval< T, Img< T > > srcImgExtended = Views.extendBorder( srcImg );
		final RealRandomAccessible< T > srcImgInterpolated = Views.interpolate( srcImgExtended, new NLinearInterpolatorFactory<>() );
		final RealTransformRandomAccessible< T, InverseRealTransform > srcImgTransformed = RealViews.transform( srcImgInterpolated, scalingTransform );
		final RandomAccessibleOnRealRandomAccessible< T > srcImgTransformedRastered = Views.raster( srcImgTransformed );
		final IntervalView< T > srcImgScaledView = Views.interval( srcImgTransformedRastered, dstImg );
		final IterableInterval< T > srcImgScaledIterable = Views.flatIterable( srcImgScaledView );
		final Cursor< T > srcImgScaledCursor = srcImgScaledIterable.cursor();
		
		// Prepare destination image
		final IterableInterval< T > dstImgIterable = Views.flatIterable( dstImg );
		final Cursor< T > dstImgCursor = dstImgIterable.cursor();
		
		// Write data
		while ( dstImgCursor.hasNext() && srcImgScaledCursor.hasNext() )
			dstImgCursor.next().set( srcImgScaledCursor.next() );
	}
	
	private final double[] rescaleHelper( final double[] srcArr, final String title )
	{
		final ArrayImg< DoubleType, DoubleArray > srcImg = ArrayImgs.doubles( srcArr, downsampledSize );
		final ArrayImg< DoubleType, DoubleArray > dstImg = ArrayImgs.doubles( originalSize );
		rescale( srcImg, dstImg );
		
		final ImagePlus imp = ImageJFunctions.wrap( dstImg, title );
		Utils.workaroundImagePlusNSlices( imp );
		IJ.saveAsTiff( imp, inputFilepath + "_" + title + ".tif" );
		
		return dstImg.update( null ).getCurrentStorageArray();
	}
	
	private void correctImages()
	{
		// Prepare broadcast variables for V and Z
		final double[] vFinalData = vFinal, zFinalData = zFinal;
		final Broadcast< double[] > vFinalBroadcasted = sparkContext.broadcast( vFinalData ), zFinalBroadcasted = sparkContext.broadcast( zFinalData );
		
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
		final JavaRDD< TileInfo > task = rdd.map(
				new Function< TileInfo, TileInfo >()
				{
					private static final long serialVersionUID = 4991255417353136684L;

					@Override
					public TileInfo call( final TileInfo tile ) throws Exception
					{
						final ImagePlus imp = IJ.openImage( tile.getFilePath() );
						Utils.workaroundImagePlusNSlices( imp );
						final ImagePlusImg< ? extends RealType, ? > img = ImagePlusImgs.from( imp );
						final Cursor< ? extends RealType > imgCursor = Views.flatIterable( img ).cursor();

						final ArrayImg< DoubleType, DoubleArray > vFinalImg = ArrayImgs.doubles( vFinalBroadcasted.value(), originalSize );
						final ArrayImg< DoubleType, DoubleArray > zFinalImg = ArrayImgs.doubles( zFinalBroadcasted.value(), originalSize );
						final Cursor< DoubleType > vCursor = Views.flatIterable( vFinalImg ).cursor();
						final Cursor< DoubleType > zCursor = Views.flatIterable( zFinalImg ).cursor();
						
						RealType val;
						double min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
						while ( imgCursor.hasNext() )
						{
							val = imgCursor.next();
							val.setReal( (val.getRealDouble() - zCursor.next().get()) / vCursor.next().get() );
							
							final double bla = vCursor.get().get();
							if (min > bla)
								min = bla;
							if (max < bla)
								max = bla;
						}
						
						System.out.println(" ################# "  + "min="+min+", max="+max    + " ################# ");
						
						tile.setFilePath( Paths.get( inputFilepath ).getParent().toString() + "/" + Utils.addFilenameSuffix( Paths.get( tile.getFilePath() ).getFileName().toString(), "_corrected" ) );
						IJ.saveAsTiff( imp, tile.getFilePath() );
						
						imp.close();
						return tile;
					}
				});

		correctedTiles = task.collect().toArray( new TileInfo[0] );
		try {
			TileInfoJSONProvider.saveTilesConfiguration( correctedTiles, Utils.addFilenameSuffix( inputFilepath, "_corrected" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
	
	
	
	private void estimateQ()
	{
		final double[] pixelsHistMean = new double[ numPixels ];
		final int[] pixelIndexes = new int[ numPixels ];
		
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
		{
			for ( int binIndex = 0; binIndex < histSize; binIndex++ )
				pixelsHistMean[ pixelIndex ] += binsMean[ binIndex ] * histograms.hist[ binIndex ][ pixelIndex ];
			pixelsHistMean[ pixelIndex ] /= N;
			pixelIndexes[ pixelIndex ] = pixelIndex;
		}
		
		quicksort( pixelsHistMean, pixelIndexes );
		
		try ( final PrintWriter writer = new PrintWriter(Utils.addFilenameSuffix(inputFilepath, "_sortedMeans" ) +".txt", "UTF-8") ) {
			for ( int p = 0; p < numPixels; p++ )
				writer.println(pixelsHistMean[p]);
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int numWindowPoints = (int) Math.round( numPixels * WINDOW_POINTS_PERCENT );
		final int mStart = (int)( Math.round( numPixels / 2.0 ) - Math.round( numWindowPoints / 2.0 ) ) - 1;
		final int mEnd   = (int)( Math.round( numPixels / 2.0 ) + Math.round( numWindowPoints / 2.0 ) );
		numWindowPoints = mEnd - mStart;
		System.out.println("Estimating Q using mStart="+mStart+", mEnd="+mEnd+" (points="+numWindowPoints+")");
		
		/*Q = new double[ histSize ];
		for (int binIndex = 0; binIndex < histSize; binIndex++) 
		{
			double sumFreq = 0;
			for ( int mIndex = mStart; mIndex < mEnd; mIndex++ )
				sumFreq += (double)histograms.hist[ binIndex ][ pixelIndexes[mIndex] ] / N;
			Q[ binIndex ] = sumFreq / numWindowPoints;
		}*/
		
		int QfirstIndex = 0, QlastIndex = N;
		/*for ( int mIndex = mStart; mIndex < mEnd; mIndex++ )
		{
			QfirstIndex = Math.max( histograms.hist[0][ pixelIndexes[mIndex] ], QfirstIndex );
			QlastIndex =  Math.min( N - histograms.hist[histSize-1][ pixelIndexes[mIndex] ], QlastIndex );
			
			if ( histograms.hist[histSize-1][ pixelIndexes[mIndex] ] != 0 )
				System.out.println("mIndex="+mIndex+", cnt="+histograms.hist[histSize-1][ pixelIndexes[mIndex] ]);
		}*/
		System.out.println("QfirstIndex="+QfirstIndex+", QlastIndex="+QlastIndex+", N="+N);
		
		Q = new double[ N ];
		final double[] q = new double[ N ];
		for ( int mIndex = mStart; mIndex < mEnd; mIndex++ )
		{
			fillSortedVectorFromHistogram( pixelIndexes[ mIndex ], q );
			for ( int i = QfirstIndex; i < QlastIndex; i++ )
				Q[ i ] += q[ i ];
		}
		for ( int i = 0; i < N; i++ )
			Q[ i ] /= numWindowPoints;
		System.out.println("Q="+Arrays.toString(Q));
	}
	
	
	private void fillSortedVectorFromHistogram_mean( final int pixelIndex, final double[] vector )
	{
		// reusing vector array
		if ( vector.length != N )
		{
			System.out.println("Bug: vector size is wrong");
			return;
		}

		int counter = 0;
		for (int bin = 0; bin < histSize; bin++)
			for ( int count = 0; count < histograms.hist[ bin ][ pixelIndex ]; count++ )
				vector[ counter++ ] = binsMean[ bin ];
		
		if ( counter != N )
		{
			System.out.println("Bug: counter value is wrong");
		}	
	}
	
	private void fillSortedVectorFromHistogram/*_interpolation*/( final int pixelIndex, final double[] vector )
	{
		// reusing vector array
		if ( vector.length != N )
		{
			System.out.println("Bug: vector size is wrong");
			return;
		}

		final double histBinSize = (histMax-histMin) / histSize;
		int counter = 0;
		for (int bin = 0; bin < histSize; bin++)
		{
			final double histBinStep = histBinSize / ( histograms.hist[ bin ][ pixelIndex ] + 1 );
			for ( int j = 0; j < histograms.hist[ bin ][ pixelIndex ]; j++ )
				vector[ counter++ ] = histMin + bin*histBinSize + (j+1)*histBinStep;
		}
		
		if ( counter != N )
		{
			System.out.println("Bug: counter value is wrong");
		}	
	}
	
	
	private boolean dumpHistogramsToDisk()
	{
		try {
			final String folder = Paths.get(inputFilepath).getParent().toString();
			final String filename = Paths.get(inputFilepath).getFileName().toString();
			
			final DataOutputStream os = new DataOutputStream(
					new BufferedOutputStream(
							new FileOutputStream( folder + "/" + "histograms_"+filename+".dat" )
							)
					);	
			
			os.writeInt(downsampleFactor);
			os.writeInt(numPixels);
			os.writeInt(histSize);
			
			os.writeDouble(histMin);
			os.writeDouble(histMax);
			
			os.writeDouble(histograms.min);
			os.writeDouble(histograms.max);
			
			for ( int bin = 0; bin < histograms.hist.length; bin++ )
				for ( int pixel = 0; pixel < histograms.hist[ bin ].length; pixel++ )
					os.writeInt(histograms.hist[bin][pixel]);
			
			os.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	private boolean readHistogramsFromDisk()
	{
		try {
			final String folder = Paths.get(inputFilepath).getParent().toString();
			final String filename = Paths.get(inputFilepath).getFileName().toString();
			final String path = folder + "/" + "histograms_"+filename+".dat";
			
			if ( !Files.exists(Paths.get(path)) )
				return false;
			
			final DataInputStream is = new DataInputStream(
					new BufferedInputStream(
							new FileInputStream( path )
							)
					);	
			
			downsampleFactor = is.readInt();
			numPixels = is.readInt();
			histSize = is.readInt();
			
			histMin = is.readDouble();
			histMax = is.readDouble();
			
			histograms = new CompressedStack(histSize, numPixels);
			histograms.min = is.readDouble();
			histograms.max = is.readDouble();
			
			for ( int bin = 0; bin < histograms.hist.length; bin++ )
				for ( int pixel = 0; pixel < histograms.hist[ bin ].length; pixel++ )
					histograms.hist[bin][pixel] = is.readInt();
			
			is.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	
	
	
	
	
	
	
	private MinFuncResult minFunc(double[] x0, MinFuncOptions minFuncOptions)
	{
		double[] x = null;
		double f = 0.0;
		
		int maxIter      = minFuncOptions.maxIter;
		int MaxFunEvals  = minFuncOptions.MaxFunEvals;
		double progTol   = minFuncOptions.progTol;
		double optTol    = minFuncOptions.optTol;
		int corrections  = minFuncOptions.Corr;
		
		int maxFunEvals = 1000;
		double c1 = 1e-4;
		double c2 = 0.9;
		int LS_interp = 2;
		int LS_multi = 0;
	
		int exitflag = 0;
		String msg = null;
		
		// Initialize
		int p = x0.length;
		double[] d = new double[p];
		x = new double[x0.length];
		for (int i = 0; i < x0.length; i++)
			x[i] = x0[i];
		double t = 1.0d;
		
		// If necessary, form numerical differentiation functions
		int funEvalMultiplier = 1;
		int numDiffType = 0;

		// Evaluate Initial Point
		CdrObjectiveResult cdrObjectiveResult = cdr_objective(x);
		f = cdrObjectiveResult.E;
		double[] g = cdrObjectiveResult.G;
		double[] g_old = new double[g.length];
		
		int computeHessian = 0;
		
		int funEvals = 1;

		// Compute optimality of initial point
		double optCond = Double.MIN_VALUE;
		for (int j = 0; j < g.length; j++)
		{
			double absValue = Math.abs(g[j]);
			if (optCond < absValue)
				optCond = absValue;
		}
		
		// Exit if initial point is optimal
		if (optCond <= optTol)
		{
		    exitflag=1;
		    msg = "Optimality Condition below optTol";
		    MinFuncResult minFuncResult = new MinFuncResult();
		    minFuncResult.x = x;
		    minFuncResult.f = f;
		    return minFuncResult;
		}

		double[][] S = new double[p][corrections]; 
		double[][] Y = new double[p][corrections]; 
		double[]  YS = new double[corrections]; 
		int lbfgs_start = 0;
		int lbfgs_end = 0;
		double Hdiag = 1.0;
	
		// Perform up to a maximum of 'maxIter' descent steps:
		for (int i = 0; i < maxIter; i++)
		{
			// LBFGS
			if (i == 0)
			{
					// Initially use steepest descent direction
					for (int j = 0; j < g.length; j++)
						d[j] = -g[j];
					lbfgs_start = 0;
					lbfgs_end = -1;
					Hdiag = 1.0;
			}
			else
			{
				double[] gMg_old = new double[g.length];
				for (int j = 0; j < g.length; j++)
					gMg_old[j] = g[j] - g_old[j];
				
				double[] tPd = new double[d.length];
				for (int j = 0; j < d.length; j++)
					tPd[j] = t * d[j];

				LbfgsAddResult lbfgsAddResult = lbfgsAdd(gMg_old, tPd, S, Y, YS, lbfgs_start, lbfgs_end, Hdiag);
				S = lbfgsAddResult.S;
				Y = lbfgsAddResult.Y;
				YS = lbfgsAddResult.YS;
				lbfgs_start = lbfgsAddResult.lbfgs_start;
				lbfgs_end = lbfgsAddResult.lbfgs_end;
				Hdiag = lbfgsAddResult.Hdiag;
				boolean skipped = lbfgsAddResult.skipped;

				d = lbfgsProd(g, S, Y, YS, lbfgs_start, lbfgs_end, Hdiag);
			}
			for (int j = 0; j < g.length; j++)
				g_old[j] = g[j];

		    // ****************** COMPUTE STEP LENGTH ************************

		    // Directional Derivative
			double gtd = 0.0;			
			for (int j = 0; j < g.length; j++)
				gtd += g[j] * d[j];

		    // Check that progress can be made along direction
		    if (gtd > -progTol)
		    {
		        exitflag = 2;
		        msg = "Directional Derivative below progTol";
		        break;
		    }
		    
		    // Select Initial Guess
		    if (i == 0)
		    {
		    	double sumAbsG = 0.0;
				for (int j = 0; j < g.length; j++)
					sumAbsG += Math.abs(g[j]);
				t = Math.min(1.0, 1.0/sumAbsG);
		    } else {
		        //if (LS_init == 0)
		    	// Newton step
		    	t = 1.0;		    	
		    }
		    double f_old = f;
		    double gtd_old = gtd;
		    
		    int Fref = 1;
		    double fr;
		    // Compute reference fr if using non-monotone objective
		    if (Fref == 1)
		    {
		        fr = f;
		    }
		    
		    computeHessian = 0; 

		    // Line Search
		    f_old = f;

		    WolfeLineSearchResult wolfeLineSearchResult = WolfeLineSearch(x,t,d,f,g,gtd,c1,c2,LS_interp,LS_multi,25,progTol,1);
		    t = wolfeLineSearchResult.t;
		    f = wolfeLineSearchResult.f_new;
		    g = wolfeLineSearchResult.g_new;
		    int LSfunEvals = wolfeLineSearchResult.funEvals;
		    
		    funEvals = funEvals + LSfunEvals;
		    for (int j = 0; j < x.length; j++)
		    	x[j] += t * d[j];
					    
			// Compute Optimality Condition
			optCond = Double.MIN_VALUE;
			for (int j = 0; j < g.length; j++)
			{
				double absValG = Math.abs(g[j]);
				if (optCond < absValG)
					optCond = absValG;
			}
			
		    // Check Optimality Condition
		    if (optCond <= optTol)
		    {
		        exitflag=1;
		        msg = "Optimality Condition below optTol";
		        break;
		    }
		    
		    // ******************* Check for lack of progress *******************

			double maxAbsTD = Double.MIN_VALUE;
			for (int j = 0; j < d.length; j++)
			{
				double absValG = Math.abs(t * d[j]);
				if (maxAbsTD < absValG)
					maxAbsTD = absValG;
			}
		    if (maxAbsTD <= progTol)
		    {
		    	exitflag=2;
		        msg = "Step Size below progTol";
		        break;
			}

		    if (Math.abs(f-f_old) < progTol)
		    {
		        exitflag=2;
		        msg = "Function Value changing by less than progTol";
		        break;
		    }
		    
		    // ******** Check for going over iteration/evaluation limit *******************

		    if (funEvals*funEvalMultiplier >= maxFunEvals)
		    {
		        exitflag = 0;
		        msg = "Reached Maximum Number of Function Evaluations";
		        break;
		    }

		    if (i == maxIter)
		    {
		        exitflag = 0;
		        msg="Reached Maximum Number of Iterations";
		        break;
		    }
		}
		
		System.out.println("Msg: " + msg);
		
	    MinFuncResult minFuncResult = new MinFuncResult();
	    minFuncResult.x = x;
	    minFuncResult.f = f;
	    return minFuncResult;
	}
	
	

	
	public CdrObjectiveResult cdr_objective(double[] x)
	{
		System.out.println("cdr_objective called");
		
		double E = 0.0;
		double[] G = null;
		
		// some basic definitions
		//int N_stan = 200;				// the standard number of quantiles used for empirical parameter setting
		double w           = CAUCHY_W;  // width of the Cauchy function
		double LAMBDA_BARR = 1e6;		// the barrier term coefficient
		
		// unpack
		double[] v_vec = Arrays.copyOfRange(x, 0, numPixels);
		double[] b_vec = Arrays.copyOfRange(x, numPixels , 2 * numPixels);
		double zx = x[2 * numPixels];
		double zy = x[2 * numPixels + 1];

		// move the zero-light point to the pivot space (zx,zy) -> (px,py)
		double px = zx - PivotShiftX;		// a scalar
		double[] py = new double[numPixels];
		for (int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++)
			py[pixelIndex] = zy - PivotShiftY[pixelIndex];


		//--------------------------------------------------------------------------
		// fitting energy
		// We compute the energy of the fitting term given v,b,zx,zy. We also
		// compute its gradient wrt the random variables.

		double[] energy_fit  = new double[numPixels];		// accumulates the fit energy
		double[] deriv_v_fit = new double[numPixels];		// derivative of fit term wrt v
		double[] deriv_b_fit = new double[numPixels];		// derivative of fit term wrt b

		double v;
		double b;
		double E_fit = 0;
		double[] G_V_fit = new double[numPixels];
		double[] G_B_fit = new double[numPixels];
		
		double[] mestimator_response = new double[N];
        double[] d_est_dv = new double[N];
        double[] d_est_db = new double[N];
        
        final double[] q = new double[N];
        int weirdCount = 0;
		for (int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++) 
		{
	        // get the quantile fit for this location and vectorize it
	        fillSortedVectorFromHistogram( pixelIndex, q );
	        
	        v = v_vec[pixelIndex];
	        b = b_vec[pixelIndex];
	        
	        //System.out.println("v="+v+", b="+b+".  Qlast="+Q[N-1]+", qlast="+q[N-1]);

	        switch (MESTIMATOR) {
	            case LS:
	            	boolean weird = false;
	            	for (int i = 0; i < N; i++) {
	            		double val = Q[i] * v + b - q[i];
	            		mestimator_response[i] = val * val;
	            		d_est_dv[i] = Q[i] * val;
	            		d_est_db[i] = val;
	            		
	            		if ( Math.abs(val) > 50 )
	            			weird = true;
	            	}
	            	if ( weird )
	            		weirdCount++;
	            	
	                break;
	            case CAUCHY:
	            	for (int i = 0; i < N; i++) {
	            		double val = Q[i] * v + b - q[i];
	            		mestimator_response[i] = w*w * Math.log(1 + (val*val) / (w*w)) / 2.0;
	            		d_est_dv[i] = (Q[i]*val) / (1.0 + (val*val) / (w*w));
	            		d_est_db[i] = val / (1.0 + (val*val) / (w*w));
	            	}
	                break;
	        }	
	        
	        for (int i = 0; i < N; i++) {
	        	energy_fit [pixelIndex] += mestimator_response[i];
	        	deriv_v_fit[pixelIndex] += d_est_dv[i];
	        	deriv_b_fit[pixelIndex] += d_est_db[i];
	        }
		}
		
		System.out.println("Weird pixels count = " + weirdCount + ",   numPixels="+numPixels);
/*
		// normalize the contribution from fitting energy term by the number of data 
		// points in S (so our balancing of the energy terms is invariant)
		int data_size_factor = N_stan/histSize;
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ ) {
			E_fit += energy_fit[pixelIndex];
			G_V_fit[pixelIndex] = deriv_v_fit[pixelIndex] * data_size_factor;	// fit term derivative wrt v
			G_B_fit[pixelIndex] = deriv_b_fit[pixelIndex] * data_size_factor;	// fit term derivative wrt b
		}
		E_fit *= data_size_factor;		// fit term energy
*/
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ ) {
			E_fit += energy_fit[pixelIndex];
			G_V_fit[pixelIndex] = deriv_v_fit[pixelIndex];	// fit term derivative wrt v
			G_B_fit[pixelIndex] = deriv_b_fit[pixelIndex];	// fit term derivative wrt b
		}
		
		
		//--------------------------------------------------------------------------
		// spatial regularization of v
		// We compute the energy of the regularization term given v,b,zx,zy. We also
		// compute its gradient wrt the random variables.

		// determine the widths we will use for the LoG filter

/*		long maxDimension = 0;
		for ( int d = 0; d < size.length; d++ )
			maxDimension = Math.max( size[ d ], maxDimension );
		int max_exp = (int)Math.max(1.0, Math.log(Math.floor(maxDimension / 50.0))/Math.log(2.0));

		double[] sigmas = new double[max_exp + 2];
		for (int i = -1; i <= max_exp; i++)
			sigmas[i + 1] = Math.pow(2, i);
		
		double[] energy_vreg = new double[sigmas.length];	// accumulates the vreg energy
		double[] deriv_v_vreg = new double[numPixels];			// derivative of vreg term wrt v
		double[] deriv_b_vreg = new double[numPixels];			// derivative of vreg term wrt b
*/
		// apply the scale-invariant LoG filter to v for all scales in SIGMAS
//		double[ /* sigma */ ][ /* flattened kernel */ ] h = new double[sigmas.length][][];
/*		for (int i = 0; i < sigmas.length; i++)
		{
		    // define the kernel size, make certain dimension is odd
		    int hsize = 6 * (int)Math.ceil(sigmas[i]); 
		    if (hsize % 2 == 0)
		        hsize++;
		    double std2 = sigmas[i] * sigmas[i];

		    // h{n} = sigmas(n)^2 * fspecial('log', hsize, sigmas(n))
		    h[i] = new double[hsize][hsize];
		    double[][] h1 = new double[hsize][hsize];
		    double sumh = 0.0;
		    for (int c = 0; c < hsize; c++) {
		    	for (int r = 0; r < hsize; r++) {
		    		double arg = -1.0 * ((c-hsize/2)*(c-hsize/2) + (r-hsize/2)*(r-hsize/2)) / (2.0*std2);
		    		h[i][c][r] = Math.exp(arg);
		    		sumh += h[i][c][r];
		    	}
		    }		    
		    // calculate Laplacian
		    double sumh1 = 0.0;
		    for (int c = 0; c < hsize; c++) {
		    	for (int r = 0; r < hsize; r++) {
		    		h[i][c][r] /= sumh;
		    		h1[c][r] = h[i][c][r] * ((c-hsize/2)*(c-hsize/2) + (r-hsize/2)*(r-hsize/2) - 2 * std2) / (std2 * std2);
		    		sumh1 += h1[c][r]; 
		    	}
		    }
		    for (int c = 0; c < hsize; c++) {
		    	for (int r = 0; r < hsize; r++) {
		    		h[i][c][r] = (h1[c][r] - sumh1/(hsize*hsize)) * (sigmas[i] * sigmas[i]); // h{n} = sigmas(n)^2 * fspecial('log', hsize, sigmas(n));
		    	}
		    }
		    
		    // apply a LoG filter to v_img to penalize disagreements between neighbors
		    double[] v_LoG = imfilter_symmetric(v_vec, S_C, S_R, h[i]);
		    for (int c = 0; c < v_LoG.length; c++)
		    	v_LoG[c] /= sigmas.length;	// normalize by the # of sigmas used

		    // energy is quadratic LoG response
		    energy_vreg[i] = 0;
		    for (int c = 0; c < v_LoG.length; c++)
			    energy_vreg[i] += v_LoG[c]*v_LoG[c];

		    for (int c = 0; c < v_LoG.length; c++)
			    v_LoG[c] *= 2;
		    double[] v_LoG2 = imfilter_symmetric(v_LoG, S_C, S_R, h[i]);
		    for (int c = 0; c < v_LoG2.length; c++)		    
		    	deriv_v_vreg[c] += v_LoG2[c];
		}

		double E_vreg = 0;							// vreg term energy
		for (int i = 0; i < sigmas.length; i++)
			E_vreg += energy_vreg[i];
		double[] G_V_vreg = deriv_v_vreg;			// vreg term gradient wrt v
		double[] G_B_vreg = deriv_b_vreg;			// vreg term gradient wrt b
		//--------------------------------------------------------------------------
		*/
		double E_vreg = 0;							// vreg term energy
		double[] deriv_v_vreg = new double[numPixels];			// derivative of vreg term wrt v
		double[] deriv_b_vreg = new double[numPixels];			// derivative of vreg term wrt b
		double[] G_V_vreg = deriv_v_vreg;			// vreg term gradient wrt v
		double[] G_B_vreg = deriv_b_vreg;			// vreg term gradient wrt b
		
		
		
		
		//--------------------------------------------------------------------------
		// The ZERO-LIGHT term
		// We compute the energy of the zero-light term given v,b,zx,zy. We also
		// compute its gradient wrt the random variables.

		double[] residual = new double[numPixels];
		for (int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++)
			residual[pixelIndex] = v_vec[pixelIndex] * px + b_vec[pixelIndex] - py[pixelIndex];
		
		double[] deriv_v_zero = new double[numPixels];
		double[] deriv_b_zero = new double[numPixels];
		double deriv_zx_zero = 0.0;
		double deriv_zy_zero = 0.0;
		for (int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++) {
			double val = b_vec[pixelIndex] + v_vec[pixelIndex] * px - py[pixelIndex];
			deriv_v_zero[pixelIndex] = 2 * px * val;
			deriv_b_zero[pixelIndex] = 2 * val;
			deriv_zx_zero += 2 * v_vec[pixelIndex] * val;
			deriv_zy_zero += -2 * val;
		}

		double E_zero = 0;	// zero light term energy
		for (int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++)
			E_zero += residual[pixelIndex] * residual[pixelIndex];

		double[] G_V_zero = deriv_v_zero;		// zero light term gradient wrt v
		double[] G_B_zero = deriv_b_zero;		// zero light term gradient wrt b
		double G_ZX_zero = deriv_zx_zero;		// zero light term gradient wrt zx
		double G_ZY_zero = deriv_zy_zero;		// zero light term gradient wrt zy
		//--------------------------------------------------------------------------

		//--------------------------------------------------------------------------
		// The BARRIER term
		// We compute the energy of the barrier term given v,b,zx,zy. We also
		// compute its gradient wrt the random variables.

		double Q_UPPER_LIMIT = ZMAX;	// upper limit - transition from zero energy to quadratic increase 
		double Q_LOWER_LIMIT = ZMIN;	// lower limit - transition from quadratic to zero energy
		double Q_RATE = 0.001;			// rate of increase in energy 

		// barrier term gradients and energy components
		double[] barrierResult = theBarrierFunction(zx, Q_LOWER_LIMIT, Q_UPPER_LIMIT, Q_RATE);
		double E_barr_xc = barrierResult[0];
		double G_ZX_barr = barrierResult[1];

		barrierResult = theBarrierFunction(zy, Q_LOWER_LIMIT, Q_UPPER_LIMIT, Q_RATE);
		double E_barr_yc = barrierResult[0];
		double G_ZY_barr = barrierResult[1];

		double E_barr = E_barr_xc + E_barr_yc;		// barrier term energy

		//--------------------------------------------------------------------------

		//--------------------------------------------------------------------------
		// The total energy 
		// Find the sum of all components of the energy. TERMSFLAG switches on and
		// off different components of the energy.
		String term_str = "";
		switch (TERMSFLAG) {
		    case 0:
		        E = E_fit;
		        term_str = "fitting only";
		        break;
		    case 1:
		        E = E_fit + LAMBDA_VREG*E_vreg + LAMBDA_ZERO*E_zero + LAMBDA_BARR*E_barr;
		        term_str = "all terms";
		        break;
		}
		//--------------------------------------------------------------------------

		//--------------------------------------------------------------------------
		// The gradient of the energy
		double[] G_V = null;
		double[] G_B = null;
		double G_ZX = 0;
		double G_ZY = 0;
		
		switch (TERMSFLAG) {
		    case 0:
		        G_V = G_V_fit;
		        G_B = G_B_fit;
		        G_ZX = 0;
		        G_ZY = 0;
		        break;
		    case 1:
		    	for (int i = 0; i < G_V_fit.length; i++) {
		    		G_V_fit[i] = G_V_fit[i] + LAMBDA_VREG*G_V_vreg[i] + LAMBDA_ZERO*G_V_zero[i];
		    		G_B_fit[i] = G_B_fit[i] + LAMBDA_VREG*G_B_vreg[i] + LAMBDA_ZERO*G_B_zero[i];
		    	}
		    	G_V = G_V_fit;
		    	G_B = G_B_fit;
		        G_ZX = LAMBDA_ZERO*G_ZX_zero + LAMBDA_BARR*G_ZX_barr;
		        G_ZY = LAMBDA_ZERO*G_ZY_zero + LAMBDA_BARR*G_ZY_barr;
		        break;
		}
		      
		// vectorize the gradient
		G = new double[x.length];
		
		int pG = 0;
		for (int i = 0; i < G_V.length; i++)
			G[pG++] = G_V[i];
		for (int i = 0; i < G_B.length; i++)
			G[pG++] = G_B[i];
		G[pG++] = G_ZX;
		G[pG++] = G_ZY;
		
		//--------------------------------------------------------------------------

		/*double[] mlX2 = readFromCSVFile(PathIn + "csv\\g_ml", 1, 2 * S_C * S_R + 2);

		double mmax = Double.MIN_VALUE;
		double mmin = Double.MAX_VALUE;
		
		for (int t = 0; t < mlX2.length; t++) {
			mlX2[t] -= G[t];
			if (mmax < mlX2[t])
				mmax = mlX2[t];
			if (mmin > mlX2[t])
				mmin = mlX2[t];
		}*/
		
		//writeToCSVFile(PathIn + "csv\\x_ml_res", mlX2, 1, n);
		
		System.out.println(String.format("iter = %d  %s %s    zx,zy=(%1.2f,%1.2f)    E=%g", ITER, MESTIMATOR, term_str, zx,zy, E));
		//System.out.println(String.format("min = %f, max = %f", mmin, mmax));
		ITER++;
		
		CdrObjectiveResult result = new CdrObjectiveResult();
		result.E = E;
		result.G = G;		
		return result;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private LbfgsAddResult lbfgsAdd(double[] y, double[] s, double[][] S, double[][] Y, double[] YS, int lbfgs_start, int lbfgs_end, double Hdiag)
	{
		double ys = 0.0;
		for (int j = 0; j < y.length; j++)
			ys += y[j] * s[j];
		boolean skipped = false;
		int corrections = S[0].length;
		if (ys > 1e-10d)
		{
			if (lbfgs_end < corrections - 1)
			{
				lbfgs_end = lbfgs_end+1;
				if (lbfgs_start != 0)
				{
					if (lbfgs_start == corrections - 1)
						lbfgs_start = 0;
					else
						lbfgs_start = lbfgs_start+1;
				}
			} else {
				lbfgs_start = Math.min(1, corrections);
				lbfgs_end = 0;
			}
			
			for (int j = 0; j < s.length; j++)
			{
				S[j][lbfgs_end] = s[j];
				Y[j][lbfgs_end] = y[j];
			}
			YS[lbfgs_end] = ys;
			
			// Update scale of initial Hessian approximation
			double yy = 0.0;
			for (int j = 0; j < y.length; j++)
				yy += y[j]*y[j];
			Hdiag = ys/yy;
		} else {
			skipped = false;
		}
		
		LbfgsAddResult lbfgsAddResult = new LbfgsAddResult();
		lbfgsAddResult.S = S;
		lbfgsAddResult.Y = Y;
		lbfgsAddResult.YS = YS;
		lbfgsAddResult.lbfgs_start = lbfgs_start;
		lbfgsAddResult.lbfgs_end = lbfgs_end;
		lbfgsAddResult.Hdiag = Hdiag;
		lbfgsAddResult.skipped = skipped;

		return lbfgsAddResult;
	}
	
	private double[] lbfgsProd(double[] g, double[][] S, double[][] Y, double[] YS, int lbfgs_start, int lbfgs_end, double Hdiag)
	{
		// BFGS Search Direction
		// This function returns the (L-BFGS) approximate inverse Hessian,
		// multiplied by the negative gradient

		// Set up indexing
		int nVars = S.length;
		int maxCorrections = S[0].length;
		int nCor;
		int[] ind;
		if (lbfgs_start == 0)
		{
			ind = new int[lbfgs_end];
			for (int j = 0; j < ind.length; j++)
				ind[j] = j;
			nCor = lbfgs_end-lbfgs_start+1;
		} else {
			ind = new int[maxCorrections];
			for (int j = lbfgs_start; j < maxCorrections; j++)
				ind[j - lbfgs_start] = j;
			for (int j = 0; j <= lbfgs_end; j++)
				ind[j + maxCorrections - lbfgs_start] = j;			
			nCor = maxCorrections;			
		}

		double[] al = new double[nCor];
		double[] be = new double[nCor];

		double[] d = new double[g.length];
		for (int j = 0; j < g.length; j++)
			d[j] = -g[j];
		for (int j = 0; j < ind.length; j++)
		{
			int i = ind[ind.length-j-1];
			double sumSD = 0.0;
			for (int k = 0; k < S.length; k++)
				sumSD += (S[k][i] * d[k]) / YS[i];
			al[i] = sumSD;

			for (int k = 0; k < d.length; k++)
				d[k] -= al[i] * Y[k][i];
		}

		// Multiply by Initial Hessian
		for (int j = 0; j < d.length; j++)
			d[j] = Hdiag * d[j];

		for (int i = 0; i < ind.length; i++)
		{
			double sumYd = 0.0;
			for (int j = 0; j < Y.length; j++)
				sumYd += Y[j][ind[i]] * d[j];
			be[ind[i]] = sumYd / YS[ind[i]];
			
			for (int j = 0; j < d.length; j++)
				d[j] += S[j][ind[i]] * (al[ind[i]] - be[ind[i]]);
		}
		return d;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private WolfeLineSearchResult WolfeLineSearch(double[] x, double t, double[] d, double f, double[] g, double gtd, double c1, double c2, 
			int LS_interp, int LS_multi, int maxLS, double progTol, int saveHessianComp)
	{
		
		
		double[] x2 = new double[x.length];
		for (int j = 0; j < x.length; j++)
			x2[j] = x[j] + t * d[j];
		CdrObjectiveResult cdrObjectiveResult = cdr_objective(x2);
		double f_new = cdrObjectiveResult.E;
		double[] g_new = cdrObjectiveResult.G;
		int funEvals = 1;
	
		double gtd_new = 0.0;			
		for (int j = 0; j < g.length; j++)
			gtd_new += g_new[j] * d[j];
		
		// Bracket an Interval containing a point satisfying the
		// Wolfe criteria

		int LSiter = 0;
		double t_prev = 0.0;
		double f_prev = f;
		double[] g_prev = new double[g.length];
		for (int j = 0; j < g.length; j++)
			g_prev[j] = g[j];
		double gtd_prev = gtd;
		double nrmD = Double.MIN_VALUE;
		for (int j = 0; j < d.length; j++)
		{
			double absValD = Math.abs(d[j]);
			if (nrmD < absValD)
				nrmD = absValD;
		}
		boolean done = false;
		
		int bracketSize = 0;
		double[] bracket = new double[2];
		double[] bracketFval = new double[2];
		double[] bracketGval = new double[2 * x.length];
		
		while (LSiter < maxLS)
		{
		    if (f_new > f + c1*t*gtd || (LSiter > 1 && f_new >= f_prev))
		    {
		    	bracketSize = 2;
		    	bracket[0] = t_prev; bracket[1] = t;
		    	bracketFval[0] = f_prev; bracketFval[1] = f_new;
		    	for (int j = 0; j < g_prev.length; j++)
		    		bracketGval[j] = g_prev[j];
		    	for (int j = 0; j < g_new.length; j++)
		    		bracketGval[g_prev.length + j] = g_new[j];		    	
		    	break;
		    }
		    else if (Math.abs(gtd_new) <= -c2*gtd)
		    {
		    	bracketSize = 1;
		        bracket[0] = t;
		        bracketFval[0] = f_new;
		    	for (int j = 0; j < g_new.length; j++)
		    		bracketGval[j] = g_new[j];
		        done = true;
		        break;
		    }
		    else if (gtd_new >= 0)
		    {
		    	bracketSize = 2;
		    	bracket[0] = t_prev; bracket[1] = t;
		    	bracketFval[0] = f_prev; bracketFval[1] = f_new;
		    	for (int j = 0; j < g_prev.length; j++)
		    		bracketGval[j] = g_prev[j];
		    	for (int j = 0; j < g_new.length; j++)
		    		bracketGval[g_prev.length + j] = g_new[j];		    	
		    	break;
		    }
	    
		    double temp = t_prev;
		    t_prev = t;
		    double minStep = t + 0.01*(t-temp);
		    double maxStep = t*10;
		    if (LS_interp <= 1)
		    	t = maxStep;
		    else if (LS_interp == 2)
		    {
		    	double[] points = new double[2*3];
		    	points[0] = temp; points[1] = f_prev; points[2] = gtd_prev;
		    	points[3] = t;    points[4] = f_new;  points[5] = gtd_new;
		    	t = polyinterp(points, minStep, maxStep);
		    }
	    
		    f_prev = f_new;
		    for (int j = 0; j < g_new.length; j++)
		    	g_prev[j] = g_new[j];
		    gtd_prev = gtd_new;
		    
			x2 = new double[x.length];
			for (int j = 0; j < x.length; j++)
				x2[j] = x[j] + t * d[j];
		    cdrObjectiveResult = cdr_objective(x2);
			f_new = cdrObjectiveResult.E;
			g_new = cdrObjectiveResult.G;
			funEvals++;
			gtd_new = 0.0;			
			for (int j = 0; j < g.length; j++)
				gtd_new += g_new[j] * d[j];
			LSiter++;
		}
		
		if (LSiter == maxLS)
		{
	    	bracketSize = 2;
	    	bracket[0] = 0; bracket[1] = t;
	    	bracketFval[0] = f; bracketFval[1] = f_new;
	    	for (int j = 0; j < g.length; j++)
	    		bracketGval[j] = g[j];
	    	for (int j = 0; j < g_new.length; j++)
	    		bracketGval[g.length + j] = g_new[j];		    	
		}
		
		// Zoom Phase

		// We now either have a point satisfying the criteria, or a bracket
		// surrounding a point satisfying the criteria
		// Refine the bracket until we find a point satisfying the criteria
		boolean insufProgress = false;
		//int Tpos = 1;
		//int LOposRemoved = 0;
		int LOpos;
		int HIpos;
		double f_LO;

		while (!done && LSiter < maxLS)
		{
		    // Find High and Low Points in bracket
		    //[f_LO LOpos] = min(bracketFval);
		    //HIpos = -LOpos + 3;
			
			if (bracketSize < 2)
			{
				f_LO = bracketFval[0];
				LOpos = 0; HIpos = 1;
			} 
			else 
			{
				if (bracketFval[0] <= bracketFval[1])
				{
					f_LO = bracketFval[0];
					LOpos = 0; HIpos = 1;
				} else {
					f_LO = bracketFval[1];
					LOpos = 1; HIpos = 0;
				}
			}
			
			// LS_interp == 2
			//t = polyinterp([bracket(1) bracketFval(1) bracketGval(:,1)'*d
			//            bracket(2) bracketFval(2) bracketGval(:,2)'*d],doPlot);
			            
		    {
				double val0 = 0.0;			
				for (int j = 0; j < g.length; j++)
					val0 += bracketGval[j] * d[j];
				
				double val1 = 0.0;			
				for (int j = 0; j < g.length; j++)
					val1 += bracketGval[g.length + j] * d[j];
		    	
		    	double[] points = new double[2*3];
		    	points[0] = bracket[0]; points[1] = bracketFval[0]; points[2] = val0;
		    	points[3] = bracket[1]; points[4] = bracketFval[1];  points[5] = val1;
		    	t = polyinterp(points, null, null);
		    }
		    
		    // Test that we are making sufficient progress
		    if (Math.min(Math.max(bracket[0], bracket[1])-t,t-Math.min(bracket[0], bracket[1]))/(Math.max(bracket[0], bracket[1])-Math.min(bracket[0], bracket[1])) < 0.1)
		    {
		        if (insufProgress || t>=Math.max(bracket[0], bracket[1]) || t <= Math.min(bracket[0], bracket[1]))
		        {
		            if (Math.abs(t-Math.max(bracket[0], bracket[1])) < Math.abs(t-Math.min(bracket[0], bracket[1])))
		            {
		                t = Math.max(bracket[0], bracket[1])-0.1*(Math.max(bracket[0], bracket[1])-Math.min(bracket[0], bracket[1]));
		            } else {
		                t = Math.min(bracket[0], bracket[1])+0.1*(Math.max(bracket[0], bracket[1])-Math.min(bracket[0], bracket[1]));
		            }
		            insufProgress = false;
		        } else {
		            insufProgress = true;
		        }
		    } else {
		        insufProgress = false;
		    }

		    // Evaluate new point
			x2 = new double[x.length];
			for (int j = 0; j < x.length; j++)
				x2[j] = x[j] + t * d[j];
		    cdrObjectiveResult = cdr_objective(x2);
			f_new = cdrObjectiveResult.E;
			g_new = cdrObjectiveResult.G;
			funEvals++;
			gtd_new = 0.0;			
			for (int j = 0; j < g.length; j++)
				gtd_new += g_new[j] * d[j];
			LSiter++;

			boolean armijo = f_new < f + c1*t*gtd;
		    if (!armijo || f_new >= f_LO)
		    {
		        // Armijo condition not satisfied or not lower than lowest point
		        bracket[HIpos] = t;
		        bracketFval[HIpos] = f_new;
		    	for (int j = 0; j < g.length; j++)
		    		bracketGval[g.length * HIpos + j] = g_new[j];    	
		        //Tpos = HIpos;
		    } else {
		        if (Math.abs(gtd_new) <= - c2*gtd)
		        {
		            // Wolfe conditions satisfied
		            done = true;
		        } else if (gtd_new*(bracket[HIpos]-bracket[LOpos]) >= 0)
		        {
		            // Old HI becomes new LO
		            bracket[HIpos] = bracket[LOpos];
		            bracketFval[HIpos] = bracketFval[LOpos];
			    	for (int j = 0; j < g.length; j++)
			    		bracketGval[g.length * HIpos + j] = bracketGval[g.length * LOpos + j];	    	
		        }
		        // New point becomes new LO
		        bracket[LOpos] = t;
		        bracketFval[LOpos] = f_new;
		    	for (int j = 0; j < g.length; j++)
		    		bracketGval[g.length * LOpos + j] = g_new[j];
		        //Tpos = LOpos;
		    }

		    if (!done && Math.abs(bracket[0]-bracket[1])*nrmD < progTol)
		    	break;
		}
		
		if (bracketSize < 2)
		{
			f_LO = bracketFval[0];
			LOpos = 0; HIpos = 1;
		} 
		else 
		{
			if (bracketFval[0] <= bracketFval[1])
			{
				f_LO = bracketFval[0];
				LOpos = 0; HIpos = 1;
			} else {
				f_LO = bracketFval[1];
				LOpos = 1; HIpos = 0;
			}
		}
	
		t = bracket[LOpos];
		f_new = bracketFval[LOpos];
    	for (int j = 0; j < g.length; j++)
    		g_new[j] = bracketGval[g.length * LOpos + j];
		
    	WolfeLineSearchResult wolfeLineSearchResult = new WolfeLineSearchResult();
    	wolfeLineSearchResult.t = t;
    	wolfeLineSearchResult.f_new = f_new;
    	wolfeLineSearchResult.g_new = g_new;
    	wolfeLineSearchResult.funEvals = funEvals;
    	return wolfeLineSearchResult;
	}
	
	
	
	
	private double polyinterp(double[] points, Double xminBound, Double xmaxBound)
	{
		double xmin = Math.min(points[0], points[3]);
		double xmax = Math.max(points[0], points[3]);
		
		// Compute Bounds of Interpolation Area
		if (xminBound == null)
		    xminBound = xmin;
		if (xmaxBound == null)
		    xmaxBound = xmax;		
		
		// Code for most common case:
		//   - cubic interpolation of 2 points
		//       w/ function and derivative values for both

		// Solution in this case (where x2 is the farthest point):
		// d1 = g1 + g2 - 3*(f1-f2)/(x1-x2);
		// d2 = sqrt(d1^2 - g1*g2);
		// minPos = x2 - (x2 - x1)*((g2 + d2 - d1)/(g2 - g1 + 2*d2));
		// t_new = min(max(minPos,x1),x2);
		
		int minPos;
		int notMinPos;
		if (points[0] < points[3])
		{
			minPos = 0;
		} else {
			minPos = 1;
		}
		notMinPos = (1 - minPos) * 3;
		double d1 = points[minPos + 2] + points[notMinPos + 2] - 3*(points[minPos + 1]-points[notMinPos + 1])/(points[minPos]-points[notMinPos]);
		double d2_2 = d1*d1 - points[minPos+2]*points[notMinPos+2];
		
		if (d2_2 >= 0.0) 
		{
		    double d2 = Math.sqrt(d2_2);
	        double t = points[notMinPos] - (points[notMinPos] - points[minPos])*((points[notMinPos + 2] + d2 - d1)/(points[notMinPos + 2] - points[minPos + 2] + 2*d2));
	        return Math.min(Math.max(t, xminBound), xmaxBound);
		} else {
			return (xmaxBound+xminBound)/2.0;
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private double[] theBarrierFunction(double x, double xmin, double xmax, double width)
	{
		// the barrier function has a well shape. It has quadratically increasing
		// energy below xmin, zero energy between xmin and xmax, and quadratically
		// increasing energy above xmax. The rate of increase is determined by width

		double[] result = new double[] {0.0, 0.0}; // E G
		
		double xl1 = xmin;
		double xl2 = xl1 + width;

		double xh2 = xmax;
		double xh1 = xh2 - width;

		if (x <= xl1) {
			result[0] = ((x-xl2)/(xl2-xl1)) * ((x-xl2)/(xl2-xl1));
			result[1] = (2*(x-xl2)) / ((xl2-xl1)*(xl2-xl1));
		}		 
		else if ((x >= xl1) && (x <= xl2)) {
			result[0] = ((x-xl2)/(xl2-xl1))*((x-xl2)/(xl2-xl1));
			result[1] = (2*(x-xl2))  / ((xl2-xl1)*(xl2-xl1));
		}
		else if ((x > xl2) && (x < xh1)) {
			result[0] = 0;
			result[1] = 0;
		}
		else if ((x >= xh1) && (x < xh2)) {
			result[0] = ((x-xh1)/(xh2-xh1))*((x-xh1)/(xh2-xh1));
			result[1] = (2*(x-xh1))  / ((xh2-xh1)*(xh2-xh1));
		}
		else {
			result[0] = ((x-xh1)/(xh2-xh1))*((x-xh1)/(xh2-xh1));
			result[1] = (2*(x-xh1))  / ((xh2-xh1)*(xh2-xh1));
		}
		
		return result;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	// computes the mean standard error of the regression
	private double computeStandardError(double[] v, double[] b) {
		// initialize a matrix to contain all the standard error calculations
		double[] se = new double[numPixels];

		// compute the standard error at each location
		double[] q = new double[N];
		double[] fitvals = new double[N];
		double[] residuals = new double[N];
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
		{
	        double vi = v[pixelIndex];
	        double bi = b[pixelIndex];

	        fillSortedVectorFromHistogram(pixelIndex, q);
	        
	        double sum_residuals2 = 0;
	        for (int i = 0; i < N; i++) 
	        {
	        	fitvals[i] = bi + Q[i] * vi;
	        	residuals[i] = q[i] - fitvals[i];
	        	sum_residuals2 += residuals[i] * residuals[i];
	        }
	        
	        se[pixelIndex] = Math.sqrt(sum_residuals2 / (N-2));
		}
		return mean(se);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	private double mean(double[] a) {
		int i;
		double sum = 0;
	    for (i = 0; i < a.length; i++) {
	        sum += a[i];
	    }
	    return sum / a.length;
	}
	
	
	
	
	
	
	
	
	
	
	
	private long[] getMinSize( final TileInfo[] tiles )
	{
		final long[] minSize = tiles[ 0 ].getSize().clone();
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < minSize.length; d++ )
				if (minSize[ d ] > tile.getSize( d ))
					minSize[ d ] = tile.getSize( d );
		return minSize;
	}
	
	
	
	
	// TODO move to some helper class
	private static void quicksort(double[] main, int[] index) {
	    quicksort(main, index, 0, index.length - 1);
	}

	// quicksort a[left] to a[right]
	private static void quicksort(double[] a, int[] index, int left, int right) {
	    if (right <= left) return;
	    int i = partition(a, index, left, right);
	    quicksort(a, index, left, i-1);
	    quicksort(a, index, i+1, right);
	}
 
	// partition a[left] to a[right], assumes left < right
	private static int partition(double[] a, int[] index, 
	int left, int right) {
	    int i = left - 1;
	    int j = right;
	    while (true) {
	        while (less(a[++i], a[right]))      // find item on left to swap
	            ;                               // a[right] acts as sentinel
	        while (less(a[right], a[--j]))      // find item on right to swap
	            if (j == left) break;           // don't go out-of-bounds
	        if (i >= j) break;                  // check if pointers cross
	        exch(a, index, i, j);               // swap two elements into place
	    }
	    exch(a, index, i, right);               // swap with partition element
	    return i;
	}

	// is x < y ?
	private static boolean less(double x, double y) {
	    return (x < y);
	}

	// exchange a[i] and a[j]
	private static void exch(double[] a, int[] index, int i, int j) {
		double swap = a[i];
	    a[i] = a[j];
	    a[j] = swap;
	    int b = index[i];
	    index[i] = index[j];
	    index[j] = b;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	class CompressedStack
	{
		final int[ /* intensity bins */ ][ /* flattened pixels */ ] hist;
		public double min, max;
		
		public CompressedStack( final int bins, final int pixels )
		{
			hist = new int[ bins ][ pixels ];
			min = Double.MAX_VALUE;
			max = -Double.MAX_VALUE;
		}
	}
	
	class MinFuncOptions {
		public int maxIter;			// max iterations for optimization
		public int MaxFunEvals;		// max evaluations of objective function
		public double progTol;		// progress tolerance
		public double optTol;		// optimality tolerance
		public int Corr;			// number of corrections to store in memory
	}
	
	class MinFuncResult {
		public double[] x;
		public double f;
	}
	
	class CdrObjectiveResult {
		public double E;
		public double[] G;
	}
	
	class LbfgsAddResult {
		public double[][] S;
		public double[][] Y;
		public double[] YS;
		public int lbfgs_start;
		public int lbfgs_end;
		public double Hdiag;
		public boolean skipped;  
	}
	
	class WolfeLineSearchResult {
		public double t;
		public double f_new;
		public double[] g_new;
		public int funEvals;
	}
}
