package org.janelia.stitching;

import java.awt.Dimension;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

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
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class IlluminationCorrectionSpark implements Runnable, Serializable
{
	private static final long serialVersionUID = 1920621620902670130L;
	
	/*private static final double WINDOW_POINTS_PERCENT = 0.25;
	
	private long[] size;
	private int numPixels;
	
	private int N; // stack size
	private double[][] sortedPixels;
	private double[] Q;
	
	
	private enum Mestimator { LS, CAUCHY };
	private double CAUCHY_W;		// width of the Cauchy function used for robust regression 
	private double PivotShiftX;		// shift on the Q axis into the pivot space 
	private double[] PivotShiftY;	// shift on the q axis into the pivot space 
	private int ITER;				// iteration count for the optimization
	private Mestimator MESTIMATOR;	// specifies "CAUCHY" or "LS" (least squares) 
	private int TERMSFLAG;			// flag specifing which terms to include in the energy function 
	private double LAMBDA_VREG;		// coefficient for the v regularization 
	private double LAMBDA_ZERO;		// coefficient for the zero-light term
	private double ZMIN;			// minimum possible value for Z
	private double ZMAX;			// maximum possible value for Z
	private double STACKMIN;
	private int maxLbgfsIterations = 500;
	
	
	private static final int maxIter = 500;			// max iterations for optimization
	private static final int MaxFunEvals = 1000;	// max evaluations of objective function
	private static final double progTol = 1e-5;		// progress tolerance
	private static final double optTol = 1e-5;		// optimality tolerance
	private static final int Corr = 100;			// number of corrections to store in memory
	*/
	
	private static final int downsampleFactor = 4;
	private static final int histSize = 256;
	private static final double histMin = 0.0, histMax = 4096.0;
	
	
	
	private final transient String inputFilepath;
	private transient JavaSparkContext sparkContext;
	private transient TileInfo[] tiles;
	
	
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
		try {
			tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilepath );
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
		
		
		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "IlluminationCorrection" )
				.set( "spark.driver.maxResultSize", "8g" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryoserializer.buffer.max", "2000m" ));
		
		long elapsed = System.nanoTime();
		final CompressedStack result = getHistograms();
		elapsed = System.nanoTime() - elapsed;
		
		sparkContext.close();
		
		System.out.println( "Size of the resulting array: " + result.hist.length + " x " + result.hist[ 0 ].length );
		System.out.println( "min=" + result.min + ", max=" + result.max );
		System.out.println( "Done" );
		
		
		System.out.println( "Elapsed time: " + (int)(elapsed / Math.pow(10, 9)) + "s" );
		
		
		int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
		for ( int pixel = 0; pixel < result.hist[ 0 ].length; pixel++ )
		{			
			for ( int bin = 0; bin < result.hist.length; bin++ )
			{
				if (min > result.hist[ bin ][ pixel ])
					min = result.hist[ bin ][ pixel ];
				if (max < result.hist[ bin ][ pixel ])
					max = result.hist[ bin ][ pixel ];
			}	
		}
		System.out.println("Min freq=" + min + ", max freq=" + max);
		
		
		
		
		// dump the matrix to disk
		DataOutputStream os;
		try {
			String folder = Paths.get(inputFilepath).getParent().toString();
			String filename = Paths.get(inputFilepath).getFileName().toString();
			
			os = new DataOutputStream(new FileOutputStream( folder + "/" + "histograms_"+filename+".dat" ));
			
			os.writeInt(downsampleFactor);
			os.writeInt(histSize);
			
			os.writeDouble(histMin);
			os.writeDouble(histMax);
			
			os.writeDouble(result.min);
			os.writeDouble(result.max);
			
			for ( int bin = 0; bin < result.hist.length; bin++ )
				for ( int pixel = 0; pixel < result.hist[ bin ].length; pixel++ )
					os.writeInt(result.hist[bin][pixel]);
			
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
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
						System.out.println( "Loading tile image " + tile.getFilePath() );
						final ImagePlus imp = IJ.openImage( tile.getFilePath() );
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
								binIndex = (int) (  (val - histMin) / ( (histMax-histMin) / histSize )  );
							
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
	
	
	
	
	private long[] getMinSize( final TileInfo[] tiles )
	{
		final long[] minSize = tiles[ 0 ].getSize().clone();
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < minSize.length; d++ )
				if (minSize[ d ] > tile.getSize( d ))
					minSize[ d ] = tile.getSize( d );
		return minSize;
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
	
}
