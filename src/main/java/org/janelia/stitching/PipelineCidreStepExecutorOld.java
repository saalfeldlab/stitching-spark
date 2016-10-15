package org.janelia.stitching;

import java.awt.Dimension;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;

import ij.IJ;
import ij.ImagePlus;
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
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class PipelineCidreStepExecutorOld extends PipelineStepExecutor 
{
	private static final long serialVersionUID = 1920621620902670130L;
	
	private static final double WINDOW_POINTS_PERCENT = 0.25;
	
	private long[] size;
	private int numPixels;
	
	private int N; // stack size
	private double[ /* flattened n-dimensional position */ ][ /* imageId */ ] sortedPixels;
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

	public PipelineCidreStepExecutorOld( final StitchingJob job, final JavaSparkContext sparkContext ) 
	{
		super(job, sparkContext);
	}

	@Override
	public void run()
	{
		try {
			execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	 
	private < T extends RealType< T > & NativeType< T > > void execute() throws Exception
	{
		final ImageType imageType = Utils.getImageType( Arrays.asList( job.getTiles() ) );
		size = getMinSize( job.getTiles() );
		N = job.getTiles().length;
		
		final ArrayImg< DoubleType, DoubleArray > sum = ArrayImgs.doubles( size );	
		final Cursor< DoubleType > sumCursor = Views.flatIterable( sum ).localizingCursor();
		final long[] position = new long[ size.length ];
		
		
		// debug
		if ( size[0]*size[1]*size[2] != (long)((int)sum.size()) )
			throw new Exception("Dimensions mismatch");
		
		numPixels = (int) sum.size();
		
		sortedPixels = new double[ numPixels ][ N ];
		
		for ( int i = 0; i < N; i++ )
		{
			final ImagePlus imp = IJ.openImage( job.getTiles()[ i ].getFilePath() );
			Utils.workaroundImagePlusNSlices( imp );
			
			final Cursor< T > imgCursor = Views.flatIterable(
					Views.interval( 
							(RandomAccessibleInterval<T>) ImagePlusImgs.from( imp ), 
							new FinalInterval( size ) 
							) 
						).cursor();
			
			while ( sumCursor.hasNext() && imgCursor.hasNext() )
			{
				final DoubleType val = sumCursor.next();
				val.set( val.get() + imgCursor.next().getRealDouble() );
				
				// copy pixel value to the corresponding index & image
				sumCursor.localize( position );
				final int pixelIndex = (int) IntervalIndexer.positionToIndex( position, size );
				sortedPixels[ pixelIndex ][ i ] = imgCursor.get().getRealDouble();
			}
			
			if ( sumCursor.hasNext() || imgCursor.hasNext() )
				throw new Exception("cursor positions mismatch");
			
			imp.close();
			sumCursor.reset();
		}
		
		
		
		
		// Sort corresponding pixel intensities
		for ( final double[] pixelsAtLocation : sortedPixels )
			Arrays.sort( pixelsAtLocation );
			
			
			
			
		final double[] pixelsMean = new double[ numPixels ];
		final int[] pixelIndexes = new int[ pixelsMean.length ];
		int index = 0;
		
		while ( sumCursor.hasNext() )
		{
			sumCursor.fwd();
			sumCursor.localize( position );
			final int ind = (int) IntervalIndexer.positionToIndex( position, size );
			pixelsMean[ ind ] = sumCursor.get().get() / N;
			pixelIndexes[ ind ] = index++;
		}
		
		quicksort( pixelsMean, pixelIndexes );
		
		int numWindowPoints = (int) Math.round( pixelsMean.length * WINDOW_POINTS_PERCENT );
		final int mStart = (int)( Math.round( pixelsMean.length / 2.0 ) - Math.round( numWindowPoints / 2.0 ) ) - 1;
		final int mEnd   = (int)( Math.round( pixelsMean.length / 2.0 ) + Math.round( numWindowPoints / 2.0 ) );
		numWindowPoints = mEnd - mStart;
		
		Q = new double[ N ];
		// Don't take pixels of an original stack
		/*for (int i = 0; i < N; i++) 
		{
			final ImagePlus imp = IJ.openImage( job.getTiles()[ i ].getFilePath() );
			Utils.workaroundImagePlusNSlices( imp );
			
			final RandomAccess< T > img = ( (RandomAccessibleInterval<T>) ImagePlusImgs.from( imp ) )
					.randomAccess( 
							new FinalInterval( minSize ) );

			double sumPixels = 0;
			for ( int mIndex = mStart; mIndex < mEnd; mIndex++ )
			{
				IntervalIndexer.indexToPosition( indexes[ mIndex ], minSize, position );
				img.setPosition( position );
				sumPixels += img.get().getRealDouble();
			}
			
			Q[ i ] = sumPixels / mLength;
			imp.close();
		}*/
		// but a stack of sorted pixels values instead
		for (int i = 0; i < N; i++) 
		{
			double sumPixels = 0;
			for ( int mIndex = mStart; mIndex < mEnd; mIndex++ )
				sumPixels += sortedPixels[ pixelIndexes[mIndex] ][ i ];
			Q[ i ] = sumPixels / numWindowPoints;
		}
		
		
		
		
		
		
		
		
		
		
		// initial guesses for the correction surfaces
		double[] v0 = new double[ numPixels ];
		double[] b0 = new double[ numPixels ];
		Arrays.fill( v0, 1.0 );
		
		
		
		
		// Transform Q and S (which contains q) to the pivot space. The pivot
		// space is just a shift of the origin to the median datum. First, the shift
		// for Q:
		final int mid_ind = (N-1) / 2;
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
			b0[ pixelIndex ] /* += doesn't make sense  */ = PivotShiftX /* doesn't make sense:  *v0[pixelIndex]  */ - PivotShiftY[ pixelIndex ];
		
		
		
		// some parameters initialization
		STACKMIN = Double.MAX_VALUE;
		for ( double[] arr : sortedPixels )
			for ( double a : arr )
				if (STACKMIN > a)
					STACKMIN = a;
		ZMAX = STACKMIN;
		
		final double zx0 = 0.85 * STACKMIN, zy0 = zx0;
		
		
		// vector containing initial values of the variables we want to estimate
		//x0 = [v0(:); b0(:); zx0; zy0];
		double[] x0 = new double[2 * numPixels + 2];
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
		MinFuncResult minFuncResult = minFunc( x0, minFuncOptions );
		
		double[] x  = minFuncResult.x;
		double fval = minFuncResult.f;

		// unpack
		double[] v1 = Arrays.copyOfRange(x, 0, numPixels);
		double[] b1 = Arrays.copyOfRange(x, numPixels, 2*numPixels);
		double zx1 = x[2 * numPixels];
		double zy1 = x[2 * numPixels + 1];
		
		// 2nd optimization using REGULARIZED ROBUST fitting
		// use the mean standard error of the LS fitting to set the width of the
		// CAUCHY function
		double mse = computeStandardError(v1, b1);
		CAUCHY_W = mse;

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
		double[] b_pivoted = Arrays.copyOfRange(x, numPixels, 2*numPixels);
		double zx = x[2 * numPixels];
		double zy = x[2 * numPixels + 1];

		// Build the final correction model 

		// Unpivot b: move pivot point back to the original location
		double[] b_unpivoted = new double[numPixels];
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
			b_unpivoted[pixelIndex] = PivotShiftY[pixelIndex] + b_pivoted[pixelIndex] - PivotShiftX * v[pixelIndex];

		// shift the b surface to the zero-light surface
		double[] z = new double[numPixels];
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ )
			z[pixelIndex] = b_unpivoted[pixelIndex] + zx * v[pixelIndex];

		
		
		
		// V and Z represent our model
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
		
		IJ.log("Msg: " + msg);
		
	    MinFuncResult minFuncResult = new MinFuncResult();
	    minFuncResult.x = x;
	    minFuncResult.f = f;
	    return minFuncResult;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public CdrObjectiveResult cdr_objective(double[] x)
	{
		double E = 0.0;
		double[] G = null;
		
		// some basic definitions
		int N_stan = 200;				// the standard number of quantiles used for empirical parameter setting
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
        
		for (int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++) {
	        // get the quantile fit for this location and vectorize it
	        double[] q = new double[N];
	        for (int i = 0; i < N; i++)
	        	q[i] = sortedPixels[ pixelIndex ][ i ];
	        
	        v = v_vec[pixelIndex];
	        b = b_vec[pixelIndex];

	        switch (MESTIMATOR) {
	            case LS:
	            	for (int i = 0; i < N; i++) {
	            		double val = Q[i] * v + b - q[i];
	            		mestimator_response[i] = val * val;
	            		d_est_dv[i] = Q[i] * val;
	            		d_est_db[i] = val;
	            	}
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
		// normalize the contribution from fitting energy term by the number of data 
		// points in S (so our balancing of the energy terms is invariant)
		int data_size_factor = N_stan/N;
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ ) {
			E_fit += energy_fit[pixelIndex];
			G_V_fit[pixelIndex] = deriv_v_fit[pixelIndex] * data_size_factor;	// fit term derivative wrt v
			G_B_fit[pixelIndex] = deriv_b_fit[pixelIndex] * data_size_factor;	// fit term derivative wrt b
		}
		E_fit *= data_size_factor;		// fit term energy
		
		
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
		
		IJ.log(String.format("iter = %d  %s %s    zx,zy=(%1.2f,%1.2f)    E=%g", ITER, MESTIMATOR, term_str, zx,zy, E));
		//IJ.log(String.format("min = %f, max = %f", mmin, mmax));
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
		for ( int pixelIndex = 0; pixelIndex < numPixels; pixelIndex++ ) {				
	        double vi = v[pixelIndex];
	        double bi = b[pixelIndex];

	        for (int i = 0; i < N; i++) {
	        	q[i] = sortedPixels[ pixelIndex ][ i ];
	        	fitvals[i] = bi + Q[i] * vi;
	        	residuals[i] = q[i] - fitvals[i];
	        }
	        double sum_residuals2 = 0;
	        for (int i = 0; i < N; i++) {
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
