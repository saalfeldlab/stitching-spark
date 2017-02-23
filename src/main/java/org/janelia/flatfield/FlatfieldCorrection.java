package org.janelia.flatfield;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.flatfield.FlatfieldCorrectionSolver.ModelType;
import org.janelia.flatfield.FlatfieldCorrectionSolver.RegularizerModelType;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.kohsuke.args4j.CmdLineException;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;
import scala.Tuple2;


public class FlatfieldCorrection implements Serializable, AutoCloseable
{
	private static final long serialVersionUID = -8987192045944606043L;

	private final String histogramsPath, solutionPath;

	private transient final JavaSparkContext sparkContext;
	private transient final TileInfo[] tiles;

	private final long[] fullTileSize;
	private final Interval workingInterval;

	private final FlatfieldCorrectionArguments args;

	public static void main( final String[] args ) throws CmdLineException, IOException
	{
		final FlatfieldCorrectionArguments argsParsed = new FlatfieldCorrectionArguments( args );
		if ( !argsParsed.parsedSuccessfully() )
			System.exit( 1 );

		try ( final FlatfieldCorrection driver = new FlatfieldCorrection( argsParsed ) )
		{
			driver.run();
		}
		System.out.println("Done");
	}


	public static <
		T extends NativeType< T > & RealType< T >,
		U extends NativeType< U > & RealType< U > >
	ImagePlusImg< FloatType, ? > applyCorrection(
			final RandomAccessibleInterval< T > src,
			final RandomAccessibleInterval< U > v,
			final RandomAccessibleInterval< U > z )
	{
		final Cursor< T > srcCursor = Views.iterable( src ).localizingCursor();
		final ImagePlusImg< FloatType, ? > dst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( src ) );
		final RandomAccess< FloatType > dstRandomAccess = Views.translate( dst, Intervals.minAsLongArray( src ) ).randomAccess();
		final RandomAccess< U > vRandomAccess = v.randomAccess();
		final RandomAccess< U > zRandomAccess = z.randomAccess();
		while ( srcCursor.hasNext() )
		{
			srcCursor.fwd();
			dstRandomAccess.setPosition( srcCursor );
			vRandomAccess.setPosition( srcCursor );
			zRandomAccess.setPosition( srcCursor.getIntPosition(0), 0 ); zRandomAccess.setPosition( srcCursor.getIntPosition(1), 1 );
			dstRandomAccess.get().setReal( srcCursor.get().getRealDouble() * vRandomAccess.get().getRealDouble() + zRandomAccess.get().getRealDouble() );
		}
		return dst;
	}



	public FlatfieldCorrection( final FlatfieldCorrectionArguments args ) throws IOException
	{
		this.args = args;

		tiles = TileInfoJSONProvider.loadTilesConfiguration( args.inputFilePath() );
		fullTileSize = getMinTileSize( tiles );
		workingInterval = args.cropMinMaxInterval( fullTileSize );

		System.out.println( "Working interval is at " + Arrays.toString( Intervals.minAsLongArray( workingInterval ) ) + " of size " + Arrays.toString( Intervals.dimensionsAsLongArray( workingInterval ) ) );

		final String basePath = args.inputFilePath().substring( 0, args.inputFilePath().lastIndexOf( "." ) );
		final String outputPath = basePath + "/" + ( args.cropMinMaxIntervalStr() == null ? "fullsize" : args.cropMinMaxIntervalStr() );
		histogramsPath = basePath + "/" + "histograms";
		solutionPath = outputPath + "/" + "solution";

		// check if all tiles have the same size
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				if ( tile.getSize(d) != fullTileSize[ d ] )
				{
					System.out.println("Assumption failed: not all the tiles are of the same size");
					System.exit(1);
				}

		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "IlluminationCorrection3D" )
				//.set( "spark.driver.maxResultSize", "8g" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				//.set( "spark.kryoserializer.buffer.max", "2047m" )
				.registerKryoClasses( new Class[] { Short.class, Integer.class, Long.class, Double.class, TreeMap.class, TreeMap[].class, long[].class, short[][].class, double[].class, List.class, Tuple2.class, Interval.class, FinalInterval.class, ArrayImg.class, DoubleType.class, DoubleArray.class } )
				.set( "spark.rdd.compress", "true" )
				//.set( "spark.executor.heartbeatInterval", "10000000" )
				//.set( "spark.network.timeout", "10000000" )
			);
	}

	@Override
	public void close()
	{
		if ( sparkContext != null )
			sparkContext.close();
	}


	public < A extends ArrayImg< DoubleType, DoubleArray >, V extends TreeMap< Short, Integer > >
	void run() throws FileNotFoundException
	{
		long elapsed = System.nanoTime();

		final HistogramsProvider histogramsProvider = new HistogramsProvider(
				sparkContext,
				workingInterval,
				histogramsPath,
				tiles,
				fullTileSize,
				args.histMinValue(), args.histMaxValue(), args.bins() );

		System.out.println( "Working with stack of size " + tiles.length );
		System.out.println( "Output directory: " + solutionPath );

		System.out.println( "Loading histograms.." );
		final JavaPairRDD< Long, long[] > rddFullHistograms = histogramsProvider.getHistograms();

		final long[] referenceHistogram = histogramsProvider.getReferenceHistogram();
		System.out.println( "Obtained reference histogram of size " + referenceHistogram.length );
		System.out.println( "Reference histogram:");
		System.out.println( Arrays.toString( referenceHistogram ) );

		final HistogramSettings histogramSettings = histogramsProvider.getHistogramSettings();

		// Define the transform and calculate the image size on each scale level
		final AffineTransform3D downsamplingTransform = new AffineTransform3D();
		downsamplingTransform.set(
				0.5, 0, 0, -0.5,
				0, 0.5, 0, -0.5,
				0, 0, 0.5, -0.5
			);

		final ShiftedDownsampling shiftedDownsampling = new ShiftedDownsampling( sparkContext, workingInterval, downsamplingTransform );
		final FlatfieldCorrectionSolver solver = new FlatfieldCorrectionSolver( sparkContext );

		final int iterations = 16;
		Pair< A, A > lastSolution = null;
		for ( int iter = 0; iter < iterations; iter++ )
		{
			Pair< A, A > downsampledSolution = null;

			// solve in a bottom-top fashion (starting from the smallest scale)
			for ( int scale = shiftedDownsampling.getNumScales() - 1; scale >= 0; scale-- )
			{
				final Pair< A, A > solution;

				final ModelType modelType;
				final RegularizerModelType regularizerModelType;

				if ( iter == 0 )
					modelType = scale >= shiftedDownsampling.getNumScales() / 2 ? ModelType.FixedTranslationAffineModel : ModelType.FixedScalingAffineModel;
				else
					modelType = iter % 2 == 1 ? ModelType.FixedTranslationAffineModel : ModelType.FixedScalingAffineModel;

				if ( iter == 0 )
					regularizerModelType = scale == shiftedDownsampling.getNumScales() - 1 ? RegularizerModelType.IdentityModel : RegularizerModelType.AffineModel;
				else
					regularizerModelType = RegularizerModelType.AffineModel;


				try ( ShiftedDownsampling.PixelsMapping pixelsMapping = shiftedDownsampling.new PixelsMapping( scale ) )
				{
					final RandomAccessiblePairNullable< DoubleType, DoubleType > regularizer;
					if ( regularizerModelType == regularizerModelType.AffineModel )
					{
						final RandomAccessible< DoubleType > scalingRegularizer;
						final RandomAccessible< DoubleType > translationRegularizer;

						if ( modelType != modelType.FixedScalingAffineModel || lastSolution == null )
							scalingRegularizer = downsampledSolution != null ? shiftedDownsampling.upsample( downsampledSolution.getA() ) : null;
						else
							scalingRegularizer = shiftedDownsampling.downsampleSolutionComponent( lastSolution.getA(), pixelsMapping );

						if ( modelType != modelType.FixedTranslationAffineModel || lastSolution == null )
							translationRegularizer = downsampledSolution != null ? shiftedDownsampling.upsample( downsampledSolution.getB() ) : null;
						else
							translationRegularizer = shiftedDownsampling.downsampleSolutionComponent( lastSolution.getB(), pixelsMapping );

						regularizer = new RandomAccessiblePairNullable<>( scalingRegularizer, translationRegularizer );
					}
					else
					{
						regularizer = null;
					}

					final JavaPairRDD< Long, long[] > rddDownsampledHistograms = shiftedDownsampling.downsampleHistograms(
							rddFullHistograms,
							pixelsMapping );

					solution = solver.leastSquaresInterpolationFit(
							rddDownsampledHistograms,
							referenceHistogram,
							histogramSettings,
							pixelsMapping,
							regularizer,
							modelType,
							regularizerModelType );
				}

				downsampledSolution = solution;
				saveSolution( iter, scale, solution );
			}

			lastSolution = downsampledSolution;
		}

		elapsed = System.nanoTime() - elapsed;
		System.out.println( "----------" );
		System.out.println( String.format( "Took %f mins", elapsed / 1e9 / 60 ) );
	}


	private < A extends ArrayImg< DoubleType, DoubleArray > > void saveSolution( final int iteration, final int scale, final Pair< A, A > solution )
	{
		if ( solution.getA() != null )
			saveSolution( iteration, scale, solution.getA(), "v" );

		if ( solution.getB() != null )
			saveSolution( iteration, scale, solution.getB(), "z" );
	}

	private < A extends ArrayImg< DoubleType, DoubleArray > > void saveSolution( final int iteration, final int scale, final A solution, final String title )
	{
		final String path = solutionPath + "/iter" + iteration + "/" + scale + "/" + title + ".tif";

		Paths.get( path ).getParent().toFile().mkdirs();

		final ImagePlus imp = ImageJFunctions.wrap( solution, title );
		Utils.workaroundImagePlusNSlices( imp );
		IJ.saveAsTiff( imp, path );
	}


	private static long[] getMinTileSize( final TileInfo[] tiles )
	{
		final long[] minSize = tiles[ 0 ].getSize().clone();
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < minSize.length; d++ )
				if (minSize[ d ] > tile.getSize( d ))
					minSize[ d ] = tile.getSize( d );
		return minSize;
	}
}
