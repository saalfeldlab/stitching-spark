package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;
import org.janelia.intensity.LinearIntensityMap;
import org.janelia.util.Conversions;

import bdv.export.Downsample;
import ij.ImagePlus;
import mpicbg.models.Affine1D;
import mpicbg.models.AffineModel1D;
import mpicbg.models.IdentityModel;
import mpicbg.models.IllDefinedDataPointsException;
import mpicbg.models.InterpolatedAffineModel1D;
import mpicbg.models.Model;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import mpicbg.models.Tile;
import mpicbg.models.TileConfiguration;
import mpicbg.models.TranslationModel1D;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.interpolation.randomaccess.FloorInterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.Scale;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsNullable;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;
import scala.Tuple2;

/**
 * Performs intensity correction of a set of tiles.
 * Saves updated tile configuration on the disk.
 *
 * @author Igor Pisarev
 */

public class PipelineIntensityCorrectionStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -8516030608688893713L;

	private static final int COEFFICIENT_DIM = 100;
	private static final int DOWNSAMPLING_FACTOR = 16;
	private static final int OPTIMIZER_ITERATIONS = 2000;
	private static final int HISTOGRAM_BINS = 256;

	private static final double SCALE_LAMBDA = 0.01;
	private static final double TRANSLATION_LAMBDA = 0.01;
	private static final double NEIGHBOR_WEIGHT = 0.1;

	private int[] coeffPerTileDimensions;
	private int coeffPerTileCount;

	public PipelineIntensityCorrectionStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run() throws PipelineExecutionException
	{
		final DataProvider dataProvider = job.getDataProvider();
		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final TileInfo[] transformedTiles = matchIntensities(
					job.getTiles( channel ),
					new InterpolatedAffineModel1D<>(
							new InterpolatedAffineModel1D<>(
									new AffineModel1D(), new TranslationModel1D(), TRANSLATION_LAMBDA ),
							new IdentityModel(), SCALE_LAMBDA ) );

			try
			{
				job.setTiles( transformedTiles, channel );
				final String outputPath = Utils.addFilenameSuffix( job.getArgs().inputTileConfigurations().get( channel ), "_transformed" );
				TileInfoJSONProvider.saveTilesConfiguration( transformedTiles, dataProvider.getJsonWriter( URI.create( outputPath ) ) );
			}
			catch ( final IOException e )
			{
				e.printStackTrace();
			}
			catch ( final Exception e )
			{
				throw new PipelineExecutionException( e );
			}
		}
	}

	/**
	 * Performs intensity correction of tiles images on a Spark cluster.
	 *
	 * @param tilePairs A list of pairs of overlapping tiles
	 */
	public < M extends Model< M > & Affine1D< M >, T extends RealType< T > & NativeType< T > > TileInfo[] matchIntensities( final TileInfo[] tiles, final M templateModel ) throws PipelineExecutionException
	{
		final List< TilePair > overlappingPairs = TileOperations.findOverlappingTiles( tiles );
		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( tiles );

		// set coeff dimensions
		final long[] tileSize = getMinTileSize( tilesMap.values().toArray( new TileInfo[ 0 ] ) );
		for ( final TileInfo tile : tilesMap.values() )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				if ( tile.getSize(d) != tileSize[ d ] )
					throw new PipelineExecutionException( "Assumption failed: not all the tiles are of the same size" );
		coeffPerTileDimensions = new int[ tileSize.length ];
		for ( int d = 0; d < tileSize.length; ++d )
			coeffPerTileDimensions[ d ] = ( int ) ( tileSize[ d ] / COEFFICIENT_DIM );
		coeffPerTileCount = ( int ) Intervals.numElements( new FinalDimensions( coeffPerTileDimensions ) );

		// construct a set of coefficients for every tile
		final Map< Integer, List< Tile< ? extends M > > > coefficients = new TreeMap<>();
		for ( final Integer key : tilesMap.keySet() )
			coefficients.put( key, createCoefficientsTiles( templateModel ) );

		// retrieve min/max values for coefficients
		final Map< Integer, double[] > tilesMinMax = getTilesMinMax( new ArrayList<>( tilesMap.values() ) );

		final double[] minmaxGlobal = new double[] { Double.MAX_VALUE, -Double.MAX_VALUE };
		for ( final double[] minmax : tilesMinMax.values() )
		{
			minmaxGlobal[ 0 ] = Math.min( minmax[ 0 ], minmaxGlobal[ 0 ] );
			minmaxGlobal[ 1 ] = Math.max( minmax[ 1 ], minmaxGlobal[ 1 ] );
		}

		// generate coefficient matches across pairs of tiles
		System.out.println( "Generate pixel matches" );
		final Map< TilePair, Map< Integer, Map< Integer, List< PointMatch > > > > pairwiseCoeffs = sparkContext.parallelize( overlappingPairs ).mapToPair(
				tilePair -> new Tuple2<>( tilePair, generatePairwiseHistogramsMatches( tilePair, new ValuePair<>( minmaxGlobal[ 0 ], minmaxGlobal[ 1 ] ) ) )
			).collectAsMap();

		// connect coefficients with these matches
		for ( final Entry< TilePair, Map< Integer, Map< Integer, List< PointMatch > > > > entry : pairwiseCoeffs.entrySet() )
		{
			final TileInfo[] tilePair = entry.getKey().toArray();
			final List< Tile< ? extends M > >[] coeff = new List[ 2 ];
			for ( int i = 0; i < 2; ++i )
				coeff[ i ] = coefficients.get( tilePair[ i ].getIndex() );

			for ( final Entry< Integer, Map< Integer, List< PointMatch > > > firstEntry : entry.getValue().entrySet() )
				for ( final Entry< Integer, List< PointMatch > > secondEntry : firstEntry.getValue().entrySet() )
					coeff[ 0 ].get( firstEntry.getKey() ).connect( coeff[ 1 ].get( secondEntry.getKey() ), secondEntry.getValue() );
		}

		// generate coefficient matches within each tile
		System.out.println( String.format( "Generate min-min and max-max connections within each tile" ) );
		for ( final Entry< Integer, List< Tile< ? extends M > > > entry : coefficients.entrySet() )
			connectCoeffsWithinTile( entry.getValue(), tilesMinMax.get( entry.getKey() ) );

		System.out.println( "Start optimization" );
		final TileConfiguration tc = new TileConfiguration();
		for ( final List< ? extends Tile< ? extends M > > value : coefficients.values() )
			tc.addTiles( value );

		try
		{
			tc.optimize( 0.01f, OPTIMIZER_ITERATIONS, OPTIMIZER_ITERATIONS, 0.75f );
		}
		catch ( final NotEnoughDataPointsException | IllDefinedDataPointsException e )
		{
			e.printStackTrace();
			throw new PipelineExecutionException( e );
		}

		System.out.println( "*** Optimization done ***" );

		// apply intensity correction transform and save new tiles on the disk
		final String destFolder = job.getBaseFolder() + "/intensity_correction";
		new File( destFolder ).mkdirs();

		final List< Tuple2< TileInfo, List< double[] > > > tileAndCoeffs = new ArrayList<>();
		for ( final Entry< Integer, List< Tile< ? extends M > > > entry : coefficients.entrySet() )
		{
			final List< double[] > coeffs = new ArrayList<>();
			for ( final Tile< ? extends M > coeff : entry.getValue() )
			{
				final double[] coeffValues = new double[ 2 ];
				coeff.getModel().toArray( coeffValues );
				coeffs.add( coeffValues );
			}
			tileAndCoeffs.add( new Tuple2<>( tilesMap.get( entry.getKey() ), coeffs ) );
		}
		final JavaRDD< TileInfo > taskTiles = sparkContext.parallelize( tileAndCoeffs ).map( tuple ->
			{
				final DataProvider dataProviderLocal = job.getDataProvider();
				final TileInfo tile = tuple._1();
				final List< double[] > coeffs = tuple._2();

				System.out.println( "Applying transformation for tile " + tile.getIndex() );

				final int[] unrolledCoeffsDim = new int[ coeffPerTileDimensions.length + 1 ];
				System.arraycopy( coeffPerTileDimensions, 0, unrolledCoeffsDim, 0, coeffPerTileDimensions.length );
				unrolledCoeffsDim[ unrolledCoeffsDim.length - 1 ] = 2;

				final int[] unrolledCoeffPos = new int[ unrolledCoeffsDim.length ];
				final double[] unrolledCoeffs = new double[ ( int ) Intervals.numElements( unrolledCoeffsDim ) ];
				for ( int ind = 0; ind < coeffs.size(); ++ind )
				{
					IntervalIndexer.indexToPosition( ind, coeffPerTileDimensions, unrolledCoeffPos );
					for ( int i = 0; i < unrolledCoeffsDim[ unrolledCoeffsDim.length - 1 ]; ++i )
					{
						unrolledCoeffPos[ unrolledCoeffPos.length - 1 ] = i;
						unrolledCoeffs[ IntervalIndexer.positionToIndex( unrolledCoeffPos, unrolledCoeffsDim ) ] = coeffs.get( ind )[ i ];
					}
				}
				final LinearIntensityMap< DoubleType > transform = new LinearIntensityMap< >( ArrayImgs.doubles( unrolledCoeffs, Conversions.toLongArray( unrolledCoeffsDim ) ) );

				final RandomAccessibleInterval< T > src = TileLoader.loadTile( tile, dataProviderLocal );
				final ImagePlusImg< T, ? > dst = new ImagePlusImgFactory< T >().create( src, Util.getTypeFromInterval( src ) );
				transform.run( dst );

				tile.setFilePath( destFolder + "/transformed_" + PathResolver.getFileName( tile.getFilePath() ) );
				final ImagePlus transformed = dst.getImagePlus();
				Utils.workaroundImagePlusNSlices( transformed );
				dataProviderLocal.saveImage( transformed, URI.create( tile.getFilePath() ) );
				return tile;
			} );
		final List< TileInfo > resultingTiles = taskTiles.collect();

		System.out.println( "*** Transformation applied ***" );

		for ( final TileInfo transformedTile : resultingTiles )
			tilesMap.get( transformedTile.getIndex() ).setFilePath( transformedTile.getFilePath() );

		return tiles;
	}


	private < T extends RealType< T > & NativeType< T > > Tuple2< Double, Double > getStackMinMax( final List< TileInfo > tiles )
	{
		return sparkContext.parallelize( tiles ).map( tile ->
			{
				final DataProvider dataProviderLocal = job.getDataProvider();
				final RandomAccessibleInterval< T > image = TileLoader.loadTile( tile, dataProviderLocal );
				final Cursor< T > cursor = Views.iterable( image ).cursor();
				double min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
				while ( cursor.hasNext() )
				{
					final double val = cursor.next().getRealDouble();
					min = Math.min( val, min );
					max = Math.max( val, max );
				}
				return new Tuple2<>( min, max );
			} )
		.reduce( ( a, b ) -> new Tuple2<>( Math.min( a._1(), b._1() ), Math.max( a._2(), b._2() ) ) );
	}


	private < T extends RealType< T > & NativeType< T > > Map< Integer, List< double[] > > getCoeffsMinMax( final List< TileInfo > tiles )
	{
		return sparkContext.parallelize( tiles ).mapToPair( tile ->
			{
				final DataProvider dataProviderLocal = job.getDataProvider();
				final RandomAccessibleInterval< T > image = TileLoader.loadTile( tile, dataProviderLocal );

				// calculate dimensions of downsampled image
				final long[] downsampledImageSize = new long[ image.numDimensions() ];
				for ( int d = 0; d < image.numDimensions(); ++d )
					downsampledImageSize[ d ] = image.dimension( d ) / DOWNSAMPLING_FACTOR;

				// downsample the image
				final RandomAccessibleInterval< T > downsampledImage = new ImagePlusImgFactory< T >().create( downsampledImageSize, Util.getTypeFromInterval( image ).createVariable() );
				final int[] downsamplingFactors = new int[ downsampledImage.numDimensions() ];
				Arrays.fill( downsamplingFactors, DOWNSAMPLING_FACTOR );
				Downsample.downsample( image, downsampledImage, downsamplingFactors );

				final Cursor< T > imgCursor = Views.flatIterable( downsampledImage ).cursor();

				final RandomAccessibleInterval< IntType > coefficientsMap = createCoefficientsIndexingMap( downsampledImage );
				final Cursor< IntType > coeffCursor = Views.flatIterable( coefficientsMap ).cursor();

				final List< double[] > coeffsMinMax = new ArrayList<>();
				for ( int i = 0; i < coeffPerTileCount; ++i )
					coeffsMinMax.add( new double[] { Double.MAX_VALUE, -Double.MAX_VALUE } );

				while ( imgCursor.hasNext() || coeffCursor.hasNext() )
				{
					final double val = imgCursor.next().getRealDouble();
					final int coeff = coeffCursor.next().get();
					final double[] minmax = coeffsMinMax.get( coeff );
					minmax[ 0 ] = Math.min( val, minmax[ 0 ] );
					minmax[ 1 ] = Math.max( val, minmax[ 1 ] );
				}
				return new Tuple2<>( tile.getIndex(), coeffsMinMax );
			} )
		.collectAsMap();
	}


	private < T extends RealType< T > & NativeType< T > > Map< Integer, double[] > getTilesMinMax( final List< TileInfo > tiles )
	{
		return sparkContext.parallelize( tiles ).mapToPair( tile ->
			{
				final DataProvider dataProviderLocal = job.getDataProvider();
				final RandomAccessibleInterval< T > image = TileLoader.loadTile( tile, dataProviderLocal );

				// calculate dimensions of the downsampled image
				final long[] downsampledImageSize = new long[ image.numDimensions() ];
				for ( int d = 0; d < image.numDimensions(); ++d )
					downsampledImageSize[ d ] = image.dimension( d ) / DOWNSAMPLING_FACTOR;

				// downsample the image
				final RandomAccessibleInterval< T > downsampledImage = new ImagePlusImgFactory< T >().create( downsampledImageSize, Util.getTypeFromInterval( image ).createVariable() );
				final int[] downsamplingFactors = new int[ downsampledImage.numDimensions() ];
				Arrays.fill( downsamplingFactors, DOWNSAMPLING_FACTOR );
				Downsample.downsample( image, downsampledImage, downsamplingFactors );

				final double[] minmax = new double[] { Double.MAX_VALUE, -Double.MAX_VALUE };

				final Cursor< T > imgCursor = Views.iterable( downsampledImage ).cursor();
				while ( imgCursor.hasNext() )
				{
					final double val = imgCursor.next().getRealDouble();
					minmax[ 0 ] = Math.min( val, minmax[ 0 ] );
					minmax[ 1 ] = Math.max( val, minmax[ 1 ] );
				}
				return new Tuple2<>( tile.getIndex(), minmax );
			} )
		.collectAsMap();
	}


	private < M extends Model< M > & Affine1D< M >, T extends RealType< T > & NativeType< T > >
	Map< Integer, Map< Integer, List< PointMatch > > > generatePairwiseHistogramsMatches( final TilePair tilePair, final Pair< Double, Double > valueRange ) throws Exception
	{
		final DataProvider dataProvider = job.getDataProvider();

		final Map< Integer, Map< Integer, List< PointMatch > > > matches = new HashMap<>();

		final TileInfo[] tilePairArr = tilePair.toArray();
		System.out.println( "Generating matches for tiles " + tilePairArr[ 0 ].getIndex() + " and " + tilePairArr[ 1 ].getIndex() );

		final Dimensions[] downsampledImageSize = new Dimensions[ 2 ];
		final RealRandomAccessible< FloatType >[] translatedInterpolatedDownsampledImages = new RealRandomAccessible[ 2 ];
		final RealInterval[] translatedDownsampledIntervals = new RealInterval[ 2 ];
		for ( int i = 0; i < 2; ++i )
		{
			// load the image
			final RandomAccessibleInterval< T > rawImage = TileLoader.loadTile( tilePairArr[ i ], dataProvider );

			// convert it to float to retain precision after downsampling
			final RandomAccessibleInterval< FloatType > image = Views.interval( Converters.convert( rawImage, new RealFloatConverter<>(), new FloatType() ), rawImage );

			// calculate dimensions of downsampled image
			final long[] downsampledImageSizeArray = new long[ image.numDimensions() ];
			for ( int d = 0; d < image.numDimensions(); ++d )
				downsampledImageSizeArray[ d ] = image.dimension( d ) / DOWNSAMPLING_FACTOR;
			downsampledImageSize[ i ] = new FinalDimensions( downsampledImageSizeArray );

			// downsample the image
			final RandomAccessibleInterval< FloatType > downsampledImage = new ImagePlusImgFactory< FloatType >().create( downsampledImageSize[ i ], new FloatType() );
			final int[] downsamplingFactors = new int[ downsampledImage.numDimensions() ];
			Arrays.fill( downsamplingFactors, DOWNSAMPLING_FACTOR );
			Downsample.downsample( image, downsampledImage, downsamplingFactors );

			// extend and interpolate
			final RealRandomAccessible< FloatType > interpolatedDownsampledImage = Views.interpolate( Views.extendBorder( downsampledImage ), new NLinearInterpolatorFactory<>() );

			// set position in downsampled space
			final double[] downsampledImageMin = new double[ downsampledImage.numDimensions() ];
			for ( int d = 0; d < downsampledImageMin.length; ++d )
				downsampledImageMin[ d ] = tilePairArr[ i ].getPosition( d ) / DOWNSAMPLING_FACTOR;
			final Translation translationTransform = new Translation( downsampledImageMin );
			translatedInterpolatedDownsampledImages[ i ] = RealViews.transform( interpolatedDownsampledImage, translationTransform );

			// define an interval with corresponding mins and maxs
			final double[] downsampledImageMax = new double[ downsampledImageMin.length ];
			for ( int d = 0; d < downsampledImageMin.length; ++d )
				downsampledImageMax[ d ] = downsampledImageMin[ d ] + downsampledImageSize[ i ].dimension( d ) - 1.0 / DOWNSAMPLING_FACTOR;
			translatedDownsampledIntervals[ i ] = new FinalRealInterval( downsampledImageMin, downsampledImageMax );

			// TODO: remove. test export
//			final String destFolder = job.getBaseFolder() + "/intensity_correction";
//			new File( destFolder ).mkdirs();
//			final ImagePlus downsampledImp = downsampledImage.getImagePlus();
//			Utils.workaroundImagePlusNSlices( downsampledImp );
//			IJ.saveAsTiff( downsampledImp, destFolder + "/downsampled_" + Paths.get( tilePairArr[ i ].getFilePath() ).getFileName().toString() );
//
//			final Boundaries fullSizeOverlap = TileOperations.getOverlappingRegion( tilePairArr[ i ], tilePairArr[ ( i + 1 ) % 2 ] );
//			final RandomAccessibleInterval< T > fullSizeOverlapImage = Views.interval( image, fullSizeOverlap );
//			final ImagePlus fullSizeOverlapImp = ImageJFunctions.wrap( fullSizeOverlapImage, "" );
//			Utils.workaroundImagePlusNSlices( fullSizeOverlapImp );
//			IJ.saveAsTiff( fullSizeOverlapImp, destFolder + "/overlap_" + Paths.get( tilePairArr[ i ].getFilePath() ).getFileName().toString() );
		}

		// find an intersection
		final RealInterval intersectionRealInterval = IntervalsNullable.intersectReal( translatedDownsampledIntervals[ 0 ], translatedDownsampledIntervals[ 1 ] );
		if ( intersectionRealInterval == null )
			throw new PipelineExecutionException( "Tiles " + tilePair.getA().getIndex() + " and " + tilePair.getB().getIndex() + " do not intersect" );

		// create integer interval from a real intersection interval
		final Interval intersectionInterval = TileOperations.floorCeilRealInterval( intersectionRealInterval );

		final Cursor< FloatType >[] imgCursors = new Cursor[ 2 ];
		final Cursor< IntType >[] coeffCursors = new Cursor[ 2 ];
		for ( int i = 0; i < 2; ++i )
		{
			// crop the intersection region
			final RandomAccessibleInterval< FloatType > cropped = Views.interval( Views.raster( translatedInterpolatedDownsampledImages[ i ] ), intersectionInterval );

			// translate it to the relative coordinate space of that tile
			final Interval rasterizedInterval = TileOperations.floorCeilRealInterval( translatedDownsampledIntervals[ i ] );
			final RandomAccessibleInterval< FloatType > croppedOffset = Views.offset( cropped, Intervals.minAsLongArray( rasterizedInterval ) );

			// create coefficients map and crop it to the desired interval
			final RandomAccessibleInterval< IntType > coefficientsMap = createCoefficientsIndexingMap( downsampledImageSize[ i ] );
			final RandomAccessibleInterval< IntType > croppedCoefficientsMap = Views.interval( coefficientsMap, croppedOffset );

			// prepare cursors
			imgCursors[ i ] = Views.flatIterable( croppedOffset ).cursor();
			coeffCursors[ i ] = Views.flatIterable( croppedCoefficientsMap ).cursor();

			// TODO: remove. test export
//			final String destFolder = job.getBaseFolder() + "/intensity_correction";
//			final ImagePlus downsampledOverlapImp = ImageJFunctions.wrap( cropped, "" );
//			Utils.workaroundImagePlusNSlices( downsampledOverlapImp );
//			IJ.saveAsTiff( downsampledOverlapImp, destFolder + "/downsampled-overlap_" + Paths.get( tilePairArr[ i ].getFilePath() ).getFileName().toString() );
		}


		// FIXME: adapt to work with new histogram matching implementation
		/*final double[] value = new double[ 2 ];
		final int[] coeffIndex = new int[ 2 ];
		final Map< Integer, Map< Integer, Histogram[] > > histograms = new HashMap<>();
		while ( imgCursors[ 0 ].hasNext() || imgCursors[ 1 ].hasNext() || coeffCursors[ 0 ].hasNext() || coeffCursors[ 1 ].hasNext() )
		{
			for ( int i = 0; i < 2; ++i )
			{
				value[ i ] = imgCursors[ i ].next().getRealDouble();
				coeffIndex[ i ] = coeffCursors[ i ].next().get();
			}

			if ( !histograms.containsKey( coeffIndex[ 0 ] ) )
				histograms.put( coeffIndex[ 0 ], new HashMap<>() );

			if ( !histograms.get( coeffIndex[ 0 ] ).containsKey( coeffIndex[ 1 ] ) )
				histograms.get( coeffIndex[ 0 ] ).put( coeffIndex[ 1 ], new Histogram[ 2 ] );

			final Histogram[] pairHistograms = histograms.get( coeffIndex[ 0 ] ).get( coeffIndex[ 1 ] );
			for ( int i = 0; i < 2; ++i )
			{
				if ( pairHistograms[ i ] == null )
					pairHistograms[ i ] = new Histogram( valueRange.getA(), valueRange.getB(), HISTOGRAM_BINS );
				pairHistograms[ i ].put( value[ i ] );
			}
		}

		for ( final Entry< Integer, Map< Integer, Histogram[] > > firstEntry : histograms.entrySet() )
		{
			for ( final Entry< Integer, Histogram[] > secondEntry : firstEntry.getValue().entrySet() )
			{
				final List< PointMatch > matchesList = HistogramMatching.generateHistogramMatches( secondEntry.getValue()[ 0 ], secondEntry.getValue()[ 1 ] );

				if ( !matches.containsKey( firstEntry.getKey() ) )
					matches.put( firstEntry.getKey(), new HashMap<>() );
				matches.get( firstEntry.getKey() ).put( secondEntry.getKey(), matchesList );
			}
		}

		return matches;*/
		return null;
	}


	/**
	 * Generates identity matches between coefficients within given tile
	 * @param coeffTilesList a list of coefficients in unrolled form
	{
	 */
	private void connectCoeffsWithinTile( final List< ? extends Tile< ? > > coeffList, final double[] minmax )
	{
		final double weight = 1;
		final int[] pos = new int[ coeffPerTileDimensions.length ];
		for ( int ind = 0; ind < coeffList.size(); ind++ )
		{
			IntervalIndexer.indexToPosition( ind, coeffPerTileDimensions, pos );
			for ( int d = 0; d < pos.length; d++ )
			{
				final int[] posNext = pos.clone();
				posNext[ d ]++;
				if ( posNext[ d ] < coeffPerTileDimensions[ d ] )
				{
					final int indNext = IntervalIndexer.positionToIndex( posNext, coeffPerTileDimensions );
					connectCoeffPairWithinTile( coeffList.get( ind ), coeffList.get( indNext ), minmax, NEIGHBOR_WEIGHT );
				}
			}
		}
	}

	private void connectCoeffPairWithinTile( final Tile< ? > t1, final Tile< ? > t2, final double[] minmax, final double weight )
	{
		final List< PointMatch > matches = new ArrayList<>();
		matches.add( new PointMatch( new Point( new double[] { minmax[ 0 ] } ), new Point( new double[] { minmax[ 0 ] } ), weight ) );
		matches.add( new PointMatch( new Point( new double[] { Math.max( minmax[ 0 ] + 1, minmax[ 1 ] ) } ), new Point( new double[] { Math.max( minmax[ 0 ] + 1, minmax[ 1 ] ) } ), weight ) );
		t1.connect( t2, matches );
	}


	private RandomAccessibleInterval< IntType > createCoefficientsIndexingMap( final Dimensions dimensions )
	{
		return createCoefficientsIndexingMap( dimensions, coeffPerTileDimensions );
	}
	public static RandomAccessibleInterval< IntType > createCoefficientsIndexingMap( final Dimensions dimensions, final int[] coeffsDim )
	{
		final int[] coeffIndexes = new int[ ( int ) Intervals.numElements( coeffsDim ) ];
		for ( int i = 0; i < coeffIndexes.length; ++i )
			coeffIndexes[ i ] = i;
		final RandomAccessibleInterval< IntType > coeffsImg = ArrayImgs.ints( coeffIndexes, Conversions.toLongArray( coeffsDim ) );

		final double[] scaling = new double[ dimensions.numDimensions() ];
		for ( int d = 0; d < scaling.length; ++d )
			scaling[ d ] = ( double ) dimensions.dimension( d ) / coeffsDim[ d ];
		final Scale scaleTransform = new Scale( scaling );
		final RealRandomAccessible< IntType > coeffsInterpolatedStretchedImg = RealViews.transform( Views.interpolate( Views.extendBorder( coeffsImg ), new FloorInterpolatorFactory<>() ), scaleTransform );
		final RandomAccessibleInterval< IntType > coeffsStretchedImg = Views.interval( Views.raster( coeffsInterpolatedStretchedImg ), new FinalInterval( dimensions ) );
		return coeffsStretchedImg;
	}


	/**
	 * Generates identity connections within specified tile
	 * @param templateModel a basic model of intensity transformation
	 * @return a list of coefficients in unrolled form
	 */
	private < M extends Model< M > & Affine1D< M > > List< Tile< ? extends M > > createCoefficientsTiles( final M templateModel )
	{
		final ArrayList< Tile< ? extends M > > coeffTiles = new ArrayList<>();
		for ( int i = 0; i < coeffPerTileCount; i++ )
			coeffTiles.add( new Tile<>( templateModel.copy() ) );
		return coeffTiles;
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