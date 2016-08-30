package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.janelia.intensity.LinearIntensityMap;
import org.janelia.util.ComparablePair;

import ij.IJ;
import ij.ImagePlus;
import ij.process.ImageProcessor;
import mpicbg.models.Affine1D;
import mpicbg.models.AffineModel1D;
import mpicbg.models.IdentityModel;
import mpicbg.models.InterpolatedAffineModel1D;
import mpicbg.models.Model;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import mpicbg.models.Tile;
import mpicbg.models.TileConfiguration;
import mpicbg.models.TranslationModel1D;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.view.IntervalView;
import net.imglib2.view.SubsampleIntervalView;
import net.imglib2.view.Views;
import scala.Tuple2;

public class PipelineIntensityCorrectionStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -8516030608688893713L;

	private static final int numCoefficients = 10;
	private static final int iterations = 100;
	private static final int subsampleStep = 1;

	private transient int counter;

	public PipelineIntensityCorrectionStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run()
	{
		final List< TilePair > overlappingShiftedTiles = TileOperations.findOverlappingTiles( job.getTiles() );
		try
		{
			matchIntensities( overlappingShiftedTiles );
		}
		catch ( final Exception e )
		{
			System.out.println( "Something went wrong during intensity correction:" );
			e.printStackTrace();
		}
	}

	/**
	 * Performs intensity correction of tiles images on a Spark cluster.
	 *
	 * @param tilePairs A list of pairs of overlapping tiles
	 */
	public < M extends Model< M > & Affine1D< M > > void matchIntensities( final List< TilePair > tilePairs ) throws Exception
	{
		// construct a map of tiles
		final Map< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TilePair tilePair : tilePairs )
			for ( final TileInfo tile : tilePair.toArray() )
				tilesMap.put( tile.getIndex(), tile );

		for ( final Integer key : job.getTilesMap().keySet() )
			if ( !tilesMap.containsKey( key ) )
				throw new Exception( "Some subset was chosen instead of the whole configuration (bad tile " + key + ")" );

		// construct a set of coefficients for every tile
		final Map< Integer, ArrayList< Tile< ? extends M > > > coeffTiles = new TreeMap<>();
		for ( final Integer key : tilesMap.keySet() )
			coeffTiles.put( key, ( ArrayList ) createCoefficientsTiles(
					new InterpolatedAffineModel1D<>(
							new InterpolatedAffineModel1D<>(
									new AffineModel1D(), new TranslationModel1D(), 0.01 ),
							new IdentityModel(), 0.01 )	) );
		/*coeffTiles.put( key, ( ArrayList ) createCoefficientsTiles(
					new AffineModel1D() ) );*/

		// coefficient connections across actual tiles
		System.out.println( "Generate pixel matches" );
		final JavaRDD< TilePair > rddPairs = sparkContext.parallelize( tilePairs );
		final JavaRDD< PairwiseMatches > taskPairs = rddPairs.map(
				new Function< TilePair, PairwiseMatches >()
				{
					private static final long serialVersionUID = -4991255417353136684L;

					@Override
					public PairwiseMatches call( final TilePair tilePair ) throws Exception
					{
						return new PairwiseMatches( tilePair, generatePairwiseMatches( tilePair ) );
					}
				});
		final List< PairwiseMatches > pairwiseCoeffs = taskPairs.collect();
		System.out.println( "All matches received, connect tiles with each other" );
		for ( final PairwiseMatches entry : pairwiseCoeffs )
		{
			final TileInfo[] tilePair = entry.getTilePair().toArray();
			final ArrayList< Tile< ? extends M > >[] actualTileCoeffs = new ArrayList[ 2 ];
			for ( int i = 0; i < 2; i++ )
				actualTileCoeffs[ i ] = coeffTiles.get( tilePair[ i ].getIndex() );

			final ArrayList< PointMatch >[][] coeffMatchesPairwise = entry.getMatches();
			for ( int c1 = 0; c1 < coeffMatchesPairwise.length; c1++ )
				for ( int c2 = 0; c2 < coeffMatchesPairwise[ c1 ].length; c2++ )
					if ( !coeffMatchesPairwise[ c1 ][ c2 ].isEmpty() )
						actualTileCoeffs[ 0 ].get( c1 ).connect( actualTileCoeffs[ 1 ].get( c2 ), coeffMatchesPairwise[ c1 ][ c2 ] );
		}

		// coefficient connections within actual tiles
		System.out.println( "Generate identity connections" );
		for ( final ArrayList< ? extends Tile< ? extends M > > value : coeffTiles.values() )
			connectCoeffsWithinTile( value );

		System.out.println( "Start optimization" );
		final TileConfiguration tc = new TileConfiguration();
		for ( final ArrayList< ? extends Tile< ? extends M > > value : coeffTiles.values() )
			tc.addTiles( value );

		tc.optimize( 0.01f, iterations, iterations, 0.75f );

		// apply intensity correction transform and save new tiles on the disk
		final String destFolder = job.getBaseFolder() + "/intensity_correction";
		new File( destFolder ).mkdirs();

		final ArrayList< Tuple2< TileInfo, ArrayList< double[] > > > tileAndCoeffs = new ArrayList<>();
		for ( final Entry< Integer, ArrayList< Tile< ? extends M > > > entry : coeffTiles.entrySet() )
		{
			final ArrayList< double[] > tileCoeffs = new ArrayList<>();
			final double[] coeffValues = new double[ 2 ];
			for ( final Tile< ? extends M > coeffTile : entry.getValue() )
			{
				coeffTile.getModel().toArray( coeffValues );
				tileCoeffs.add( coeffValues );
			}
			tileAndCoeffs.add( new Tuple2<>( tilesMap.get( entry.getKey() ), tileCoeffs ) );
		}
		final JavaRDD< Tuple2< TileInfo, ArrayList< double[] > > > rddTiles = sparkContext.parallelize( tileAndCoeffs );
		final JavaRDD< TileInfo > taskTiles = rddTiles.map(
				new Function< Tuple2< TileInfo, ArrayList< double[] > >, TileInfo >()
				{
					private static final long serialVersionUID = -4991255417353136684L;

					@Override
					public TileInfo call( final Tuple2< TileInfo, ArrayList< double[] > > tileCoeffs ) throws Exception
					{
						final TileInfo tile = tileCoeffs._1;
						final ArrayList< double[] > coeffs = tileCoeffs._2;

						System.out.println( "Applying transfomration for tile " + tile.getIndex() );

						final double[] coeffsPlain = new double[ coeffs.size() * 2 ];
						for ( int i = 0; i < 2; i++)
							for ( int j = 0; j < coeffs.size(); j++ )
								coeffsPlain[ j + i * coeffs.size() ] = coeffs.get( j )[ i ];
						final LinearIntensityMap< DoubleType > transform = new LinearIntensityMap< >( ArrayImgs.doubles( coeffsPlain, numCoefficients, numCoefficients, numCoefficients, 2 ) );

						final ImagePlus imp = IJ.openImage( tile.getFilePath() );
						final RandomAccessibleInterval r = ImagePlusImgs.from( imp );
						transform.run( r );

						tile.setFilePath( destFolder + "/" + Utils.addFilenameSuffix( new File( tile.getFilePath() ).getName(), "_transformed" ) );
						final ImagePlus transformed = ImageJFunctions.wrap( r, "" );
						IJ.saveAsTiff( transformed, tile.getFilePath() );

						return tile;
					}
				});
		final List< TileInfo > resultingTiles = taskTiles.collect();

		final Map< Integer, TileInfo > allTilesMap = job.getTilesMap();
		for ( final TileInfo transformedTile : resultingTiles )
			allTilesMap.get( transformedTile.getIndex() ).setFilePath( transformedTile.getFilePath() );

		try {
			TileInfoJSONProvider.saveTilesConfiguration( job.getTiles(), job.getBaseFolder() + "/" + Utils.addFilenameSuffix( job.getDatasetName() + ".json", "_transformed" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}

	/**
	 * Generates pixel-pixel matches between two particular tiles
	 * @param tilePair a pair of tiles
	 * @return a list of matches in "unrolled" form (double-dimensional array for pairwise matches between coefficients)
	 */
	private < M extends Model< M > & Affine1D< M >, T extends RealType< T > & NativeType< T > >
	ArrayList< PointMatch >[][] generatePairwiseMatches( final TilePair tilePair ) throws Exception
	{
		// matrix for collecting pixel-pixel matches within coefficient tiles pairwise
		final ArrayList< PointMatch >[][] coeffMatchesPairwise = new ArrayList[ coeffsPerTile() ][ coeffsPerTile() ];
		for ( int c1 = 0; c1 < coeffMatchesPairwise.length; c1++ )
			for ( int c2 = 0; c2 < coeffMatchesPairwise[ c1 ].length; c2++ )
				coeffMatchesPairwise[ c1 ][ c2 ] = new ArrayList<>();

		final TileInfo[] tilePairArr = tilePair.toArray();

		System.out.println( "Generating matches for tiles " + tilePairArr[ 0 ].getIndex() + " and " + tilePairArr[ 1 ].getIndex() );

		final Cursor< T >[] cursors = new Cursor[ 2 ];
		final long[][] localIntersectionOrigin = new long[ 2 ][];
		final long[][] tileSize = new long[ 2 ][];
		final double[] minValues = new double[ 2 ], maxValues = new double[ 2 ], valuesRange = new double[ 2 ];
		for ( int i = 0; i < 2; i++ )
		{
			final ImagePlus imp = IJ.openImage( Utils.getAbsoluteImagePath( job, tilePairArr[ i ] ) );
			final Boundaries overlap = TileOperations.getOverlappingRegion( tilePairArr[ i ], tilePairArr[ (i + 1) % 2 ] );
			localIntersectionOrigin[ i ] = overlap.getMin();
			tileSize[ i ] = tilePairArr[ i ].getSize();

			minValues[ i ] = Double.MAX_VALUE;
			maxValues[ i ] = -Double.MAX_VALUE;
			for ( int slice = 1; slice <= imp.getNSlices(); slice++ )
			{
				imp.setSlice( slice );
				final ImageProcessor sliceImgProcessor = imp.getProcessor();
				sliceImgProcessor.resetMinAndMax();
				minValues[ i ] = Math.min( sliceImgProcessor.getMin(), minValues[ i ] );
				maxValues[ i ] = Math.max( sliceImgProcessor.getMax(), maxValues[ i ] );
			}
			valuesRange[ i ] = maxValues[ i ] - minValues[ i ];

			final RandomAccessibleInterval< T > image = ImagePlusImgs.from( imp );
			final IntervalView< T > intersectionInterval = Views.interval( image, overlap );
			final SubsampleIntervalView< T > subsampledIntersectionInterval = Views.subsample( intersectionInterval, subsampleStep );
			cursors[ i ] = Views.flatIterable( subsampledIntersectionInterval ).localizingCursor();
		}

		final Point[] pointMatch = new Point[ 2 ];
		final long[] pos = new long[ job.getDimensionality() ];
		final int[] coeffTilesPlainInd = new int[ 2 ];
		while ( cursors[ 0 ].hasNext() && cursors[ 1 ].hasNext() )
		{
			for ( int i = 0; i < 2; i++ )
			{
				pointMatch[ i ] = new Point( new double[] { ( cursors[ i ].next().getRealDouble() - minValues[ i ] ) / valuesRange[ i ] } );

				cursors[ i ].localize( pos );
				// restore original position from the subsampled one
				for ( int d = 0; d < pos.length; d++ )
					pos[ d ] = pos[ d ] * subsampleStep + localIntersectionOrigin[ i ][ d ];

				final long[] coeffTileInd = matchPositionToCoeffIndex( pos, tileSize[ i ], numCoefficients );
				coeffTilesPlainInd[ i ] = IntervalIndexer.positionToIndex( coeffTileInd, coeffsDimPerTile() );
			}

			if ( pointMatch[ 0 ].getL()[ 0 ] < 0.5 && pointMatch[ 1 ].getL()[ 0 ] < 0.5 )
				continue;

			final PointMatch match = new PointMatch( pointMatch[ 0 ], pointMatch[ 1 ], pointMatch[ 0 ].getL()[ 0 ] * pointMatch[ 1 ].getL()[ 0 ] );
			coeffMatchesPairwise[ coeffTilesPlainInd[ 0 ] ][ coeffTilesPlainInd[ 1 ] ].add( match );
		}
		if ( cursors[ 0 ].hasNext() != cursors[ 1 ].hasNext() )
			throw new Exception( "Different intervals" );

		// filter matches
		/*final ArrayList< PointMatch > inliers = new ArrayList<>();
		final PointMatchFilter filter = new RansacRegressionFilter();
		for ( int c1 = 0; c1 < coeffMatchesPairwise.length; c1++ )
		{
			for ( int c2 = 0; c2 < coeffMatchesPairwise[ c1 ].length; c2++ )
			{
				final ArrayList< PointMatch > candidates = coeffMatchesPairwise[ c1 ][ c2 ];

				if ( !candidates.isEmpty() )
				{
					final int total = candidates.size();

					filter.filter( candidates, inliers );
					candidates.clear();
					candidates.addAll( inliers );
					inliers.clear();

					System.out.println( "CoeffTiles " + c1 + " and " + c2 + ": total=" + total + ", inliers=" + candidates.size() );
				}
			}
		}*/
		return coeffMatchesPairwise;
	}

	/**
	 * Generates identity connections within specified tile
	 * @param coeffTilesList a list of coefficients in unrolled form
	 */
	private void connectCoeffsWithinTile( final ArrayList< ? extends Tile< ? > > coeffTilesList )
	{
		counter = 0;
		final Set< Integer > visited = new HashSet<>();
		connectCoeffsWithinTileRecursive( coeffTilesList, null, -1, visited, new int[ job.getDimensionality() ], coeffsDimPerTile() );

		if ( counter == Math.pow( numCoefficients - 1, job.getDimensionality() ) )
			System.out.println( "counter OK" );
		else
			System.out.println( "counter failed: expected " + Math.pow( numCoefficients - 1, job.getDimensionality() ) + ", actual " + counter );
	}
	private void connectCoeffsWithinTileRecursive( final ArrayList< ? extends Tile< ? > > coeffTilesList, final Tile< ? > prevTile, final int prevPlainIndex, final Set< Integer > visited, final int[] pos, final int[] dim )
	{
		final int currPlainIndex = IntervalIndexer.positionToIndex( pos, dim );
		if ( visited.contains( currPlainIndex ) )
			return;

		visited.add( currPlainIndex );
		final Tile< ? > currTile = coeffTilesList.get( currPlainIndex );
		if ( prevTile != null)
			identityConnect( prevTile, prevPlainIndex, currTile, currPlainIndex );

		for ( int d = 0; d < pos.length; d++ )
		{
			final int[] nextPos = pos.clone();
			nextPos[ d ]++;
			if ( nextPos[ d ] < dim[ d ] )
				connectCoeffsWithinTileRecursive( coeffTilesList, currTile, currPlainIndex, visited, nextPos, dim );
		}
	}

	private void identityConnect( final Tile< ? > t1, final int p1, final Tile< ? > t2, final int p2 )
	{
		counter++;
		final ArrayList< PointMatch > matches = new ArrayList< >();

		final double min1 = ( minsMaxs.containsKey( p1 ) ? minsMaxs.get( p1 ).first  : 0 );
		final double max1 = ( minsMaxs.containsKey( p1 ) ? minsMaxs.get( p1 ).second : 1 );

		final double min2 = ( minsMaxs.containsKey( p2 ) ? minsMaxs.get( p2 ).first  : 0 );
		final double max2 = ( minsMaxs.containsKey( p2 ) ? minsMaxs.get( p2 ).second : 1 );

		matches.add( new PointMatch( new Point( new double[] { min2 } ), new Point( new double[] { min1 } ) ) );
		matches.add( new PointMatch( new Point( new double[] { max2 } ), new Point( new double[] { max1 } ) ) );

		t1.connect( t2, matches );
	}

	/**
	 * Generates identity connections within specified tile
	 * @param templateModel a basic model of intensity transformation
	 * @return a list of coefficients in unrolled form
	 */
	private < M extends Model< M > & Affine1D< M > > ArrayList< Tile< ? extends M > > createCoefficientsTiles( final M templateModel )
	{
		final ArrayList< Tile< ? extends M > > coeffTiles = new ArrayList<>();
		for ( int i = 0; i < coeffsPerTile(); i++ )
			coeffTiles.add( new Tile<>( templateModel.copy() ) );
		return coeffTiles;
	}

	private int coeffsPerTile()
	{
		return ( int ) Math.pow( numCoefficients, job.getDimensionality() );
	}
	private int[] coeffsDimPerTile()
	{
		final int[] tileCoeffDim = new int[ job.getDimensionality() ];
		Arrays.fill( tileCoeffDim, numCoefficients );
		return tileCoeffDim;
	}

	private long[] matchPositionToCoeffIndex( final long[] pos, final long[] size, final int coeffsPerDim )
	{
		final long[] ret = new long[ job.getDimensionality() ];
		for ( int d = 0; d < ret.length; d++ )
			ret[ d ] = coeffsPerDim * pos[ d ] / size[ d ];
		return ret;
	}



	/**
	 * * * * * *
	 *
	 * Experimental code for intensity correction of a single tile
	 *
	 * * * * * *
	 */
	transient ImagePlus singleTile = null;
	transient Map< Integer, ComparablePair< Double, Double > > minsMaxs = new HashMap<>();
	public < M extends Model< M > & Affine1D< M > > void matchIntensities( final TileInfo tile ) throws Exception
	{
		// construct a set of coefficients for every tile
		final ArrayList< Tile< ? extends M > > coeffTiles = ( ArrayList ) createCoefficientsTiles(
				new AffineModel1D() );

		singleTile = IJ.openImage( tile.getFilePath() );
		final RandomAccessibleInterval rai = ImagePlusImgs.from( singleTile );
		TileOperations.translateTilesToOrigin( new TileInfo[] { tile } );
		final ArrayList< TileInfo > coeffSubregions = TileOperations.divideSpaceByCount( tile.getBoundaries(), numCoefficients );
		for ( final TileInfo coeffSubregion : coeffSubregions )
		{
			final IntervalView coeffTileInterval = Views.interval( rai, coeffSubregion.getBoundaries() );
			final ImagePlus coeffTileIntervalWrapped = ImageJFunctions.wrap( coeffTileInterval, "" );
			final int[] dimensions = coeffTileIntervalWrapped.getDimensions();
			final int numSlices = dimensions[2 ] * dimensions[3] * dimensions[4];

			double minValue = Double.MAX_VALUE, maxValue = -Double.MAX_VALUE;
			for ( int slice = 1; slice <= numSlices; slice++ )
			{
				coeffTileIntervalWrapped.setSlice( slice );
				final ImageProcessor sliceImgProcessor = coeffTileIntervalWrapped.getProcessor();
				sliceImgProcessor.resetMinAndMax();
				minValue = Math.min( sliceImgProcessor.getMin(), minValue );
				maxValue = Math.max( sliceImgProcessor.getMax(), maxValue );
			}

			final int[] coeffIndices = new int[ job.getDimensionality() ];
			for ( int d = 0; d < coeffIndices.length; d++ )
				coeffIndices[ d ] = Math.min( (int) ( coeffSubregion.getPosition( d ) / coeffSubregion.getSize( d ) ), numCoefficients - 1 );

			final int plainInd = IntervalIndexer.positionToIndex( coeffIndices, coeffsDimPerTile() );
			minsMaxs.put( plainInd, new ComparablePair( minValue, maxValue ) );
		}

		{
			double minValue = Double.MAX_VALUE, maxValue = -Double.MAX_VALUE;
			for ( final ComparablePair< Double, Double > val : minsMaxs.values() )
			{
				minValue = Math.min( val.first, minValue );
				maxValue = Math.max( val.second, maxValue );
			}
			final double valuesRange = maxValue - minValue;
			for ( final ComparablePair< Double, Double > val : minsMaxs.values() )
			{
				val.first = ( val.first - minValue ) / valuesRange;
				val.second = ( val.second - minValue ) / valuesRange;
			}

			//for ( final ComparablePair< Double, Double > val : minsMaxs.values() )
			//	System.out.println( val );
		}


		// coefficient connections within actual tiles
		System.out.println( "Generate identity connections" );
		connectCoeffsWithinTile( coeffTiles );

		System.out.println( "Start optimization" );
		final TileConfiguration tc = new TileConfiguration();
		tc.addTiles( coeffTiles );

		tc.optimize( 0.01f, iterations, iterations, 0.75f );

		// apply intensity correction transform and save new tiles on the disk
		final String destFolder = job.getBaseFolder() + "/intensity_correction";
		new File( destFolder ).mkdirs();

		final ArrayList< Tuple2< TileInfo, ArrayList< double[] > > > tileAndCoeffs = new ArrayList<>();

		{
			final ArrayList< double[] > tileCoeffs = new ArrayList<>();
			final double[] coeffValues = new double[ 2 ];
			System.out.println( "Coeffs:" );
			for ( final Tile< ? extends M > coeffTile : coeffTiles )
			{
				coeffTile.getModel().toArray( coeffValues );
				tileCoeffs.add( coeffValues );
				System.out.println( Arrays.toString( coeffValues ) );
			}
			tileAndCoeffs.add( new Tuple2<>( tile, tileCoeffs ) );
		}
		final JavaRDD< Tuple2< TileInfo, ArrayList< double[] > > > rddTiles = sparkContext.parallelize( tileAndCoeffs );
		final JavaRDD< TileInfo > taskTiles = rddTiles.map(
				new Function< Tuple2< TileInfo, ArrayList< double[] > >, TileInfo >()
				{
					private static final long serialVersionUID = -4991255417353136684L;

					@Override
					public TileInfo call( final Tuple2< TileInfo, ArrayList< double[] > > tileCoeffs ) throws Exception
					{
						final TileInfo tile = tileCoeffs._1;
						final ArrayList< double[] > coeffs = tileCoeffs._2;

						System.out.println( "Applying transfomration for tile " + tile.getIndex() );

						final double[] coeffsPlain = new double[ coeffs.size() * 2 ];
						for ( int i = 0; i < 2; i++)
							for ( int j = 0; j < coeffs.size(); j++ )
								coeffsPlain[ j + i * coeffs.size() ] = coeffs.get( j )[ i ];
						final LinearIntensityMap< DoubleType > transform = new LinearIntensityMap< >( ArrayImgs.doubles( coeffsPlain, numCoefficients, numCoefficients, numCoefficients, 2 ) );

						final ImagePlus imp = IJ.openImage( tile.getFilePath() );
						final RandomAccessibleInterval r = ImagePlusImgs.from( imp );
						transform.run( r );

						tile.setFilePath( destFolder + "/" + Utils.addFilenameSuffix( new File( tile.getFilePath() ).getName(), "_transformed" ) );
						final ImagePlus transformed = ImageJFunctions.wrap( r, "" );
						IJ.saveAsTiff( transformed, tile.getFilePath() );

						return tile;
					}
				});
		final List< TileInfo > resultingTiles = taskTiles.collect();
		final TileInfo[] allTiles = job.getTiles();
		final Map< Integer, TileInfo > allTilesMap = job.getTilesMap();
		for ( final TileInfo transformedTile : resultingTiles )
			allTilesMap.get( transformedTile.getIndex() ).setFilePath( transformedTile.getFilePath() );

		try {
			TileInfoJSONProvider.saveTilesConfiguration( allTiles, job.getBaseFolder() + "/" + Utils.addFilenameSuffix( job.getDatasetName() + ".json", "_transformed" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
}
