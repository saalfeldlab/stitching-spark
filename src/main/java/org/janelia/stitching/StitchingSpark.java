package org.janelia.stitching;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.janelia.stitching.StitchingJob.Mode;
import org.janelia.stitching.analysis.ThresholdEstimation;

import ij.IJ;
import ij.ImagePlus;
import ij.gui.Roi;
import ij.plugin.filter.GaussianBlur;
import mpicbg.models.Tile;
import mpicbg.stitching.ComparePair;
import mpicbg.stitching.GlobalOptimization;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
import mpicbg.stitching.PairWiseStitchingImgLib;
import mpicbg.stitching.PairWiseStitchingResult;
import mpicbg.stitching.StitchingParameters;
import stitching.utils.Log;

/**
 * Driver class for running stitching jobs on a Spark cluster.
 *
 * @author Igor Pisarev
 */

public class StitchingSpark implements Runnable, Serializable {

	public static void main( final String[] args ) {
		final StitchingArguments stitchingArgs = new StitchingArguments( args );
		if ( !stitchingArgs.parsedSuccessfully() )
			System.exit( 1 );

		final StitchingSpark st = new StitchingSpark( stitchingArgs );
		st.run();
	}

	private static final long serialVersionUID = 6006962943789087537L;

	private final StitchingArguments args;
	private StitchingJob job;
	private transient JavaSparkContext sparkContext;

	public StitchingSpark( final StitchingArguments args ) {
		this.args = args;
	}

	@Override
	public void run()
	{
		job = new StitchingJob( args );
		try {
			job.setTiles( TileInfoJSONProvider.loadTilesConfiguration( args.getInput() ) );
		} catch ( final Exception e ) {
			System.out.println( "Aborted: " + e.getMessage() );
			e.printStackTrace();
			System.exit( 2 );
		}

		sparkContext = new JavaSparkContext( new SparkConf().setAppName( "Stitching" ) );




		// --------------------------------------------------------------
		/*if ( job.getTiles().length == 1 )
		{
			final IntensityCorrection ic = new IntensityCorrection( job, sparkContext );
			try
			{
				ic.matchIntensities( job.getTiles()[ 0 ] );
			}
			catch ( final Exception e )
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			sparkContext.close();
			System.out.println( "done" );
			return;
		}*/
		// --------------------------------------------------------------



		// Query metadata
		final ArrayList< TileInfo > tilesWithoutMetadata = new ArrayList<>();
		for ( final TileInfo tile : job.getTiles() )
			if ( tile.getSize() == null || tile.getType() == null )
				tilesWithoutMetadata.add( tile );

		if ( !tilesWithoutMetadata.isEmpty() )
			queryMetadata( tilesWithoutMetadata );

		TileOperations.translateTilesToOrigin( job.getTiles() );

		if ( job.getMode() == Mode.Blur)
		{
			// Just perform smoothing and exit
			applyGaussianBlur();

			sparkContext.close();
			System.out.println( "done" );
			return;
		}

		if ( job.getMode() == Mode.Hdf5)
		{
			// Just create hdf5 file and exit
			createHdf5();

			sparkContext.close();
			System.out.println( "done" );
			return;
		}

		if ( job.getMode() != Mode.Metadata )
		{
			job.validateTiles();

			final StitchingParameters params = new StitchingParameters();
			params.dimensionality = job.getDimensionality();
			params.channel1 = 0;
			params.channel2 = 0;
			params.timeSelect = 0;
			params.checkPeaks = 250;
			params.computeOverlap = true;
			params.subpixelAccuracy = false;
			params.virtual = true;
			params.absoluteThreshold = 7;
			params.relativeThreshold = 5;
			job.setParams( params );

			// Positions correction
			if ( job.getMode() != Mode.FuseOnly )
			{
				final ArrayList< TilePair > overlappingTiles = TileOperations.findOverlappingTiles( job.getTiles() );
				System.out.println( "Overlapping pairs count = " + overlappingTiles.size() );

				final List< SerializablePairWiseStitchingResult > pairwiseShifts = preparePairwiseShifts( overlappingTiles );

				final double threshold = ( job.getFindOptimalThreshold() ? ThresholdEstimation.findOptimalThreshold( pairwiseShifts ) : job.getCrossCorrelationThreshold() );
				System.out.println( "Cross correlation threshold value: " + threshold + ( job.getFindOptimalThreshold() ? " (determined automatically)" : " (custom value)" ) );

				optimizeShifts( pairwiseShifts, threshold );
			}

			// Intensity correction
			if ( !args.getNoIntensityCorrection() )
			{
				final List< TilePair > overlappingShiftedTiles = TileOperations.findOverlappingTiles( job.getTiles() );
				final IntensityCorrection ic = new IntensityCorrection( job, sparkContext );
				try
				{
					ic.matchIntensities( overlappingShiftedTiles );
				}
				catch ( final Exception e )
				{
					System.out.println( "Something went wrong during intensity correction:" );
					e.printStackTrace();
					return;
				}
			}

			// Fusion
			if ( job.getMode() != Mode.NoFuse ) {

				final Boundaries boundaries = TileOperations.getCollectionBoundaries( job.getTiles() );
				final ArrayList< TileInfo > subregions = TileOperations.divideSpaceBySize( boundaries, job.getSubregionSize() );
				fuse( subregions );
			}
		}

		sparkContext.close();
		System.out.println( "done" );
	}

	/**
	 * Queries metadata (image type and dimensions) for each tile using Spark cluster.
	 */
	private void queryMetadata( final ArrayList< TileInfo > tilesWithoutMetadata )
	{
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( tilesWithoutMetadata );
		final JavaRDD< TileInfo > task = rdd.map(
				new Function< TileInfo, TileInfo >()
				{
					private static final long serialVersionUID = -4991255417353136684L;

					@Override
					public TileInfo call( final TileInfo tile ) throws Exception
					{
						final ImageCollectionElement el = Utils.createElement( job, tile );
						final ImagePlus imp = el.open( true );

						// FIXME: workaround for misinterpreting slices as timepoints when no metadata is present
						long[] size = Conversions.toLongArray( el.getDimensions() );
						if ( size.length == 2 && imp.getNFrames() > 1 )
							size = new long[] { size[ 0 ], size[ 1 ], imp.getNFrames() };

						tile.setType( ImageType.valueOf( imp.getType() ) );
						tile.setSize( size );

						el.close();
						return tile;
					}
				});

		final List< TileInfo > tilesMetadata = task.collect();

		System.out.println( "Obtained metadata for all tiles" );

		final TileInfo[] tiles = job.getTiles();
		final Map< Integer, TileInfo > tilesMap = job.getTilesMap();
		for ( final TileInfo tileMetadata : tilesMetadata ) {
			final TileInfo tile = tilesMap.get( tileMetadata.getIndex() );
			tile.setType( tileMetadata.getType() );
			tile.setSize( tileMetadata.getSize() );
		}

		try {
			TileInfoJSONProvider.saveTilesConfiguration( tiles, job.getBaseFolder() + "/" + Utils.addFilenameSuffix( job.getDatasetName() + ".json", "_full" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}

	/**
	 * Tries to load precalculated pairwise shifts from disk to save computational time.
	 */
	private List< SerializablePairWiseStitchingResult > preparePairwiseShifts( final ArrayList< TilePair > overlappingTiles )
	{
		// Try to load precalculated shifts for some pairs of tiles
		final String pairwiseResultsFile = Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args.getInput(), "_full" ), "_pairwise" );
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = new ArrayList<>();
		try {
			System.out.println( "try to load pairwise results from disk" );
			pairwiseShifts.addAll( TileInfoJSONProvider.loadPairwiseShifts( pairwiseResultsFile ) );
		} catch ( final FileNotFoundException e ) {
			System.out.println( "Pairwise results file not found" );
		} catch ( final NullPointerException e ) {
			System.out.println( "Pairwise results file is malformed" );
		}  catch ( final IOException e ) {
			e.printStackTrace();
		}

		// Create a cache to efficiently lookup the existing pairs of tiles loaded from disk
		final TreeMap< Integer, TreeSet< Integer > > cache = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult result : pairwiseShifts ) {
			final int firstIndex  =  Math.min( result.getTilePair().first().getIndex(), result.getTilePair().second().getIndex() ),
					secondIndex =  Math.max( result.getTilePair().first().getIndex(), result.getTilePair().second().getIndex() );
			if ( !cache.containsKey( firstIndex ) )
				cache.put( firstIndex, new TreeSet< Integer >() );
			cache.get( firstIndex ).add( secondIndex );
		}

		// Remove pending pairs of tiles which were already processed
		for ( final Iterator< TilePair > it = overlappingTiles.iterator(); it.hasNext(); )
		{
			final TilePair pair = it.next();
			final int firstIndex  =  Math.min( pair.first().getIndex(), pair.second().getIndex() ),
					secondIndex =  Math.max( pair.first().getIndex(), pair.second().getIndex() );
			if ( cache.containsKey( firstIndex ) && cache.get( firstIndex ).contains( secondIndex ) )
				it.remove();
		}

		if ( overlappingTiles.isEmpty() && !pairwiseShifts.isEmpty() )
		{
			// If we're able to load precalculated pairwise results, save some time skipping this step and jump to the global optimization
			System.out.println( "Successfully loaded all pairwise results from disk!" );
		}
		else
		{
			pairwiseShifts.addAll( computePairwiseShifts( overlappingTiles ) );

			try {
				System.out.println( "Stitched all tiles pairwise, store this information on disk.." );
				TileInfoJSONProvider.savePairwiseShifts( pairwiseShifts, pairwiseResultsFile );
			} catch ( final IOException e ) {
				e.printStackTrace();
			}
		}

		return pairwiseShifts;
	}

	/**
	 * Computes the best possible pairwise shifts between every pair of tiles on a Spark cluster.
	 * It uses phase correlation for measuring similarity between two images.
	 */
	private List< SerializablePairWiseStitchingResult > computePairwiseShifts( final ArrayList< TilePair > overlappingTiles )
	{
		if ( !args.getNoRoi() )
			System.out.println( "*** Use ROIs as usual ***" );
		else
			System.out.println( "*** Compute phase correlation between full tile images instead of their ROIs ***" );

		final JavaRDD< TilePair > rdd = sparkContext.parallelize( overlappingTiles );
		final JavaRDD< SerializablePairWiseStitchingResult > pairwiseStitching = rdd.map(
				new Function< TilePair, SerializablePairWiseStitchingResult >()
				{
					private static final long serialVersionUID = -2907426581991906327L;

					@Override
					public SerializablePairWiseStitchingResult call( final TilePair pairOfTiles ) throws Exception
					{
						final ImageCollectionElement el1 = Utils.createElement( job, pairOfTiles.first() );
						final ImageCollectionElement el2 = Utils.createElement( job, pairOfTiles.second() );

						final int timepoint = 1;
						final ComparePair pair = new ComparePair(
								new ImagePlusTimePoint( IJ.openImage( Utils.getAbsoluteImagePath( job, pairOfTiles.first() ) ), el1.getIndex(), timepoint, el1.getModel(), el1 ),
								new ImagePlusTimePoint( IJ.openImage( Utils.getAbsoluteImagePath( job, pairOfTiles.second() ) ), el2.getIndex(), timepoint, el2.getModel(), el2 ) );

						Roi roi1 = null, roi2 = null;
						if ( !args.getNoRoi() )
						{
							final Boundaries overlap1 = TileOperations.getOverlappingRegion( pairOfTiles.first(), pairOfTiles.second() );
							final Boundaries overlap2 = TileOperations.getOverlappingRegion( pairOfTiles.second(), pairOfTiles.first() );

							// mpicbg accepts only 2d rectangular ROIs
							roi1 = new Roi( overlap1.min( 0 ), overlap1.min( 1 ), overlap1.dimension( 0 ), overlap1.dimension( 1 ) );
							roi2 = new Roi( overlap2.min( 0 ), overlap2.min( 1 ), overlap2.dimension( 0 ), overlap2.dimension( 1 ) );
						}

						final double[] initialOffset = new double[ job.getDimensionality() ];
						for ( int d = 0; d < initialOffset.length; d++ )
							initialOffset[ d ] = pairOfTiles.second().getPosition( d ) - pairOfTiles.first().getPosition( d );

						final PairWiseStitchingResult result = PairWiseStitchingImgLib.stitchPairwise(
								pair.getImagePlus1(), pair.getImagePlus2(), roi1, roi2, pair.getTimePoint1(), pair.getTimePoint2(), job.getParams(), initialOffset, pairOfTiles.first().getSize() );

						System.out.println( "Stitched tiles " + pairOfTiles.first().getIndex() + " and " + pairOfTiles.second().getIndex() + System.lineSeparator() +
								"   CrossCorr=" + result.getCrossCorrelation() + ", PhaseCorr=" + result.getPhaseCorrelation() + ", RelShift=" + Arrays.toString( result.getOffset() ) );

						pair.getImagePlus1().close();
						pair.getImagePlus2().close();

						return new SerializablePairWiseStitchingResult( pairOfTiles, result );
					}
				});
		return pairwiseStitching.collect();
	}

	/**
	 * Finds final tile positions by globally optimizing pairwise shifts and stores updated tile configuration on the disk
	 */
	private void optimizeShifts( final List< SerializablePairWiseStitchingResult > pairwiseShifts, final double threshold )
	{
		job.getParams().regThreshold = threshold;

		// Create fake tile objects so that they don't hold any image data
		// required by the GlobalOptimization
		final TreeMap< Integer, Tile< ? > > fakeTileImagesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult pair : pairwiseShifts ) {
			for ( final TileInfo tileInfo : pair.getTilePair().toArray() ) {
				if ( !fakeTileImagesMap.containsKey( tileInfo.getIndex() ) ) {
					try {
						final ImageCollectionElement el = Utils.createElement( job, tileInfo );
						final ImagePlus fakeImage = new ImagePlus( tileInfo.getIndex().toString(), (java.awt.Image)null );
						final Tile< ? > fakeTile = new ImagePlusTimePoint( fakeImage, el.getIndex(), 1, el.getModel(), el );
						fakeTileImagesMap.put( tileInfo.getIndex(), fakeTile );
					} catch ( final Exception e ) {
						e.printStackTrace();
					}
				}
			}
		}

		final Vector< ComparePair > comparePairs = new Vector<>();
		for ( final SerializablePairWiseStitchingResult pair : pairwiseShifts ) {
			final ComparePair comparePair = new ComparePair(
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().first().getIndex() ),
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().second().getIndex() ) );

			comparePair.setRelativeShift( pair.getOffset() );
			comparePair.setCrossCorrelation( pair.getCrossCorrelation() );

			comparePairs.addElement( comparePair );
		}

		System.out.println( "Perform global optimization" );
		final ArrayList< ImagePlusTimePoint > optimized = new ArrayList<>();
		if ( !comparePairs.isEmpty() )
			optimized.addAll( GlobalOptimization.optimize( comparePairs, comparePairs.get( 0 ).getTile1(), job.getParams() ) );

		System.out.println( "Global optimization done" );
		for ( final ImagePlusTimePoint imt : optimized )
			Log.info( imt.getImpId() + ": " + imt.getModel() );

		// Process the result updating the tiles position within the job object
		final TileInfo[] tiles = job.getTiles();
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tilesMap.put( tile.getIndex(), tile );

		for ( final ImagePlusTimePoint optimizedTile : optimized ) {
			final double[] pos = new double[ job.getDimensionality() ];
			optimizedTile.getModel().applyInPlace( pos );
			tilesMap.get( optimizedTile.getImpId() ).setPosition( pos );
		}

		try {
			TileInfoJSONProvider.saveTilesConfiguration( tiles, Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args.getInput(), "_full" ), "_shifted" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}

	/**
	 * Fuses tile images within a set of subregions (whole space divided into cells) on a Spark cluster.
	 */
	private void fuse( final ArrayList< TileInfo > subregions )
	{
		System.out.println( "There are " + subregions.size() + " subregions in total" );

		final String fusedFolder = job.getBaseFolder() + "/fused";
		new File( fusedFolder ).mkdirs();

		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( subregions );
		final JavaRDD< TileInfo > fused = rdd.map(
				new Function< TileInfo, TileInfo >()
				{
					private static final long serialVersionUID = 8324712817942470416L;

					@Override
					public TileInfo call( final TileInfo subregion ) throws Exception
					{
						final ArrayList< TileInfo > tilesWithinSubregion = TileOperations.findTilesWithinSubregion( job.getTiles(), subregion );
						if ( tilesWithinSubregion.isEmpty() )
							return null;

						subregion.setFile( fusedFolder + "/" + job.getDatasetName() + "_tile" + subregion.getIndex() + ".tif" );
						System.out.println( "Starting to fuse tiles within subregion " + subregion.getIndex() );

						final FusionPerformer fusion = new FusionPerformer( job );
						fusion.fuseTilesWithinSubregion( tilesWithinSubregion, subregion );

						System.out.println( "Completed for subregion " + subregion.getIndex() );

						return subregion;
					}
				});

		final ArrayList< TileInfo > output = new ArrayList<>( fused.collect() );
		output.removeAll( Collections.singleton( null ) );
		System.out.println( "Obtained " + output.size() + " tiles" );

		try
		{
			final TileInfo[] newTiles = output.toArray( new TileInfo[ 0 ] );
			job.setTiles( newTiles );
			TileInfoJSONProvider.saveTilesConfiguration( newTiles, Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args.getInput(), "_full" ), "_fused" ) );
		}
		catch ( final Exception e )
		{
			e.printStackTrace();
		}
	}

	/**
	 * Applies gaussian blur to the input tile images and stores them separately on the disk
	 */
	private void applyGaussianBlur()
	{
		final String blurredFolder = job.getBaseFolder() + "/blurred";
		new File( blurredFolder ).mkdirs();

		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( job.getTiles() ) );
		final JavaRDD< TileInfo > task = rdd.map(
				new Function< TileInfo, TileInfo >()
				{
					private static final long serialVersionUID = 772321548054114279L;

					@Override
					public TileInfo call( final TileInfo tile ) throws Exception
					{
						System.out.println( "Blurring tile " + tile.getIndex() );
						final ImagePlus imp = IJ.openImage( tile.getFile() );
						final GaussianBlur blur = new GaussianBlur();
						blur.setup( "", imp );
						for ( int slice = 1; slice <= imp.getNSlices(); slice++ ) {
							imp.setSlice( slice );
							//blur.run( imp.getProcessor() );
							blur.blurGaussian( imp.getProcessor(), 3.5 );
						}

						System.out.println( "Saving blurred tile " + tile.getIndex() );
						final TileInfo blurredTile = tile.clone();
						blurredTile.setFile( blurredFolder + "/" + Utils.addFilenameSuffix( new File( tile.getFile() ).getName(), "_blurred" ) );
						IJ.saveAsTiff( imp, blurredTile.getFile() );
						return blurredTile;
					}
				});

		final List< TileInfo > blurredTiles = task.collect();

		System.out.println( "Obtained blurred tiles" );

		final TileInfo[] tiles = job.getTiles();
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tilesMap.put( tile.getIndex(), tile );

		for ( final TileInfo blurredTile : blurredTiles ) {
			final TileInfo tile = tilesMap.get( blurredTile.getIndex() );
			tile.setFile( blurredTile.getFile() );
		}

		try {
			TileInfoJSONProvider.saveTilesConfiguration( tiles, Utils.addFilenameSuffix( Utils.removeFilenameSuffix( args.getInput(), "_full" ), "_blurred" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates HDF5 dataset consisting of tile images. May be useful after fusion for reducing the overall size and number of the output files.
	 */
	private void createHdf5()
	{
		final String fusedFolder = job.getBaseFolder() + "/fused";
		new File( fusedFolder ).mkdirs();
		final String hdf5 = fusedFolder + "/" + job.getDatasetName() + ".hdf5";
		Hdf5Creator.createHdf5( job.getTiles(), hdf5 );
	}
}
