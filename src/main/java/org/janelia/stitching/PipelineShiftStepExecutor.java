package org.janelia.stitching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.janelia.stitching.analysis.ThresholdEstimation;

import ij.IJ;
import ij.ImagePlus;
import mpicbg.models.Tile;
import mpicbg.stitching.ComparePair;
import mpicbg.stitching.GlobalOptimization;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
import mpicbg.stitching.PairWiseStitchingImgLib;
import mpicbg.stitching.PairWiseStitchingResult;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.exception.ImgLibException;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

/**
 * Computes updated tile positions using phase correlation for pairwise matches and then global optimization for fitting all of them together.
 * Saves updated tile configuration on the disk.
 *
 * @author Igor Pisarev
 */

public class PipelineShiftStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -7152174064553332061L;

	public PipelineShiftStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run()
	{
		final ArrayList< TilePair > overlappingTiles = TileOperations.findOverlappingTiles( job.getTiles() );
		System.out.println( "Overlapping pairs count = " + overlappingTiles.size() );

		final List< SerializablePairWiseStitchingResult > pairwiseShifts = preparePairwiseShifts( overlappingTiles );

		double threshold = job.getArgs().crossCorrelationThreshold();
		if ( threshold < 0 )
			threshold = ThresholdEstimation.findOptimalThreshold( pairwiseShifts );
		System.out.println( "Cross correlation threshold value: " + threshold + ( job.getArgs().crossCorrelationThreshold() < 0 ? " (determined automatically)" : " (custom value)" ) );

		optimizeShifts( pairwiseShifts, threshold );
	}

	/**
	 * Tries to load precalculated pairwise shifts from disk to save computational time.
	 */
	private List< SerializablePairWiseStitchingResult > preparePairwiseShifts( final ArrayList< TilePair > overlappingTiles )
	{
		// Try to load precalculated shifts for some pairs of tiles
		final String pairwiseResultsFile = Utils.addFilenameSuffix( Utils.removeFilenameSuffix( job.getArgs().inputFilePath(), "_full" ), "_pairwise" );
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

		final List< SerializablePairWiseStitchingResult[] > pairwiseShiftsMulti = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : pairwiseShifts )
			pairwiseShiftsMulti.add( new SerializablePairWiseStitchingResult[] { shift } );

		if ( overlappingTiles.isEmpty() && !pairwiseShifts.isEmpty() )
		{
			// If we're able to load precalculated pairwise results, save some time skipping this step and jump to the global optimization
			System.out.println( "Successfully loaded all pairwise results from disk!" );
		}
		else
		{
			final List< SerializablePairWiseStitchingResult[] > adjacentShiftsMulti = computePairwiseShifts( overlappingTiles );
			pairwiseShiftsMulti.addAll( adjacentShiftsMulti );

			try {
				System.out.println( "Stitched all tiles pairwise, store this information on disk.." );
				TileInfoJSONProvider.savePairwiseShiftsMulti( pairwiseShiftsMulti, Utils.addFilenameSuffix( pairwiseResultsFile, "_multi" ) );
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
	private List< SerializablePairWiseStitchingResult[] > computePairwiseShifts( final ArrayList< TilePair > overlappingTiles )
	{
		/*if ( !job.getArgs().noRoi() )
			System.out.println( "*** Use ROIs as usual ***" );
		else
			System.out.println( "*** Compute phase correlation between full tile images instead of their ROIs ***" );*/


		// ignore first slice for the first channel
		final boolean ignoreFirstSlice = true;//job.getTilesMap().firstKey().equals( 0 );
		System.out.println( "ignoreFirstSlice = " + ignoreFirstSlice );


		System.out.println( "Processing " + overlappingTiles.size() + " pairs..." );

		final JavaRDD< TilePair > rdd = sparkContext.parallelize( overlappingTiles );
		final JavaRDD< SerializablePairWiseStitchingResult[] > pairwiseStitching = rdd.map(
				new Function< TilePair, SerializablePairWiseStitchingResult[] >()
				{
					private static final long serialVersionUID = -2907426581991906327L;

					@Override
					public SerializablePairWiseStitchingResult[] call( final TilePair pairOfTiles ) throws Exception
					{
						System.out.println( "Processing tiles " + pairOfTiles.first().getIndex() + " and " + pairOfTiles.second().getIndex() + "..." );

						final TileInfo[] pair = pairOfTiles.toArray();
						final Boundaries[] overlaps = new Boundaries[ pair.length ];
						final ImageCollectionElement[] elements = new ImageCollectionElement[ pair.length ];
						final ImagePlus[] imps = new ImagePlus[ pair.length ];

						final double blurSigma = job.getArgs().blurStrength();
						System.out.println( "Blur sigma set to " + blurSigma );

						for ( int j = 0; j < pair.length; j++ )
						{
							System.out.println( "Prepairing #" + (j+1) + " of a pair" );

							overlaps[ j ] = TileOperations.getOverlappingRegion( pair[ j ], pair[ ( j + 1 ) % pair.length ] );

							elements[ j ] = Utils.createElement( job, pair[ j ] );
							final ImagePlus imp = IJ.openImage( Utils.getAbsoluteImagePath( job, pair[ j ] ) );
							Utils.workaroundImagePlusNSlices( imp );

							RandomAccessibleInterval rai = ImagePlusImgs.from( imp );

							if ( ignoreFirstSlice )
							{
								System.out.println( "Chopping off the first slice.." );
								if( overlaps[ j ].min( 2 ) == 0 )
									overlaps[ j ].setMin( 2, 1 );

								final Boundaries ignoreFirstSliceInterval = new Boundaries( pair[ j ].getSize() );
								ignoreFirstSliceInterval.setMin( 2, 1 );
								rai = Views.interval( rai, ignoreFirstSliceInterval );
							}

							blur( rai, overlaps[ j ], new double[] { blurSigma, blurSigma, blurSigma * 80.0 / 150.0 } );

							imps[ j ] = crop( imp, overlaps[ j ] );
							imp.close();
						}

						System.out.println( "Stitching.." );
						final int timepoint = 1;
						final ComparePair comparePair = new ComparePair(
								new ImagePlusTimePoint( imps[ 0 ], elements[ 0 ].getIndex(), timepoint, elements[ 0 ].getModel(), elements[ 0 ] ),
								new ImagePlusTimePoint( imps[ 1 ], elements[ 1 ].getIndex(), timepoint, elements[ 1 ].getModel(), elements[ 1 ] ) );

						PairWiseStitchingImgLib.setThreads( 1 );
						final PairWiseStitchingResult[] result = PairWiseStitchingImgLib.stitchPairwise(
								comparePair.getImagePlus1(), comparePair.getImagePlus2(), null, null, null, null, comparePair.getTimePoint1(), comparePair.getTimePoint2(), job.getParams(), 5 );

						System.out.println( "Stitched tiles " + pairOfTiles.first().getIndex() + " and " + pairOfTiles.second().getIndex() + ", got " + result.length + " peaks" );

						for ( int i = 0; i < result.length; i++ )
							for ( int j = 0; j < pair.length; j++ )
								for ( int d = 0; d < job.getDimensionality(); d++ )
									result[ i ].getOffset()[ d ] += (j==0?1:-1) * overlaps[ j ].min( d );

						/*System.out.println( "*****" );
						System.out.println( "Original offset: " + Arrays.toString( new double[]{
								pair[1].getPosition( 0 ) - pair[0].getPosition( 0 ),
								pair[1].getPosition( 1 ) - pair[0].getPosition( 1 ),
								pair[1].getPosition( 2 ) - pair[0].getPosition( 2 ) } ) );
						System.out.println( "New offset: " + Arrays.toString( result[0].getOffset() ) );
						System.out.println( "*****" );*/

						for ( int j = 0; j < pair.length; j++ )
							imps[ j ].close();

						final SerializablePairWiseStitchingResult[] ret = new SerializablePairWiseStitchingResult[ result.length ];
						for ( int i = 0; i < ret.length; i++ )
							ret[ i ] = new SerializablePairWiseStitchingResult( pairOfTiles, result[ i ] );
						return ret;
					}
				});
		return pairwiseStitching.collect();
	}


	private static < T extends RealType< T > & NativeType< T > > ImagePlus crop( final ImagePlus inImp, final Boundaries region )
	{
		final T type = ( T ) ImageType.valueOf( inImp.getType() ).getType();

		final RandomAccessible< T > inImg = ImagePlusImgs.from( inImp );
		final ImagePlusImg< T, ? > outImg = new ImagePlusImgFactory< T >().create( region, type.createVariable() );

		final IterableInterval< T > inInterval = Views.flatIterable( Views.offsetInterval( inImg, region ) );
		final IterableInterval< T > outInterval = Views.flatIterable( outImg );

		final Cursor< T > inCursor = inInterval.cursor();
		final Cursor< T > outCursor = outInterval.cursor();

		while ( inCursor.hasNext() || outCursor.hasNext() )
			outCursor.next().set( inCursor.next() );


		ImagePlus impOut;
		try
		{
			impOut = outImg.getImagePlus();
		}
		catch ( final ImgLibException e )
		{
			impOut = ImageJFunctions.wrap( outImg, "" );
		}
		Utils.workaroundImagePlusNSlices( impOut );
		return impOut;
	}


	private static <T extends NumericType< T > >void blur(
			final RandomAccessibleInterval< T > image,
			final Interval interval,
			final double[] sigmas ) throws IncompatibleTypeException
	{
		final RandomAccessible< T > extendedImage = Views.extendMirrorSingle( image );
		final RandomAccessibleInterval< T > crop = Views.interval( extendedImage, interval );
		final ExecutorService service = Executors.newFixedThreadPool( 1 );
		Gauss3.gauss( sigmas, extendedImage, crop, service );
		service.shutdown();
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

		int validPairs = 0;
		final Vector< ComparePair > comparePairs = new Vector<>();
		for ( final SerializablePairWiseStitchingResult pair : pairwiseShifts ) {
			final ComparePair comparePair = new ComparePair(
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().first().getIndex() ),
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().second().getIndex() ) );

			comparePair.setRelativeShift( pair.getOffset() );
			comparePair.setCrossCorrelation( pair.getCrossCorrelation() );
			comparePair.setIsValidOverlap( pair.getIsValidOverlap() );

			comparePairs.addElement( comparePair );

			if ( pair.getIsValidOverlap() )
				validPairs++;
		}
		System.out.println( "There are " + validPairs + " valid pairs" );





		final boolean loadAnotherChannel = true;

		final Map< Integer, Map< Integer, ComparePair > > anotherChannel = new TreeMap<>();
		final Map< Integer, TileInfo > anotherChannelTiles = new TreeMap<>();
		List< SerializablePairWiseStitchingResult > anotherChannelShifts;

		if ( loadAnotherChannel )
		{
			try
			{
				anotherChannelShifts = TileInfoJSONProvider.loadPairwiseShifts( "/nobackup/saalfeld/igor/Sample9/fix-blur/Scan1/optimization/Scan2-merged_pairwise_shifts_set.json" );
				//anotherChannelShifts = TileInfoJSONProvider.loadPairwiseShifts( "/nobackup/saalfeld/igor/Yoshi_Flybrain/fix-blur/decon/ch0/optimization/both_channels_together/ch1-merged_pairwise_shifts_set.json" );
			}
			catch ( final IOException e1 )
			{
				e1.printStackTrace();
				return;
			}

			if ( anotherChannelShifts != null )
				anotherChannelTiles.putAll( Utils.createTilesMap( anotherChannelShifts, false ) );
			final int tilesPerChannel = anotherChannelTiles.size();
			System.out.println( "Tiles per channel=" + tilesPerChannel );

			final TreeMap< Integer, Tile< ? > > anotherChannelFakeTileImagesMap = new TreeMap<>();
			for ( final SerializablePairWiseStitchingResult pair : anotherChannelShifts ) {
				for ( final TileInfo tileInfo : pair.getTilePair().toArray() ) {
					if ( !anotherChannelFakeTileImagesMap.containsKey( tileInfo.getIndex() ) ) {
						try {
							final ImageCollectionElement el = Utils.createElement( job, tileInfo );
							final ImagePlus fakeImage = new ImagePlus( tileInfo.getIndex().toString(), (java.awt.Image)null );
							final Tile< ? > fakeTile = new ImagePlusTimePoint( fakeImage, el.getIndex(), 1, el.getModel(), el );
							anotherChannelFakeTileImagesMap.put( tileInfo.getIndex(), fakeTile );
						} catch ( final Exception e ) {
							e.printStackTrace();
						}
					}
				}
			}
			int additionalMatches = 0;
			for ( final SerializablePairWiseStitchingResult shift : anotherChannelShifts )
			{
				final int ind1 = Math.min( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() ) - tilesPerChannel;
				final int ind2 = Math.max( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() ) - tilesPerChannel;

				if ( !anotherChannel.containsKey( ind1 ) )
					anotherChannel.put( ind1, new TreeMap<>() );


				final ComparePair comparePair = new ComparePair(
						(ImagePlusTimePoint)anotherChannelFakeTileImagesMap.get( shift.getTilePair().first().getIndex() ),
						(ImagePlusTimePoint)anotherChannelFakeTileImagesMap.get( shift.getTilePair().second().getIndex() ) );

				comparePair.setRelativeShift( shift.getOffset() );
				comparePair.setCrossCorrelation( shift.getCrossCorrelation() );
				comparePair.setIsValidOverlap( shift.getIsValidOverlap() );

				anotherChannel.get( ind1 ).put( ind2, comparePair );
				additionalMatches++;
			}
		}


		final long lastTime = System.nanoTime();
		System.out.println( "Perform global optimization" );
		final ArrayList< ImagePlusTimePoint > optimized = new ArrayList<>();
		if ( !comparePairs.isEmpty() )
			optimized.addAll( GlobalOptimization.optimize( comparePairs, comparePairs.get( 0 ).getTile1(), job.getParams(), loadAnotherChannel ? anotherChannel : null ) );

		System.out.println( "Global optimization done" );
		//for ( final ImagePlusTimePoint imt : optimized )
		//	Log.info( imt.getImpId() + ": " + imt.getModel() );

		System.out.println( "****" );
		System.out.println( "Global optimization took " + ((System.nanoTime() - lastTime)/Math.pow( 10, 9 )) +"s" );

		// Update tile positions
		final Map< Integer, TileInfo > tilesMap = job.getTilesMap();
		for ( final ImagePlusTimePoint optimizedTile : optimized )
		{
			final double[] pos = new double[ job.getDimensionality() ];
			optimizedTile.getModel().applyInPlace( pos );

			if ( tilesMap.containsKey( optimizedTile.getImpId() ) )
				tilesMap.get( optimizedTile.getImpId() ).setPosition( pos );
			else
				anotherChannelTiles.get( optimizedTile.getImpId() ).setPosition( pos );
		}

		for ( final Tile<?> lostTile : GlobalOptimization.lostTiles )
		{
			final int lostTileIndex = ((ImagePlusTimePoint)lostTile).getImpId();
			tilesMap.remove( lostTileIndex );
			anotherChannelTiles.remove( lostTileIndex );
		}

		System.out.println( "Channel #0 tiles: " + tilesMap.size() );
		System.out.println( "Channel #1 tiles: " + anotherChannelTiles.size() );

		try {
			TileInfoJSONProvider.saveTilesConfiguration( tilesMap.values().toArray( new TileInfo[ 0 ] ), "ch0-final.json" );

			if ( !anotherChannelTiles.isEmpty() )
				TileInfoJSONProvider.saveTilesConfiguration( anotherChannelTiles.values().toArray( new TileInfo[ 0 ] ), "ch1-final.json" );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}
}
