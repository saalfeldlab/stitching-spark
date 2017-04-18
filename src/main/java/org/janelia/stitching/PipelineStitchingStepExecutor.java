package org.janelia.stitching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.flatfield.FlatfieldCorrectedRandomAccessible;
import org.janelia.flatfield.FlatfieldCorrection;
import org.janelia.stitching.analysis.CheckConnectedGraphs;
import org.janelia.stitching.analysis.FilterAdjacentShifts;
import org.janelia.stitching.analysis.ThresholdEstimation;
import org.janelia.util.ImageImporter;
import org.janelia.util.concurrent.SameThreadExecutorService;

import ij.ImagePlus;
import mpicbg.models.Tile;
import mpicbg.stitching.ComparePair;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
import net.imglib2.Cursor;
import net.imglib2.FinalDimensions;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealFloatConverter;
import net.imglib2.exception.IncompatibleTypeException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

/**
 * Computes updated tile positions using phase correlation for pairwise matches and then global optimization for fitting all of them together.
 * Saves updated tile configuration on the disk.
 *
 * @author Igor Pisarev
 */

public class PipelineStitchingStepExecutor extends PipelineStepExecutor
{
	private static final long serialVersionUID = -7152174064553332061L;

	public PipelineStitchingStepExecutor( final StitchingJob job, final JavaSparkContext sparkContext )
	{
		super( job, sparkContext );
	}

	@Override
	public void run() throws PipelineExecutionException
	{
		// Scale positions to the same range as size (make them independent of voxel dimensions)
		// TODO: inconvenient, better require the input configuration file to be already rescaled with respect to voxel dimensions
//		final VoxelDimensions voxelDimensions = job.getArgs().voxelDimensions();
//		for ( final TileInfo tile : job.getTiles() )
//			for ( int d = 0; d < tile.numDimensions(); d++ )
//				tile.setPosition( d, tile.getPosition( d ) / voxelDimensions.dimension( d ) );

		final List< TilePair > overlappingTiles = TileOperations.findOverlappingTiles( job.getTiles( 0 ) );
		System.out.println( "Overlapping pairs count = " + overlappingTiles.size() );

		// Remove pairs with small overlap area if only adjacent pairs are requested
		if ( !job.getArgs().useAllPairs() )
		{
			final List< TilePair > adjacentOverlappingTiles = FilterAdjacentShifts.filterAdjacentPairs( overlappingTiles );
			overlappingTiles.clear();
			overlappingTiles.addAll( adjacentOverlappingTiles );
			System.out.println( "Retaining only " + overlappingTiles.size() + " adjacent pairs of them" );
		}

		// NOTE: may fail with StackOverflowError. Pass -Xss to the JVM
		final List< Integer > connectedComponentsSize = CheckConnectedGraphs.connectedComponentsSize( overlappingTiles );
		if ( connectedComponentsSize.size() > 1 )
			throw new PipelineExecutionException( "Overlapping pairs form more than one connected component: " + connectedComponentsSize );

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
	private List< SerializablePairWiseStitchingResult > preparePairwiseShifts( final List< TilePair > overlappingTiles )
	{
		// Try to load precalculated shifts for some pairs of tiles
		final String pairwiseResultsFile = Utils.addFilenameSuffix( Utils.removeFilenameSuffix( job.getArgs().inputTileConfigurations().get( 0 ), "_full" ), "_pairwise" );
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
			final int firstIndex =  Math.min( result.getTilePair().getA().getIndex(), result.getTilePair().getB().getIndex() ),
					secondIndex  =  Math.max( result.getTilePair().getA().getIndex(), result.getTilePair().getB().getIndex() );
			if ( !cache.containsKey( firstIndex ) )
				cache.put( firstIndex, new TreeSet< Integer >() );
			cache.get( firstIndex ).add( secondIndex );
		}

		// Remove pending pairs of tiles which were already processed
		for ( final Iterator< TilePair > it = overlappingTiles.iterator(); it.hasNext(); )
		{
			final TilePair pair = it.next();
			final int firstIndex =  Math.min( pair.getA().getIndex(), pair.getB().getIndex() ),
					secondIndex =  Math.max( pair.getA().getIndex(), pair.getB().getIndex() );
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
			// Initiate the computation
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
	private < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > List< SerializablePairWiseStitchingResult[] > computePairwiseShifts( final List< TilePair > overlappingTiles )
	{
		/*if ( !job.getArgs().noRoi() )
			System.out.println( "*** Use ROIs as usual ***" );
		else
			System.out.println( "*** Compute phase correlation between full tile images instead of their ROIs ***" );*/


		// ignore first slice for the first channel
		final boolean ignoreFirstSlice = false;//job.getTilesMap().firstKey().equals( 0 );
		System.out.println( "ignoreFirstSlice = " + ignoreFirstSlice );


		System.out.println( "Broadcasting flatfield correction images" );
		final List< RandomAccessiblePairNullable< U, U > > flatfieldCorrectionForChannels = new ArrayList<>();
		for ( final String channelTileConfiguration : job.getArgs().inputTileConfigurations() )
		{
			flatfieldCorrectionForChannels.add(
					FlatfieldCorrection.loadCorrectionImages(
							Paths.get( channelTileConfiguration ).getParent().toString() + "/v.tif",
							Paths.get( channelTileConfiguration ).getParent().toString() + "/z.tif"
						)
				);
		}
		final Broadcast< List< RandomAccessiblePairNullable< U, U > > > broadcastedFlatfieldCorrectionForChannels = sparkContext.broadcast( flatfieldCorrectionForChannels );

		// extract padding from arguments which should be applied to the overlap area
		final long[] overlapPadding = job.getArgs().padding();

		System.out.println( "Processing " + overlappingTiles.size() + " pairs..." );

		final JavaRDD< TilePair > rdd = sparkContext.parallelize( overlappingTiles );
		final JavaRDD< SerializablePairWiseStitchingResult[] > pairwiseStitching = rdd.map( pairOfTiles ->
			{
				System.out.println( "Processing tile pair " + pairOfTiles );

				final TileInfo[] pair = pairOfTiles.toArray();
				final Boundaries[] overlaps = new Boundaries[ pair.length ];
				final ImagePlus[] imps = new ImagePlus[ pair.length ];

				final double[] normalizedVoxelDimensions = Utils.normalizeVoxelDimensions( job.getArgs().voxelDimensions() );
				System.out.println( "Normalized voxel size = " + Arrays.toString( normalizedVoxelDimensions ) );
				final double blurSigma = job.getArgs().blurSigma();
				final double[] blurSigmas = new  double[ normalizedVoxelDimensions.length ];
				for ( int d = 0; d < blurSigmas.length; d++ )
					blurSigmas[ d ] = blurSigma / normalizedVoxelDimensions[ d ];

				for ( int j = 0; j < pair.length; j++ )
				{
					System.out.println( "Prepairing #" + (j+1) + " of a pair,  padding ROI by " + Arrays.toString( overlapPadding ) );

					overlaps[ j ] = TileOperations.padInterval(
							TileOperations.getOverlappingRegion( pair[ j ], pair[ ( j + 1 ) % pair.length ] ),
							new FinalDimensions( pair[ j ].getSize() ),
							overlapPadding
						);

					// Check if overlap area exists as a separate file
					/*final String overlapImagePath = String.format( "%s_overlaps/%d(%d).tif", job.getArgs().inputFilePath(), pair[ j ].getIndex(), pair[ ( j + 1 ) % pair.length ].getIndex() );
					if ( Files.exists( Paths.get( overlapImagePath ) ) )
					{
						System.out.println( String.format( "Found an exported overlap area image on disk for pair %d(%d)", pair[ j ].getIndex(), pair[ ( j + 1 ) % pair.length ].getIndex() ) );
						imps[ j ] = ImageImporter.openImage( overlapImagePath );
						Utils.workaroundImagePlusNSlices( imps[ j ] );

						final int[] overlapImgDimensions = Utils.getImagePlusDimensions( imps[ j ] );
						for ( int d = 0; d < Math.min( overlaps[ j ].numDimensions(), overlapImgDimensions.length ); d++ )
							if ( overlaps[ j ].numDimensions() != overlapImgDimensions.length || overlaps[ j ].dimension( d ) != overlapImgDimensions[ d ] )
								throw new PipelineExecutionException( String.format( "Tile pair %s: exported overlap area image %s and actual overlap area %s have different size", pairOfTiles, Arrays.toString( overlapImgDimensions ), Arrays.toString( overlaps[ j ].getDimensions() ) ) );
					}
					else*/
					{
						//RandomAccessibleInterval rai = ImagePlusImgs.from( imp );
						//RandomAccessibleInterval rai = VirtualStackImageLoader.createUnsignedShortInstance( imp ).getSetupImgLoader( 0 ).getImage( 1 );
						/*RandomAccessibleInterval rai = null;

						if ( ignoreFirstSlice )
						{
							System.out.println( "Chopping off the first slice.." );
							if( overlaps[ j ].min( 2 ) == 0 )
								overlaps[ j ].setMin( 2, 1 );

							final Boundaries ignoreFirstSliceInterval = new Boundaries( pair[ j ].getSize() );
							ignoreFirstSliceInterval.setMin( 2, 1 );
							rai = Views.interval( rai, ignoreFirstSliceInterval );
						}*/

						/*if ( blurSigma > 0 )
						{
							System.out.println( String.format( "Blurring region at %s of size %s with sigmas=%s (s=%f)",
									Arrays.toString( overlaps[ j ].getMin() ),
									Arrays.toString( overlaps[ j ].getDimensions() ),
									Arrays.toString( blurSigmas ), blurSigma ) );
							blur( rai, overlaps[ j ], blurSigmas );
						}*/

						// TODO: use virtual loader if imp is a virtual ImagePlus
						//imps[ j ] = cropVirtualAveragingChannels( imp, overlaps[ j ] );
//								imps[ j ] = crop( imp, overlaps[ j ] );
//								imp.close();


						System.out.println( "Averaging corresponding tile images for " + job.getChannels() + " channels" );
						final ImagePlusImg< FloatType, ? > dst = ImagePlusImgs.floats( Intervals.dimensionsAsLongArray( overlaps[ j ] ) );
						for ( int channel = 0; channel < job.getChannels(); channel++ )
						{
							final TileInfo tileInfo = Utils.createTilesMap( job.getTiles( channel ) ).get( pair[ j ].getIndex() );
							final ImagePlus imp = ImageImporter.openImage( tileInfo.getFilePath() );
							Utils.workaroundImagePlusNSlices( imp );

							final RandomAccessibleInterval< T > img = ImagePlusImgs.from( imp );
							final RandomAccessibleInterval< T > imgCrop = Views.interval( img, overlaps[ j ] );

							final RandomAccessibleInterval< FloatType > sourceInterval;
							final RandomAccessiblePairNullable< U, U > flatfield = broadcastedFlatfieldCorrectionForChannels.value().get( channel );
							if ( flatfield != null )
							{
								final FlatfieldCorrectedRandomAccessible< T, U > flatfieldCorrected = new FlatfieldCorrectedRandomAccessible<>( imgCrop, flatfield.toRandomAccessiblePair() );
								final RandomAccessibleInterval< U > correctedImg = Views.interval( flatfieldCorrected, imgCrop );
								sourceInterval = Converters.convert( correctedImg, new RealFloatConverter<>(), new FloatType() );
							}
							else
							{
								sourceInterval = Converters.convert( imgCrop, new RealFloatConverter<>(), new FloatType() );
							}

							final Cursor< FloatType > srcCursor = Views.flatIterable( sourceInterval ).cursor();
							final Cursor< FloatType > dstCursor = Views.flatIterable( dst ).cursor();
							while ( dstCursor.hasNext() || srcCursor.hasNext() )
								dstCursor.next().add( srcCursor.next() );

							imp.close();
						}

						final FloatType denom = new FloatType( job.getChannels() );
						final Cursor< FloatType > dstCursor = Views.iterable( dst ).cursor();
						while ( dstCursor.hasNext() )
							dstCursor.next().div( denom );

						if ( blurSigma > 0 )
						{
							System.out.println( String.format( "Blurring the overlap area of size %s with sigmas=%s (s=%f)", Arrays.toString( Intervals.dimensionsAsLongArray( dst ) ), Arrays.toString( blurSigmas ), blurSigma ) );
							blur( dst, blurSigmas );
						}

						imps[ j ] = dst.getImagePlus();
						Utils.workaroundImagePlusNSlices( imps[ j ] );
					}
				}

				System.out.println( "Stitching.." );

				final int timepoint = 1;
				PairwiseStitchingPerformer.setThreads( 1 ); // TODO: determine automatically based on parallelism / smth else
				final SerializablePairWiseStitchingResult[] result = PairwiseStitchingPerformer.stitchPairwise(
						imps[0], imps[1], null, null, null, null, timepoint, timepoint, job.getParams(), 1 );

				System.out.println( "Stitched tile pair " + pairOfTiles + ", got " + result.length + " peaks" );

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

				for ( int i = 0; i < result.length; i++ )
					result[ i ].setTilePair( pairOfTiles );
				return result;
			} );

		final List< SerializablePairWiseStitchingResult[] > stitchingResults = pairwiseStitching.collect();
		broadcastedFlatfieldCorrectionForChannels.destroy();
		return stitchingResults;
	}


//	private static < T extends RealType< T > & NativeType< T > > ImagePlus crop( final ImagePlus inImp, final Boundaries region )
//	{
//		final T type = ( T ) ImageType.valueOf( inImp.getType() ).getType();
//
//		final RandomAccessible< T > inImg = ImagePlusImgs.from( inImp );
//		final ImagePlusImg< T, ? > outImg = new ImagePlusImgFactory< T >().create( region, type.createVariable() );
//
//		final IterableInterval< T > inInterval = Views.flatIterable( Views.offsetInterval( inImg, region ) );
//		final IterableInterval< T > outInterval = Views.flatIterable( outImg );
//
//		final Cursor< T > inCursor = inInterval.cursor();
//		final Cursor< T > outCursor = outInterval.cursor();
//
//		while ( inCursor.hasNext() || outCursor.hasNext() )
//			outCursor.next().set( inCursor.next() );
//
//
//		final ImagePlus impOut = outImg.getImagePlus();
//		Utils.workaroundImagePlusNSlices( impOut );
//		return impOut;
//	}

	// Based on slices + raw pixels
	/*private static < T extends RealType< T > & NativeType< T > > ImagePlus cropVirtualAveragingChannels( final ImagePlus imp, final Boundaries region )
	{
		final Interval regionSlice = new FinalInterval( new long[] { region.min(0), region.min(1) }, new long[] { region.max(0), region.max(1) } );

		final T type = ( T ) ImageType.valueOf( imp.getType() ).getType();
		final ImagePlusImg< T, ? > outImg = new ImagePlusImgFactory< T >().create( region, type.createVariable() );

		final int channels = imp.getNChannels();
		final int frame = 0;
		for ( int z = 0; z < imp.getNSlices(); z++ )
		{
			final Img< T >[] data = new Img[ channels ];
			final Cursor< T >[] cursors = new Cursor[ channels ];
			for ( int ch = 0; ch < channels; ch++ )
			{
				data[ ch ] = ( Img ) ArrayImgs.unsignedShorts( ( short[] ) imp.getStack().getProcessor( imp.getStackIndex( ch + 1, z + 1, frame + 1 ) ).getPixels(), new long[] { imp.getWidth(), imp.getHeight() } );
				cursors[ ch ] = Views.flatIterable( Views.offsetInterval( data[ ch ], regionSlice ) ).cursor();
			}

			final IntervalView< T > outSlice = Views.hyperSlice( outImg, 2, z );
			final Cursor< T > outSliceCursor = Views.flatIterable( outSlice ).cursor();

			while ( outSliceCursor.hasNext() )
			{
				double val = 0;
				for ( int ch = 0; ch < channels; ch++ )
					val += cursors[ch].next().getRealDouble();
				outSliceCursor.next().setReal( val / channels );
			}

			for ( int ch = 0; ch < channels; ch++ )
				if ( cursors[ch].hasNext() )
				{
					System.out.println( "Cursors mismatch" );
					return null;
				}
		}

		final ImagePlus impOut = ImageJFunctions.wrap( outImg, null );
		Utils.workaroundImagePlusNSlices(impOut);
		return impOut;
	}*/

	// Fully imglib2 approach
	/*private static < T extends RealType< T > & NativeType< T > > ImagePlus cropVirtualAveragingChannels( final ImagePlus inImp, final Boundaries region )
	{
		final int timepoint = 1;
		final int channels = inImp.getNChannels();

		final VirtualStackImageLoader< T, ?, ? > virtualLoader = ( VirtualStackImageLoader ) VirtualStackImageLoader.createUnsignedShortInstance( inImp );
		final Img< FloatType > tmpImg = new ImagePlusImgFactory< FloatType >().create( region, new FloatType() );

		for ( int ch = 0; ch < channels; ch++ )
		{
			System.out.println( "Cropping channel " + ch );
			final RandomAccessibleInterval< T > inImg = virtualLoader.getSetupImgLoader( ch ).getImage( timepoint );

			final Cursor< T > inCursor = Views.flatIterable( Views.offsetInterval( inImg, region ) ).cursor();
			final Cursor< FloatType > tmpCursor = Views.flatIterable( tmpImg ).cursor();

			while ( tmpCursor.hasNext() || inCursor.hasNext() )
			{
				final FloatType val = tmpCursor.next();
				val.set( val.get() + inCursor.next().getRealFloat() );
			}
		}

		System.out.println( "Averaging the results" );
		final T type = ( T ) ImageType.valueOf( inImp.getType() ).getType();
		final ImagePlusImg< T, ? > outImg = new ImagePlusImgFactory< T >().create( region, type.createVariable() );
		final Cursor< FloatType > tmpCursor = Views.flatIterable( tmpImg ).cursor();
		final Cursor< T > outCursor = Views.flatIterable( outImg ).cursor();

		while ( outCursor.hasNext() || tmpCursor.hasNext() )
			outCursor.next().setReal( tmpCursor.next().getRealFloat() / channels );

		final ImagePlus impOut;
		try
		{
			impOut = outImg.getImagePlus();
		}
		catch ( final ImgLibException e )
		{
			e.printStackTrace();
			return null;
		}
		Utils.workaroundImagePlusNSlices( impOut );
		return impOut;
	}*/

	// virtualstack loader + slice by slice traversing with cache reset
//	private static < T extends RealType< T > & NativeType< T > > ImagePlus cropVirtualAveragingChannels( final ImagePlus inImp, final Boundaries region )
//	{
//		final int timepoint = 1;
//		final int channels = inImp.getNChannels();
//
//		final VirtualStackImageLoader< T, ?, ? > virtualLoader = ( VirtualStackImageLoader ) VirtualStackImageLoader.createUnsignedShortInstance( inImp );
//		final RandomAccessibleInterval< T >[] channelImgs = new RandomAccessibleInterval[ channels ];
//		for ( int ch = 0; ch < channels; ch++ )
//			channelImgs[ ch ] = virtualLoader.getSetupImgLoader( ch ).getImage( timepoint );
//
//		final Interval regionSlice = new FinalInterval( new long[] { region.min(0), region.min(1) }, new long[] { region.max(0), region.max(1) } );
//
//		final T type = ( T ) ImageType.valueOf( inImp.getType() ).getType();
//		final ImagePlusImg< T, ? > outImg = new ImagePlusImgFactory< T >().create( region, type.createVariable() );
//
//		final int slices = inImp.getNSlices();
//		for ( int z = 0; z < slices; z++ )
//		{
//			final Cursor< T >[] cursors = new Cursor[ channels ];
//			for ( int ch = 0; ch < channels; ch++ )
//				cursors[ ch ] = Views.flatIterable( Views.offsetInterval( Views.hyperSlice( channelImgs[ ch ], 2, z ), regionSlice ) ).cursor();
//
//			final Cursor< T > outSliceCursor = Views.flatIterable( Views.hyperSlice( outImg, 2, z ) ).cursor();
//
//			while ( outSliceCursor.hasNext() )
//			{
//				double val = 0;
//				for ( int ch = 0; ch < channels; ch++ )
//					val += cursors[ch].next().getRealDouble();
//				outSliceCursor.next().setReal( val / channels );
//			}
//
//			for ( int ch = 0; ch < channels; ch++ )
//				if ( cursors[ch].hasNext() )
//				{
//					System.out.println( "Cursors mismatch" );
//					return null;
//				}
//
//			for ( int ch = 0; ch < channels; ch++ )
//				virtualLoader.getCacheControl().clearCache();
//		}
//
//		final ImagePlus impOut = outImg.getImagePlus();
//		Utils.workaroundImagePlusNSlices(impOut);
//		return impOut;
//	}


//	private static <T extends NumericType< T > >void blur(
//			final RandomAccessibleInterval< T > image,
//			final Interval interval,
//			final double[] sigmas ) throws IncompatibleTypeException
//	{
//		final RandomAccessible< T > extendedImage = Views.extendMirrorSingle( image );
//		final RandomAccessibleInterval< T > crop = Views.interval( extendedImage, interval );
//		final ExecutorService service = Executors.newFixedThreadPool( 1 );
//		Gauss3.gauss( sigmas, extendedImage, crop, service );
//		service.shutdown();
//	}

	private static <T extends NumericType< T > >void blur(
			final RandomAccessibleInterval< T > image,
			final double[] sigmas ) throws IncompatibleTypeException
	{
		final RandomAccessible< T > extendedImage = Views.extendMirrorSingle( image );
		Gauss3.gauss( sigmas, extendedImage, image, new SameThreadExecutorService() );
	}



	/**
	 * Finds final tile positions by globally optimizing pairwise shifts and stores updated tile configuration on the disk
	 */
	private void optimizeShifts( final List< SerializablePairWiseStitchingResult > pairwiseShifts, final double threshold ) throws PipelineExecutionException
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
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().getA().getIndex() ),
					(ImagePlusTimePoint)fakeTileImagesMap.get( pair.getTilePair().getB().getIndex() ) );

			comparePair.setRelativeShift( pair.getOffset() );
			comparePair.setCrossCorrelation( pair.getCrossCorrelation() );
			comparePair.setIsValidOverlap( pair.getIsValidOverlap() );

			comparePairs.addElement( comparePair );
		}





		/*final boolean loadAnotherChannel = false;

		final Map< Integer, Map< Integer, ComparePair > > anotherChannel = new TreeMap<>();
		final Map< Integer, TileInfo > anotherChannelTiles = new TreeMap<>();
		List< SerializablePairWiseStitchingResult > anotherChannelShifts;

		if ( loadAnotherChannel )
		{
			try
			{
				anotherChannelShifts = TileInfoJSONProvider.loadPairwiseShifts( "/nobackup/saalfeld/igor/Sample1_C1/ch0/optimization/ch1-merged_pairwise_shifts_set.json" );
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
				final int ind1 = Math.min( shift.getTilePair().getA().getIndex(), shift.getTilePair().getB().getIndex() ) - tilesPerChannel;
				final int ind2 = Math.max( shift.getTilePair().getA().getIndex(), shift.getTilePair().getB().getIndex() ) - tilesPerChannel;

				if ( !anotherChannel.containsKey( ind1 ) )
					anotherChannel.put( ind1, new TreeMap<>() );


				final ComparePair comparePair = new ComparePair(
						(ImagePlusTimePoint)anotherChannelFakeTileImagesMap.get( shift.getTilePair().getA().getIndex() ),
						(ImagePlusTimePoint)anotherChannelFakeTileImagesMap.get( shift.getTilePair().getB().getIndex() ) );

				comparePair.setRelativeShift( shift.getOffset() );
				comparePair.setCrossCorrelation( shift.getCrossCorrelation() );
				comparePair.setIsValidOverlap( shift.getIsValidOverlap() );

				anotherChannel.get( ind1 ).put( ind2, comparePair );
				additionalMatches++;
			}
		}*/


		final long timestamp = System.nanoTime();
		System.out.println( "Perform global optimization" );
		final ArrayList< ImagePlusTimePoint > optimized = new ArrayList<>();
		if ( !comparePairs.isEmpty() )
		{
			// Save the optimized tile configuration on every iteration
			final GlobalOptimizationPerformer.TileConfigurationObserver tileConfigurationObserver = new GlobalOptimizationPerformer.TileConfigurationObserver()
			{
				@Override
				public void configurationUpdated( final ArrayList< ImagePlusTimePoint > updatedTiles )
				{
					// Update tile positions
					for ( int channel = 0; channel < job.getChannels(); channel++ )
					{
						final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( job.getTiles( channel ) );
						for ( final ImagePlusTimePoint optimizedTile : updatedTiles )
						{
							final double[] pos = new double[ job.getDimensionality() ];
							optimizedTile.getModel().applyInPlace( pos );

							if ( tilesMap.containsKey( optimizedTile.getImpId() ) )
								tilesMap.get( optimizedTile.getImpId() ).setPosition( pos );
//							else
//								anotherChannelTiles.get( optimizedTile.getImpId() ).setPosition( pos );
						}

						for ( final Tile<?> lostTile : GlobalOptimizationPerformer.lostTiles )
						{
							final int lostTileIndex = ((ImagePlusTimePoint)lostTile).getImpId();
							tilesMap.remove( lostTileIndex );
//							anotherChannelTiles.remove( lostTileIndex );
						}

//						System.out.println( "Intermediate tiles configuration: " + "Channel #0 tiles=" + tilesMap.size()+", Channel #1 tiles=" + anotherChannelTiles.size() );
						System.out.println( "Intermediate tiles configuration: " + "Channel #"+channel+" tiles=" + tilesMap.size() );

						try
						{
							final TileInfo[] tilesToSave = tilesMap.values().toArray( new TileInfo[ 0 ] );
							TileOperations.translateTilesToOriginReal( tilesToSave );
							TileInfoJSONProvider.saveTilesConfiguration( tilesToSave, Utils.addFilenameSuffix( job.getArgs().inputTileConfigurations().get( channel ), "-intermediate" ) );

							/*if ( !anotherChannelTiles.isEmpty() )
							{
								final TileInfo[] anotherChannelTilesToSave = anotherChannelTiles.values().toArray( new TileInfo[ 0 ] );
								TileOperations.translateTilesToOriginReal( anotherChannelTilesToSave );
								TileInfoJSONProvider.saveTilesConfiguration( anotherChannelTilesToSave, job.getBaseFolder() + "/ch1-intermediate.json" );
							}*/
						}
						catch ( final IOException e )
						{
							e.printStackTrace();
						}
					}
				}
			};

			optimized.addAll(
					GlobalOptimizationPerformer.optimize(
							comparePairs,
							comparePairs.get( 0 ).getTile1(),
							job.getParams(),
							tileConfigurationObserver,
							/* loadAnotherChannel ? anotherChannel : null */ null )
					);
		}

		System.out.println( "Global optimization done" );
		//for ( final ImagePlusTimePoint imt : optimized )
		//	Log.info( imt.getImpId() + ": " + imt.getModel() );

		System.out.println( "****" );
		System.out.println( "Global optimization took " + ((System.nanoTime() - timestamp)/Math.pow( 10, 9 )) +"s" );

		// Update tile positions
		for ( int channel = 0; channel < job.getChannels(); channel++ )
		{
			final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( job.getTiles( channel ) );
			for ( final ImagePlusTimePoint optimizedTile : optimized )
			{
				final double[] pos = new double[ job.getDimensionality() ];
				optimizedTile.getModel().applyInPlace( pos );

				if ( tilesMap.containsKey( optimizedTile.getImpId() ) )
					tilesMap.get( optimizedTile.getImpId() ).setPosition( pos );
//				else
//					anotherChannelTiles.get( optimizedTile.getImpId() ).setPosition( pos );
			}

			for ( final Tile<?> lostTile : GlobalOptimizationPerformer.lostTiles )
			{
				final int lostTileIndex = ((ImagePlusTimePoint)lostTile).getImpId();
				tilesMap.remove( lostTileIndex );
//				anotherChannelTiles.remove( lostTileIndex );
			}

			System.out.println( "Channel #"+channel+" tiles: " + tilesMap.size() );
//			System.out.println( "Channel #1 tiles: " + anotherChannelTiles.size() );


			// save final tiles configuration
			try
			{
				final TileInfo[] tilesToSave = tilesMap.values().toArray( new TileInfo[ 0 ] );
				TileOperations.translateTilesToOriginReal( tilesToSave );
				TileInfoJSONProvider.saveTilesConfiguration( tilesToSave, Utils.addFilenameSuffix( job.getArgs().inputTileConfigurations().get( channel ), "-final" ) );

				/*if ( !anotherChannelTiles.isEmpty() )
				{
					final TileInfo[] anotherChannelTilesToSave = anotherChannelTiles.values().toArray( new TileInfo[ 0 ] );
					TileOperations.translateTilesToOriginReal( anotherChannelTilesToSave );
					TileInfoJSONProvider.saveTilesConfiguration( anotherChannelTilesToSave, job.getBaseFolder() + "/ch1-final.json" );
				}*/
			}
			catch ( final IOException e )
			{
				e.printStackTrace();
			}


			// save final pairwise shifts configuration (pairs that have been used on the final step of the optimization routine)
			try
			{
				final List< SerializablePairWiseStitchingResult > finalPairwiseShifts = new ArrayList<>();
				for ( final ComparePair finalPair : comparePairs )
				{
					final TilePair tilePair = new TilePair( tilesMap.get( finalPair.getTile1().getImpId() ), tilesMap.get( finalPair.getTile2().getImpId() ) );
					if ( tilePair.getA() != null && tilePair.getB() != null )
					{
						final SerializablePairWiseStitchingResult finalShift = new SerializablePairWiseStitchingResult( tilePair, finalPair.getRelativeShift(), finalPair.getCrossCorrelation() );
						finalShift.setIsValidOverlap( finalPair.getIsValidOverlap() );
						finalPairwiseShifts.add( finalShift );
					}
				}
				TileInfoJSONProvider.savePairwiseShifts( finalPairwiseShifts, Utils.addFilenameSuffix( Utils.addFilenameSuffix( job.getArgs().inputTileConfigurations().get( channel ), "-final" ), "_pairwise"	) );

				/*if ( !anotherChannelTiles.isEmpty() )
				{
					final TileInfo[] anotherChannelTilesToSave = anotherChannelTiles.values().toArray( new TileInfo[ 0 ] );
					TileOperations.translateTilesToOriginReal( anotherChannelTilesToSave );
					TileInfoJSONProvider.saveTilesConfiguration( anotherChannelTilesToSave, job.getBaseFolder() + "/ch1-final.json" );
				}*/
			}
			catch ( final IOException e )
			{
				e.printStackTrace();
			}
		}
	}
}
