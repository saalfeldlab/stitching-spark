package org.janelia.stitching.experimental;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.Boundaries;
import org.janelia.stitching.ImageType;
import org.janelia.stitching.PipelineExecutionException;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;
import org.janelia.stitching.analysis.CheckConnectedGraphs;
import org.janelia.util.ImageImporter;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class ExportOverlaps
{
	private static final double[] voxelSize = new double[] { 0.2250074, 0.2250074, 1.0 };

	/*public static < T extends RealType< T > & NativeType< T > > void main( final String[] args ) throws Exception
	{
		final String inputFilepath = args[ 0 ];
		final String outputPath = inputFilepath + "_overlaps";
		new File( outputPath ).mkdirs();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final TreeMap< Integer, TileInfo > tilesMap = Utils.createTilesMap( tiles );

		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setPosition( d, tile.getPosition( d ) / voxelSize[ d ] );

		final ArrayList< TilePair > overlappingTiles = TileOperations.findOverlappingTiles( tiles );
		System.out.println( "There are " + overlappingTiles.size() + " overlapping pairs" );
		// Remove pairs with small overlap area if only adjacent pairs are requested
		if ( args.length > 1 && args[ 1 ].equals( "--adjacent" ) )
		{
			final List< TilePair > adjacentOverlappingTiles = FilterAdjacentShifts.filterAdjacentPairs( overlappingTiles );
			overlappingTiles.clear();
			overlappingTiles.addAll( adjacentOverlappingTiles );
			System.out.println( "Retaining only " + overlappingTiles.size() + " adjacent pairs of them" );
		}

		final List< Integer > connectedComponentsSize = CheckConnectedGraphs.connectedComponentsSize( overlappingTiles );
		if ( connectedComponentsSize.size() > 1 )
			throw new PipelineExecutionException( "Overlapping pairs form more than one connected component: " + connectedComponentsSize );
		if ( connectedComponentsSize.get( 0 ) != tiles.length )
			throw new PipelineExecutionException( "Overlapping pairs cover only " + connectedComponentsSize.get( 0 ) + " tiles out of " + tiles.length );

		final TreeMap< Integer, List< Integer > > tileToOverlaps = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tileToOverlaps.put( tile.getIndex(), new ArrayList<>() );
		for ( final TilePair tilePair : overlappingTiles )
			for ( int i = 0; i < 2; i++ )
				tileToOverlaps.get( tilePair.toArray()[ i ].getIndex() ).add( tilePair.toArray()[ ( i + 1 ) % 2 ].getIndex() );

		final MultithreadedExecutor threadPool = new MultithreadedExecutor();
		final AtomicInteger openedImages = new AtomicInteger();

		final List< Integer > keys = new ArrayList<>( tileToOverlaps.keySet() );
		threadPool.run( keyIndex ->
			{
				final int tileIndex = keys.get( keyIndex );
				final TileInfo tile = tilesMap.get( tileIndex );
				final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
				Utils.workaroundImagePlusNSlices( imp );

				System.out.println( "Opened an image, openedImages="+openedImages.incrementAndGet() );

				final T type = ( T ) ImageType.valueOf( imp.getType() ).getType();
				final Img< T > avImg = new ImagePlusImgFactory< T >().create( tile.getSize(), type.createVariable() );

				final int channels = imp.getNChannels();
				final int frame = 0;
				for ( int z = 0; z < imp.getNSlices(); z++ )
				{
					final Cursor< T >[] cursors = new Cursor[ channels ];
					for ( int ch = 0; ch < channels; ch++ )
						cursors[ ch ] = Views.flatIterable(
								( Img ) ArrayImgs.unsignedShorts(
										( short[] ) imp.getStack().getProcessor(
												imp.getStackIndex( ch + 1, z + 1, frame + 1 ) ).getPixels(),
										new long[] { imp.getWidth(), imp.getHeight() }
								)
							).cursor();

					final Cursor< T > outSliceCursor = Views.flatIterable( Views.hyperSlice( avImg, 2, z ) ).cursor();

					while ( outSliceCursor.hasNext() )
					{
						double val = 0;
						for ( int ch = 0; ch < channels; ch++ )
							val += cursors[ch].next().getRealDouble();
						outSliceCursor.next().setReal( val / channels );
					}

					for ( int ch = 0; ch < channels; ch++ )
						if ( cursors[ch].hasNext() )
							System.out.println( "Cursors mismatch" );
				}

				imp.close();
				System.out.println( "Created avImg and closed the image, openedImages="+openedImages.decrementAndGet() );

				for ( final int overlappingTileIndex : tileToOverlaps.get( tileIndex ) )
				{
					final TileInfo overlappingTile = tilesMap.get( overlappingTileIndex );
					final Boundaries overlap = TileOperations.getOverlappingRegion( tile, overlappingTile );
					final RandomAccessibleInterval< T > avImgOverlap = Views.interval( avImg, overlap );
					final ImagePlus impOverlap = ImageJFunctions.wrap( avImgOverlap, String.format( "%d(%d)", tileIndex, overlappingTileIndex ) );
					Utils.workaroundImagePlusNSlices( impOverlap );
					IJ.saveAsTiff( impOverlap, outputPath + "/" + impOverlap.getTitle() + ".tif" );
					impOverlap.close();
				}
			},
			keys.size() );

		threadPool.shutdown();
	}*/



	public static < T extends RealType< T > & NativeType< T > > void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final String inputFilepath = args[ 0 ];
		final String outputPath = inputFilepath + "_overlaps";
		new File( outputPath ).mkdirs();

		final int tileIndex = Integer.parseInt( args[ 1 ] );
		System.out.println( "Processing tile " + tileIndex );

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final TreeMap< Integer, TileInfo > tilesMap = Utils.createTilesMap( tiles );

		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < tile.numDimensions(); d++ )
				tile.setPosition( d, tile.getPosition( d ) / voxelSize[ d ] );

		final ArrayList< TilePair > overlappingTiles = TileOperations.findOverlappingTiles( tiles );

		final List< Integer > connectedComponentsSize = CheckConnectedGraphs.connectedComponentsSize( tiles );
		if ( connectedComponentsSize.size() > 1 )
			throw new PipelineExecutionException( "Overlapping pairs form more than one connected component: " + connectedComponentsSize );
		if ( connectedComponentsSize.get( 0 ) != tiles.length )
			throw new PipelineExecutionException( "Overlapping pairs cover only " + connectedComponentsSize.get( 0 ) + " tiles out of " + tiles.length );

		final TreeMap< Integer, List< Integer > > tileToOverlaps = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tileToOverlaps.put( tile.getIndex(), new ArrayList<>() );
		for ( final TilePair tilePair : overlappingTiles )
			for ( int i = 0; i < 2; i++ )
				tileToOverlaps.get( tilePair.toArray()[ i ].getIndex() ).add( tilePair.toArray()[ ( i + 1 ) % 2 ].getIndex() );

		final TileInfo tile = tilesMap.get( tileIndex );
		final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
		Utils.workaroundImagePlusNSlices( imp );

		final Boundaries[] overlappingRegions = new Boundaries[ tileToOverlaps.get( tileIndex ).size() ];
		for ( int i = 0; i < overlappingRegions.length; i++ )
			overlappingRegions[ i ] = TileOperations.getOverlappingRegion( tile, tilesMap.get( tileToOverlaps.get( tileIndex ).get( i ) ) );

		final T type = ( T ) ImageType.valueOf( imp.getType() ).getType();
		final Img< T >[] avImgs = new Img[ overlappingRegions.length ];
		for ( int i = 0; i < avImgs.length; i++ )
			avImgs[ i ] = new ImagePlusImgFactory< T >().create( overlappingRegions[ i ], type.createVariable() );

		System.out.println( "Allocated space for " + avImgs.length + " overlap area images:" );
		for ( int i = 0; i < avImgs.length; i++ )
			System.out.println( "--- " + Arrays.toString( Intervals.dimensionsAsIntArray( avImgs[ i ] ) ) );

		final int slices = imp.getNSlices();
		final int channels = imp.getNChannels();
		final int frame = 0;
		for ( int z = 0; z < slices; z++ )
		{
			if ( z % 500 == 0 )
				System.out.println( String.format( "Tile %d: processing slice %d out of %d", tileIndex, z, slices ) );

			final Img< T >[] chImg = new Img[ channels ];
			for ( int ch = 0; ch < channels; ch++ )
				chImg[ ch ] = ( Img ) ArrayImgs.unsignedShorts(
						( short[] ) imp.getStack().getProcessor(
								imp.getStackIndex( ch + 1, z + 1, frame + 1 ) ).getPixels(),
						new long[] { imp.getWidth(), imp.getHeight() } );

			for ( int i = 0; i < avImgs.length; i++ )
			{
				final Cursor< T >[] cursors = new Cursor[ channels ];
				for ( int ch = 0; ch < channels; ch++ )
					cursors[ ch ] = Views.flatIterable(
							Views.offsetInterval(
									chImg[ ch ],
									new FinalInterval(
											new long[] { overlappingRegions[ i ].min(0), overlappingRegions[ i ].min(1) },
											new long[] { overlappingRegions[ i ].max(0), overlappingRegions[ i ].max(1) } ) )
							).cursor();

				final Cursor< T > outSliceCursor = Views.flatIterable( Views.hyperSlice( avImgs[ i ], 2, z ) ).cursor();

				while ( outSliceCursor.hasNext() )
				{
					double val = 0;
					for ( int ch = 0; ch < channels; ch++ )
						val += cursors[ch].next().getRealDouble();
					outSliceCursor.next().setReal( val / channels );
				}

				for ( int ch = 0; ch < channels; ch++ )
					if ( cursors[ch].hasNext() )
						System.out.println( "Cursors mismatch" );
			}
		}

		imp.close();

		for ( int i = 0; i < avImgs.length; i++ )
		{
			final ImagePlus impOverlap = ImageJFunctions.wrap( avImgs[ i ], String.format( "%d(%d)", tileIndex, tileToOverlaps.get( tileIndex ).get( i )) );
			Utils.workaroundImagePlusNSlices( impOverlap );
			IJ.saveAsTiff( impOverlap, outputPath + "/" + impOverlap.getTitle() + ".tif" );
			impOverlap.close();
		}
	}










}
