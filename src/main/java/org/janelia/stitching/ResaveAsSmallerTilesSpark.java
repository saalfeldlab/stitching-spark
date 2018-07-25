package org.janelia.stitching;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class ResaveAsSmallerTilesSpark implements Serializable, AutoCloseable
{
	private static final long serialVersionUID = -8600903133731319708L;

	private static class ResaveAsSmallerTilesCmdArgs implements Serializable
	{
		private static final long serialVersionUID = -8996450783846140673L;

		@Option(name = "-i", aliases = { "--input" }, required = true,
				usage = "Path/link to a tile configuration JSON file. Multiple configurations can be passed at once.")
		public List< String > inputTileConfigurations;

		@Option(name = "-t", aliases = { "--target" }, required = true,
				usage = "Target location (filesystem directory or cloud bucket) to store the resulting tile images and configurations.")
		public String targetLocation;

		@Option(name = "-d", aliases = { "--dimension" }, required = false,
				usage = "Dimension to split tiles along (Z by default)")
		public int splitDimension = 2;

		@Option(name = "-s", aliases = { "--size" }, required = false,
				usage = "Size of a new tile in the dimension to split along. Defaults to an average size in other two dimensions")
		public Integer splitSize = null;

		@Option(name = "-o", aliases = { "--overlap" }, required = false,
				usage = "Overlap on each side as a ratio relative to the new tile size. This is only an initial guess, where the actual overlap is determined based on the size of the volume such that all tiles have the same size.")
		public double minOverlapRatioEachSide = 0.1;

		public boolean parsedSuccessfully = false;

		public ResaveAsSmallerTilesCmdArgs( final String[] args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try {
				parser.parseArgument( args );
				parsedSuccessfully = true;
			} catch ( final CmdLineException e ) {
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}
		}
	}

	public static void main( final String[] args ) throws Exception
	{
		final ResaveAsSmallerTilesCmdArgs resaveSmallerTilesCmdArgs = new ResaveAsSmallerTilesCmdArgs( args );
		if ( !resaveSmallerTilesCmdArgs.parsedSuccessfully )
			System.exit( 1 );

		try ( final ResaveAsSmallerTilesSpark driver = new ResaveAsSmallerTilesSpark( resaveSmallerTilesCmdArgs ) )
		{
			driver.run();
		}
	}

	private final ResaveAsSmallerTilesCmdArgs args;
	private final String targetImagesLocation;
	private final transient JavaSparkContext sparkContext;

	private ResaveAsSmallerTilesSpark( final ResaveAsSmallerTilesCmdArgs args ) throws IOException
	{
		this.args = args;

		targetImagesLocation = PathResolver.get( args.targetLocation, "tile-images-retiled-in-" + AxisMapping.getAxisStr( args.splitDimension ).toUpperCase() );
		final DataProvider dataProviderTarget = DataProviderFactory.createByURI( URI.create( targetImagesLocation ) );
		dataProviderTarget.createFolder( URI.create( targetImagesLocation ) );

		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "ResaveSmallerTiles" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);
	}

	private void run() throws IOException
	{
		for ( final String inputTileConfiguration : args.inputTileConfigurations )
			processChannel( inputTileConfiguration );
	}

	private void processChannel( final String inputTileConfiguration ) throws IOException
	{
		final DataProvider sourceDataProvider = DataProviderFactory.createByURI( URI.create( inputTileConfiguration ) );
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( sourceDataProvider.getJsonReader( URI.create( inputTileConfiguration ) ) );

		final long[] originalTileSize = tiles[ 0 ].getSize();
		final long[] newTileSize = determineNewTileSize( originalTileSize );
		final List< Interval > newTilesIntervalsInSingleTile = getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize );

		final Broadcast< List< Interval > > broadcastedNewTilesIntervalsInSingleTile = sparkContext.broadcast( newTilesIntervalsInSingleTile );
		final List< TileInfo > newTiles = sparkContext.parallelize( Arrays.asList( tiles ), tiles.length ).flatMap(
				tile -> resaveTileAsSmallerTiles( tile, broadcastedNewTilesIntervalsInSingleTile.value() ).iterator()
			).collect();

		// global indexing
		for ( int i = 0; i < newTiles.size(); ++i )
			newTiles.get( i ).setIndex( i );

		final DataProvider targetDataProvider = DataProviderFactory.createByURI( URI.create( args.targetLocation ) );
		final String newTilesConfigurationFilename = Utils.addFilenameSuffix( PathResolver.getFileName( inputTileConfiguration ), "-retiled-in-" + AxisMapping.getAxisStr( args.splitDimension ).toUpperCase() );
		final String newTilesConfigurationPath = PathResolver.get( args.targetLocation, newTilesConfigurationFilename );
		TileInfoJSONProvider.saveTilesConfiguration( newTiles.toArray( new TileInfo[ 0 ] ), targetDataProvider.getJsonWriter( URI.create( newTilesConfigurationPath ) ) );
	}

	private < T extends NativeType< T > & RealType< T > > List< TileInfo > resaveTileAsSmallerTiles( final TileInfo tile, final List< Interval > newTilesIntervalsInSingleTile ) throws IOException, ImgLibException
	{
		final DataProvider sourceDataProvider = DataProviderFactory.createByURI( URI.create( tile.getFilePath() ) );
		final RandomAccessibleInterval< T > tileImg = TileLoader.loadTile( tile, sourceDataProvider );
		final T tileImageType = Util.getTypeFromInterval( tileImg );

		final DataProvider targetDataProvider = DataProviderFactory.createByURI( URI.create( targetImagesLocation ) );
		final List< TileInfo > newTilesInSingleTile = new ArrayList<>();
		for ( final Interval newTileInterval : newTilesIntervalsInSingleTile )
		{
			final ImagePlusImg< T, ? > newTileImg = new ImagePlusImgFactory< T >().create( newTileInterval, tileImageType );
			final Cursor< T > newTileImgCursor = Views.flatIterable( newTileImg ).cursor();
			final Cursor< T > tileImgIntervalCursor = Views.flatIterable( Views.interval( tileImg, newTileInterval ) ).cursor();
			while ( newTileImgCursor.hasNext() || tileImgIntervalCursor.hasNext() )
				newTileImgCursor.next().set( tileImgIntervalCursor.next() );
			final ImagePlus newTileImagePlus = newTileImg.getImagePlus();
			Utils.workaroundImagePlusNSlices( newTileImagePlus );

			final String newTileImageFilename = Utils.addFilenameSuffix( PathResolver.getFileName( tile.getFilePath() ), "_retiled-" + newTilesInSingleTile.size() + AxisMapping.getAxisStr( args.splitDimension ) );
			final String newTileImagePath = PathResolver.get( targetImagesLocation, newTileImageFilename );
			targetDataProvider.saveImage( newTileImagePlus, URI.create( newTileImagePath ) );

			final double[] newTilePosition = new double[ tile.numDimensions() ];
			for ( int d = 0; d < newTilePosition.length; ++d )
				newTilePosition[ d ] = tile.getPosition( d ) + newTileInterval.min( d );

			final TileInfo newTile = new TileInfo( tile.numDimensions() );
			newTile.setFilePath( newTileImagePath );
			newTile.setIndex( newTilesInSingleTile.size() );
			newTile.setPosition( newTilePosition );
			newTile.setSize( Intervals.dimensionsAsLongArray( newTileInterval ) );
			newTile.setType( tile.getType() );
			newTile.setPixelResolution( tile.getPixelResolution().clone() );

			newTilesInSingleTile.add( newTile );
		}
		return newTilesInSingleTile;
	}

	private long[] determineNewTileSize( final long[] originalTileSize )
	{
		final long[] newTileSize = originalTileSize.clone();
		if ( args.splitSize != null )
		{
			newTileSize[ args.splitDimension ] = args.splitSize;
		}
		else
		{
			int tileSizeSumExcludingSplitDimension = 0;
			for ( int d = 0; d < originalTileSize.length; ++d )
				if ( d != args.splitDimension )
					tileSizeSumExcludingSplitDimension += originalTileSize[ d ];
			newTileSize[ args.splitDimension ] = Math.round( ( double ) tileSizeSumExcludingSplitDimension / ( newTileSize.length - 1 ) );
		}
		return newTileSize;
	}

	private List< Interval > getNewTilesIntervalsInSingleTile( final long[] originalTileSize, final long[] newTileSize )
	{
		return getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize, args.splitDimension, args.minOverlapRatioEachSide );
	}

	public static List< Interval > getNewTilesIntervalsInSingleTile(
			final long[] originalTileSize,
			final long[] newTileSize,
			final int splitDimension,
			final double minOverlapRatioEachSide )
	{
		for ( int d = 0; d < Math.max( originalTileSize.length, newTileSize.length ); ++d )
			if ( d != splitDimension && originalTileSize[ d ] != newTileSize[ d ] )
				throw new RuntimeException( "tile size is different in any dimensions other than split dimension: originalTileSize=" + Arrays.toString( originalTileSize ) + ", newTileSize=" + Arrays.toString( newTileSize ) );

		// fit in new tiles such that they exactly cover the original tile while the overlap is as close as possible to the requrested min ratio
		final long minOverlapInSplitDimensionEachSide = ( long ) Math.ceil( newTileSize[ splitDimension ] * minOverlapRatioEachSide );
		final int numNewTilesInSplitDimension = ( int ) Math.ceil( ( double ) ( originalTileSize[ splitDimension ] - minOverlapInSplitDimensionEachSide ) / ( newTileSize[ splitDimension ] - minOverlapInSplitDimensionEachSide ) );
		final long newTotalSizeInSplitDimension = numNewTilesInSplitDimension * ( newTileSize[ splitDimension ] - minOverlapInSplitDimensionEachSide ) + minOverlapInSplitDimensionEachSide;
		final int numNewTilesWithIncreasedOverlap = ( int ) ( newTotalSizeInSplitDimension - originalTileSize[ splitDimension ] );
		if ( numNewTilesWithIncreasedOverlap < 0 || numNewTilesWithIncreasedOverlap >= numNewTilesInSplitDimension )
			throw new RuntimeException( "incorrect calculation, numNewTilesWithIncreasedOverlap=" + numNewTilesWithIncreasedOverlap + ", numNewTilesInSplitDimension=" + numNewTilesInSplitDimension );

		final List< Interval > newTilesIntervals = new ArrayList<>();
		for ( int i = 0; i < numNewTilesInSplitDimension; ++i )
		{
			final long[] min = new long[ newTileSize.length ], max = Intervals.maxAsLongArray( new FinalInterval( originalTileSize ) );
			min[ splitDimension ] = i * ( newTileSize[ splitDimension ] - minOverlapInSplitDimensionEachSide ) - Math.min( i, numNewTilesWithIncreasedOverlap );
			max[ splitDimension ] = min[ splitDimension ] + newTileSize[ splitDimension ] - 1;
			newTilesIntervals.add( new FinalInterval( min, max ) );
		}
		return newTilesIntervals;
	}

	@Override
	public void close() throws Exception
	{
		sparkContext.close();
	}
}
