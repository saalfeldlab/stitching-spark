package org.janelia.stitching;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.dataaccess.CloudURI;
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

		@Option(name = "-t", aliases = { "--target" }, required = false,
				usage = "Target location (filesystem directory or cloud bucket) to store the resulting tile images and configurations.")
		public String targetLocation;

		@Option(name = "-d", aliases = { "--dimension" }, required = false,
				usage = "Dimension to retile (Z by default)")
		public int retileDimension = 2;

		@Option(name = "-s", aliases = { "--size" }, required = false,
				usage = "Size of a new tile in the retile dimension. Defaults to an average size in other two dimensions")
		public Integer retileSize = null;

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

			// make sure that inputTileConfigurations are absolute file paths if running on a traditional filesystem
			for ( int i = 0; i < inputTileConfigurations.size(); ++i )
				if ( !CloudURI.isCloudURI( inputTileConfigurations.get( i ) ) )
					inputTileConfigurations.set( i, Paths.get( inputTileConfigurations.get( i ) ).toAbsolutePath().toString() );

			if ( targetLocation != null )
			{
				// make sure that targetLocation is an absolute file path if running on a traditional filesystem
				if ( !CloudURI.isCloudURI( targetLocation ) )
					targetLocation = Paths.get( targetLocation ).toAbsolutePath().toString();
			}
			else
			{
				targetLocation = PathResolver.get( PathResolver.getParent( inputTileConfigurations.get( 0 ) ), "retiled-images" );
			}
		}
	}

	public static void main( final String[] args ) throws Exception
	{
		final ResaveAsSmallerTilesCmdArgs resaveSmallerTilesCmdArgs = new ResaveAsSmallerTilesCmdArgs( args );
		if ( !resaveSmallerTilesCmdArgs.parsedSuccessfully )
			System.exit( 1 );

		if ( resaveSmallerTilesCmdArgs.minOverlapRatioEachSide > 0.5 )
			throw new IllegalArgumentException( "Given min.overlap ratio " + resaveSmallerTilesCmdArgs.minOverlapRatioEachSide + " exceeds its max.allowed value (0.5)" );

		try ( final ResaveAsSmallerTilesSpark driver = new ResaveAsSmallerTilesSpark( resaveSmallerTilesCmdArgs ) )
		{
			driver.run();
		}
	}

	private final ResaveAsSmallerTilesCmdArgs args;
	private final String targetImagesLocation;
	private final transient JavaSparkContext sparkContext;
	private final transient PrintWriter logWriter;

	private ResaveAsSmallerTilesSpark( final ResaveAsSmallerTilesCmdArgs args ) throws IOException
	{
		this.args = args;

		targetImagesLocation = PathResolver.get( args.targetLocation, "tile-images-retiled-in-" + AxisMapping.getAxisStr( args.retileDimension ).toUpperCase() );
		final DataProvider dataProviderTarget = DataProviderFactory.create( DataProviderFactory.detectType( targetImagesLocation ) );
		dataProviderTarget.createFolder( targetImagesLocation );

		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "ResaveSmallerTiles" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);

		logWriter = new PrintWriter( dataProviderTarget.getOutputStream( PathResolver.get( args.targetLocation, "retiling-log.txt" ) ) );
	}

	private void run() throws IOException
	{
		for ( final String inputTileConfiguration : args.inputTileConfigurations )
			processChannel( inputTileConfiguration );
	}

	private void processChannel( final String inputTileConfiguration ) throws IOException
	{
		final DataProvider sourceDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputTileConfiguration ) );
		final TileInfo[] tiles = sourceDataProvider.loadTiles( inputTileConfiguration );

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

		if ( logWriter != null )
		{
			logWriter.println();
			logWriter.println( "Total number of input tiles: " + tiles.length );
			logWriter.println( "Total number of output tiles: " + newTiles.size() );
			logWriter.println( "--------------------------------------------" );
		}

		final DataProvider targetDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( args.targetLocation ) );
		final String newTilesConfigurationFilename = Utils.addFilenameSuffix( PathResolver.getFileName( inputTileConfiguration ), "-retiled-in-" + AxisMapping.getAxisStr( args.retileDimension ).toUpperCase() );
		final String newTilesConfigurationPath = PathResolver.get( args.targetLocation, newTilesConfigurationFilename );
		targetDataProvider.saveTiles( newTiles.toArray( new TileInfo[ 0 ] ), newTilesConfigurationPath );
	}

	private < T extends NativeType< T > & RealType< T > > List< TileInfo > resaveTileAsSmallerTiles( final TileInfo tile, final List< Interval > newTilesIntervalsInSingleTile ) throws IOException, ImgLibException
	{
		final DataProvider sourceDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( tile.getFilePath() ) );
		final RandomAccessibleInterval< T > tileImg = TileLoader.loadTile( tile, sourceDataProvider );
		final T tileImageType = Util.getTypeFromInterval( tileImg );

		final DataProvider targetDataProvider = DataProviderFactory.create( DataProviderFactory.detectType( targetImagesLocation ) );
		final List< TileInfo > newTilesInSingleTile = new ArrayList<>();
		for ( final Interval newTileInterval : newTilesIntervalsInSingleTile )
		{
			final ImagePlusImg< T, ? > newTileImg = new ImagePlusImgFactory<>( tileImageType ).create( newTileInterval );
			final Cursor< T > newTileImgCursor = Views.flatIterable( newTileImg ).cursor();
			final Cursor< T > tileImgIntervalCursor = Views.flatIterable( Views.interval( tileImg, newTileInterval ) ).cursor();
			while ( newTileImgCursor.hasNext() || tileImgIntervalCursor.hasNext() )
				newTileImgCursor.next().set( tileImgIntervalCursor.next() );
			final ImagePlus newTileImagePlus = newTileImg.getImagePlus();
			Utils.workaroundImagePlusNSlices( newTileImagePlus );

			final String newTileImageFilename = Utils.addFilenameSuffix( PathResolver.getFileName( tile.getFilePath() ), "_retiled-" + newTilesInSingleTile.size() + AxisMapping.getAxisStr( args.retileDimension ) ) + ".tif";
			final String newTileImagePath = PathResolver.get( targetImagesLocation, newTileImageFilename );
			targetDataProvider.saveImage( newTileImagePlus, newTileImagePath );

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
		return determineNewTileSize( originalTileSize, args.retileDimension, args.retileSize );
	}

	public static long[] determineNewTileSize( final long[] originalTileSize, final int retileDimension )
	{
		return determineNewTileSize( originalTileSize, retileDimension, null );
	}

	public static long[] determineNewTileSize( final long[] originalTileSize, final int retileDimension, final Integer retileSize )
	{
		final long[] newTileSize = originalTileSize.clone();
		if ( retileSize != null )
		{
			newTileSize[ retileDimension ] = retileSize;
		}
		else
		{
			int tileSizeSumExcludingRetileDimension = 0;
			for ( int d = 0; d < originalTileSize.length; ++d )
				if ( d != retileDimension )
					tileSizeSumExcludingRetileDimension += originalTileSize[ d ];
			newTileSize[ retileDimension ] = Math.round( ( double ) tileSizeSumExcludingRetileDimension / ( newTileSize.length - 1 ) );
		}
		return newTileSize;
	}

	private List< Interval > getNewTilesIntervalsInSingleTile( final long[] originalTileSize, final long[] newTileSize )
	{
		return getNewTilesIntervalsInSingleTile(
				originalTileSize,
				newTileSize,
				args.retileDimension,
				args.minOverlapRatioEachSide,
				logWriter
			);
	}

	public static List< Interval > getNewTilesIntervalsInSingleTile(
			final long[] originalTileSize,
			final long[] newTileSize,
			final int retileDimension,
			final double minOverlapRatioEachSide )
	{
		return getNewTilesIntervalsInSingleTile( originalTileSize, newTileSize, retileDimension, minOverlapRatioEachSide, null );
	}

	public static List< Interval > getNewTilesIntervalsInSingleTile(
			final long[] originalTileSize,
			final long[] newTileSize,
			final int retileDimension,
			final double minOverlapRatioEachSide,
			final PrintWriter logWriter )
	{
		for ( int d = 0; d < Math.max( originalTileSize.length, newTileSize.length ); ++d )
			if ( d != retileDimension && originalTileSize[ d ] != newTileSize[ d ] )
				throw new RuntimeException( "tile size is different in any dimensions other than retile dimension: originalTileSize=" + Arrays.toString( originalTileSize ) + ", newTileSize=" + Arrays.toString( newTileSize ) );

		// fit in new tiles such that they exactly cover the original tile while the overlap is as close as possible to the requrested min ratio
		final long minOverlapEachSide = ( long ) Math.ceil( newTileSize[ retileDimension ] * minOverlapRatioEachSide );
		final int numNewTiles = ( int ) Math.ceil( ( double ) ( originalTileSize[ retileDimension ] - minOverlapEachSide ) / ( newTileSize[ retileDimension ] - minOverlapEachSide ) );
		final long newTotalSize = numNewTiles * ( newTileSize[ retileDimension ] - minOverlapEachSide ) + minOverlapEachSide;
		final long extraOverlap = newTotalSize - originalTileSize[ retileDimension ];
		final long extraOverlapPerIntersection = numNewTiles <= 1 ? 0 : extraOverlap / ( numNewTiles - 1 );
		final int numNewTilesWithExtraOnePixelOverlap = ( int ) ( extraOverlap - extraOverlapPerIntersection * ( numNewTiles - 1 ) );

		final List< Interval > newTilesIntervals = new ArrayList<>();
		for ( int i = 0; i < numNewTiles; ++i )
		{
			final long[] min = new long[ newTileSize.length ], max = Intervals.maxAsLongArray( new FinalInterval( originalTileSize ) );
			min[ retileDimension ] = i * ( newTileSize[ retileDimension ] - minOverlapEachSide - extraOverlapPerIntersection ) - Math.min( i, numNewTilesWithExtraOnePixelOverlap );
			max[ retileDimension ] = min[ retileDimension ] + newTileSize[ retileDimension ] - 1;
			newTilesIntervals.add( new FinalInterval( min, max ) );
		}

		if ( logWriter != null )
		{
			logWriter.println( "Original tile size: " + Arrays.toString( originalTileSize ) );
			logWriter.println( "New tile size: " + Arrays.toString( newTileSize ) );
			logWriter.println( "Retile dimension: " + retileDimension );
			logWriter.println( "Min overlap ratio (each side): " + minOverlapRatioEachSide );
			logWriter.println();
			logWriter.println( "Number of new tiles: " + numNewTiles );
			logWriter.println( "Min overlap (each side): " + minOverlapEachSide );
			logWriter.println( "Extra overlap per intersection: " + extraOverlapPerIntersection );
			logWriter.println( "Number of new tiles with extra 1px overlap: " + numNewTilesWithExtraOnePixelOverlap );
		}

		return newTilesIntervals;
	}

	@Override
	public void close() throws Exception
	{
		sparkContext.close();
		logWriter.close();
	}
}
