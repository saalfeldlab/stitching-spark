package org.janelia.stitching;

import ij.ImagePlus;
import net.imglib2.FinalInterval;
import net.imglib2.util.Intervals;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.CloudURI;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.spark.util.CmdUtils;
import org.janelia.stitching.analysis.CheckConnectedGraphs;
import org.janelia.stitching.analysis.FilterAdjacentShifts;
import org.janelia.util.ImageImporter;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ParseCZITilesMetadata
{
	private static class ParseCZITilesMetadataCmdArgs implements Serializable
	{
		private static final long serialVersionUID = -8042404946489264151L;

		@Option(name = "-i", aliases = { "--input" }, required = true,
				usage = "Path to the .mvl metadata file.")
		private String metadataFilepath;

		@Option(name = "-b", aliases = { "--basePath" }, required = true,
				usage = "Path to the base folder containing input tile images.")
		private String basePath;

		@Option(name = "-f", aliases = { "--filenamePattern" }, required = true,
				usage = "File name pattern for tile images. Tiles are supposed to have their number in the filename (starting from 0), provide it as %d in the filename pattern. " +
						"If the filenames contain leading zeroes, the format can be specified, for example, as %02d for a fixed length of two.")
		private String filenamePattern;

		@Option(name = "-r", aliases = { "--pixelResolution" }, required = true,
				usage = "Physical pixel resolution in mm (comma-separated, for example, '0.097,0.097,0.18').")
		private String pixelResolutionStr;

		@Option(name = "-o", aliases = { "--outputPath" }, required = false,
				usage = "Path to the base folder containing output. If this is not provided, the parent directory of the --input file will be used for output.")
		private String outputPath;

		@Option(name = "-a", aliases = { "--axes" }, required = false,
				usage = "Axis mapping for the objective->pixel coordinates conversion (comma-separated axis specification with optional flips, for example, '-x,y,z').")
		private String axisMappingStr = "-x,y,z";

		private boolean parsedSuccessfully = false;

		public ParseCZITilesMetadataCmdArgs( final String... args )
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}

			// make sure that metadataFilepath is an absolute file path if it's pointing to a filesystem location
			if ( !CloudURI.isCloudURI( metadataFilepath ) )
				metadataFilepath = Paths.get( metadataFilepath ).toAbsolutePath().toString();

			// make sure that basePath is an absolute file path if it's pointing to a filesystem location
			if ( basePath != null && !CloudURI.isCloudURI( basePath ) )
				basePath = Paths.get( basePath ).toAbsolutePath().toString();
		}
	}

	private static final String TILE_LIST_TAG = "Data";
	private static final String TILE_TAG_PATTERN = "Entry%d";

	private static final String TILE_OBJECTIVE_X_POSITION_TAG = "PositionX";
	private static final String TILE_OBJECTIVE_Y_POSITION_TAG = "PositionY";
	private static final String TILE_OBJECTIVE_Z_POSITION_TAG = "PositionZ";

	private static final String TILE_SIZE_X_TAG = "AcquisitionFrameWidth";
	private static final String TILE_SIZE_Y_TAG = "AcquisitionFrameHeight";
	private static final String TILE_SIZE_Z_TAG = "StackSlices";

	public static void main( final String[] args ) throws Exception
	{
		final ParseCZITilesMetadataCmdArgs parsedArgs = new ParseCZITilesMetadataCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			throw new IllegalArgumentException( "argument format mismatch" );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf().setAppName( "ParseCZITilesMetadata" ) ) )
		{
			// even though Spark is not used in this step, AWS requires to initialize SparkContext if running in the cloud
		}

		run(
				parsedArgs.outputPath,
				parsedArgs.metadataFilepath,
				parsedArgs.basePath,
				parsedArgs.filenamePattern,
				CmdUtils.parseDoubleArray( parsedArgs.pixelResolutionStr ),
				parsedArgs.axisMappingStr.trim().split( "," )
			);
	}

	public static void run(
			final String outputFilepath,
			final String metadataFilepath,
			final String basePath,
			final String filenamePattern,
			final double[] pixelResolution,
			final String[] axisMappingStr ) throws Exception
	{
		if ( pixelResolution.length != 3 || axisMappingStr.length != 3 )
			throw new IllegalArgumentException( "expected pixelResolution and axisMapping arrays of length 3" );

		final AxisMapping axisMapping = new AxisMapping( axisMappingStr );

		System.out.println( "Pixel resolution: " + Arrays.toString( pixelResolution ) );
		System.out.println( "Axis mapping: " + Arrays.toString( axisMappingStr ) );

		final String baseOutputFolder = StringUtils.isBlank(outputFilepath) ?
				Paths.get( metadataFilepath ).getParent().toString() : outputFilepath;

		final SAXBuilder sax = new SAXBuilder();
		final Document doc = sax.build( metadataFilepath );
		final Element rootElement = doc.getRootElement();

		final Element tileListElement = rootElement.getChild(TILE_LIST_TAG);

		final int numTiles = tileListElement.getChildren().size();
		final List< TileInfo > tiles = new ArrayList<>();

		// all channels are contained in the same .czi file for each tile, so the parser generates only one tile configuration file, and then CZI->N5 converter will split into channels

		// find starting index of the first tile in the filenames (could be either 0-indexed or 1-indexed, or can be stored in a single .czi container)
		final List< String > tileFilenames = new ArrayList<>();
		for ( final int filenameStartIndex : new int[] { 0, 1 } )
		{
			for ( int i = 0; i < numTiles; ++i )
				tileFilenames.add( String.format( filenamePattern, i + filenameStartIndex ) );

			if ( numTiles > 1 && new HashSet<>( tileFilenames ).size() == 1 ) {
				// Check if all tiles are stored in a single czi container.
				break;
			}

			boolean allTilesExist = true;
			for ( final String tileFilename : tileFilenames ) {
				if ( !Files.exists( Paths.get( PathResolver.get( basePath, tileFilename ) ) ) ) {
					allTilesExist = false;
					break;
				}
			}

			if (allTilesExist) // check if found the right indexing scheme
				break;

			tileFilenames.clear();
		}

		if ( tileFilenames.isEmpty() )
			throw new RuntimeException( "Could not find correct file paths to all tile images" );

		for ( int i = 0; i < numTiles; ++i )
		{
			final Element tileElement = tileListElement.getChild( String.format( TILE_TAG_PATTERN, i ) );
			final String filepath = PathResolver.get( basePath, tileFilenames.get( i ) );

			final double[] objCoords = new double[] {
					Double.parseDouble( tileElement.getAttributeValue( TILE_OBJECTIVE_X_POSITION_TAG ) ),
					Double.parseDouble( tileElement.getAttributeValue( TILE_OBJECTIVE_Y_POSITION_TAG ) ),
					Double.parseDouble( tileElement.getAttributeValue( TILE_OBJECTIVE_Z_POSITION_TAG ) )
				};

			final double[] pixelCoords = new double[ objCoords.length ];
			for ( int d = 0; d < pixelCoords.length; ++d )
				pixelCoords[ d ] = objCoords[ axisMapping.axisMapping[ d ] ] / pixelResolution[ d ] * ( axisMapping.flip[ d ] ? -1 : 1 );

			final long[] tileSize = new long[] {
					Long.parseLong( tileElement.getAttributeValue( TILE_SIZE_X_TAG ) ),
					Long.parseLong( tileElement.getAttributeValue( TILE_SIZE_Y_TAG ) ),
					Long.parseLong( tileElement.getAttributeValue( TILE_SIZE_Z_TAG ) )
				};

			final TileInfo tile = new TileInfo( 3 );
			tile.setIndex( i );
			tile.setFilePath( filepath );
			tile.setPosition( pixelCoords );
			tile.setSize( tileSize );
			tile.setPixelResolution( pixelResolution.clone() );

			tiles.add( tile );
		}

		// check that tile configuration forms a single graph
		// NOTE: may fail with StackOverflowError. Pass -Xss to the JVM
		final TileInfo[] tileInfos = tiles.toArray( new TileInfo[ 0 ] );
		final List< Set< TileInfo > > connectedComponents = CheckConnectedGraphs.connectedComponents( tileInfos );
		if ( connectedComponents.size() > 1 )
		{
			Set< TileInfo > smallestComponent = null;
			for ( final Set< TileInfo > component : connectedComponents )
				if ( smallestComponent == null || component.size() < smallestComponent.size() )
					smallestComponent = component;

			final Set< Integer > tileIndexesInSmallestComponent = new TreeSet<>();
			for ( final TileInfo tile : smallestComponent )
				tileIndexesInSmallestComponent.add( tile.getIndex() );

			throw new Exception(
					"Expected single graph, got several components of size " + CheckConnectedGraphs.connectedComponentsSize( connectedComponents ) + System.lineSeparator() +
					"Tiles in the smallest component: " + tileIndexesInSmallestComponent
				);
		}

		// determine data type by reading metadata of the first tile image
		System.out.println("Opening image...");
		long elapsedMsec = System.currentTimeMillis();
		final ImagePlus[] imps = ImageImporter.openBioformatsImageSeries( tiles.iterator().next().getFilePath() );
		elapsedMsec = System.currentTimeMillis() - elapsedMsec;
		System.out.println("Opened, took " + (elapsedMsec / 1000) + "s");

		final ImageType type = ImageType.valueOf( imps[ 0 ].getType() );
		for ( final TileInfo tile : tiles )
			tile.setType( type );

		System.out.println( "Parsed metadata for " + tiles.size() + " tiles" );

		for ( final ImagePlus imp : imps )
		{
			final long[] actualImageSize = { imp.getWidth(), imp.getHeight(), imp.getNSlices() };
			if (!Intervals.equals(new FinalInterval(tiles.get(0).getSize()), new FinalInterval(actualImageSize)))
			{
				throw new RuntimeException("Tile size from metadata doesn't match actual image size: " +
						"metadata=" + Arrays.toString(tiles.get(0).getSize()) + ", actual=" + Arrays.toString(actualImageSize));
			}
		}

		// trace overlaps to ensure that tile positions are accurate
		System.out.println();
		final long[] tileSize = tiles.get( 0 ).getSize();
		System.out.println( "tile size in px = " + Arrays.toString( tileSize ) );
		final List< TilePair > overlappingPairs = TileOperations.findOverlappingTiles( tileInfos );
		for ( int d = 0; d < tiles.get( 0 ).numDimensions(); ++d )
		{
			final List< TilePair > adjacentPairsDim = FilterAdjacentShifts.filterAdjacentPairs( overlappingPairs, d );

			if ( !adjacentPairsDim.isEmpty() )
			{
				double minOverlap = Double.POSITIVE_INFINITY, maxOverlap = Double.NEGATIVE_INFINITY, avgOverlap = 0;
				for ( final TilePair pair : adjacentPairsDim )
				{
					final double overlap = tileSize[ d ] - Math.abs( pair.getB().getPosition( d ) - pair.getA().getPosition( d ) );
					minOverlap = Math.min( overlap, minOverlap );
					maxOverlap = Math.max( overlap, maxOverlap );
					avgOverlap += overlap;
				}
				avgOverlap /= adjacentPairsDim.size();
				System.out.println( String.format( "Overlaps for [%c] adjacent pairs: min=%d px, max=%d px, avg=%d px",
						new char[] { 'x', 'y', 'z' }[ d ],
						Math.round( minOverlap ),
						Math.round( maxOverlap ),
						Math.round( avgOverlap )
					) + " (" + Math.round( avgOverlap / tileSize[ d ] * 100 ) + "% of tile size)" );
			}
			else
			{
				System.out.println( String.format( "No overlapping adjacent pairs in [%c]", new char[] { 'x', 'y', 'z' }[ d ] ) );
			}
		}
		System.out.println();

		// save the configuration as a JSON file
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final String jsonTileConfigurationFilename = "tiles.json";
		dataProvider.saveTiles( tiles.toArray( new TileInfo[ 0 ] ), PathResolver.get( baseOutputFolder, jsonTileConfigurationFilename ) );

		// Now that we know the number of channels in the CZI file, prepare for the next step
		// and create the N5 container and datasets for conversion
		final int numChannels = imps[ 0 ].getNChannels();
		System.out.println("There are " + numChannels + " channels in the CZI image file(s)");
		ConvertCZITilesToN5Spark.createTargetDirectories(
				PathResolver.get( baseOutputFolder, ConvertCZITilesToN5Spark.tilesN5ContainerName ),
				numChannels
		);

		System.out.println( "Done" );
	}
}
