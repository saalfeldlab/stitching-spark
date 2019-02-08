package org.janelia.stitching;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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

import ij.ImagePlus;

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
				usage = "File name pattern for tile images. Tiles are supposed to have their number in the filename (starting from 0), provide it as %d in the filename pattern.")
		private String filenamePattern;

		@Option(name = "-r", aliases = { "--pixelResolution" }, required = true,
				usage = "Physical pixel resolution in mm (comma-separated, for example, '0.097,0.097,0.18').")
		private String pixelResolutionStr;

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

		run(
				parsedArgs.metadataFilepath,
				parsedArgs.basePath,
				parsedArgs.filenamePattern,
				CmdUtils.parseDoubleArray( parsedArgs.pixelResolutionStr ),
				parsedArgs.axisMappingStr.trim().split( "," )
			);
	}

	public static void run(
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

		final String baseOutputFolder = Paths.get( metadataFilepath ).getParent().toString();

		final SAXBuilder sax = new SAXBuilder();
		final Document doc = sax.build( metadataFilepath );
		final Element rootElement = doc.getRootElement();

		final Element tileListElement = rootElement.getChild(TILE_LIST_TAG);

		final int numTiles = tileListElement.getChildren().size();
		final List< TileInfo > tiles = new ArrayList<>();

		// all channels are contained in the same .czi file for each tile, so the parser generates only one tile configuration file, and then CZI->N5 converter will split into channels
		for ( int i = 0; i < numTiles; ++i )
		{
			final Element tileElement = tileListElement.getChild( String.format( TILE_TAG_PATTERN, i ) );
			final String filepath = PathResolver.get( basePath, String.format( filenamePattern, i ) );

			final double[] objCoords = new double[] {
					Double.parseDouble( tileElement.getAttributeValue( TILE_OBJECTIVE_X_POSITION_TAG ) ),
					Double.parseDouble( tileElement.getAttributeValue( TILE_OBJECTIVE_Y_POSITION_TAG ) ),
					Double.parseDouble( tileElement.getAttributeValue( TILE_OBJECTIVE_Z_POSITION_TAG ) )
				};

			final double[] pixelCoords = new double[ objCoords.length ];
			for ( int d = 0; d < pixelCoords.length; ++d )
				pixelCoords[ d ] = objCoords[ axisMapping.axisMapping[ d ] ] / pixelResolution[ axisMapping.axisMapping[ d ] ] * ( axisMapping.flip[ d ] ? -1 : 1 );

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
		final ImagePlus[] imps = ImageImporter.openBioformatsImageSeries( tiles.iterator().next().getFilePath() );
		final ImageType type = ImageType.valueOf( imps[ 0 ].getType() );
		for ( final TileInfo tile : tiles )
			tile.setType( type );

		System.out.println( "Parsed metadata for " + tiles.size() + " tiles" );

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
		final String jsonTileConfigurationFilename = PathResolver.getFileName( metadataFilepath ).substring( 0, PathResolver.getFileName( metadataFilepath ).lastIndexOf( "." ) ) + ".json";
		dataProvider.saveTiles( tiles.toArray( new TileInfo[ 0 ] ), PathResolver.get( baseOutputFolder, jsonTileConfigurationFilename ) );

		System.out.println( "Done" );
	}
}
