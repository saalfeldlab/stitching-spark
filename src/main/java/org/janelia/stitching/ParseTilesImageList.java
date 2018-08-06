package org.janelia.stitching;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.PipelineMetadataStepExecutor.NonExistingTilesException;
import org.janelia.stitching.analysis.CheckConnectedGraphs;
import org.janelia.stitching.analysis.FilterAdjacentShifts;
import org.janelia.util.Conversions;

public class ParseTilesImageList
{
	private static final int NUM_COLUMNS_FULL = 8; // filepath,filename,stageX,stageY,stageZ,objectiveX,objectiveY,objectiveZ

	public static void main( final String[] args ) throws Exception
	{
		final String imageListFilepath;
		final String tileImagesFolder;
		final double[] pixelResolution;
		final String[] axisMappingStr;
		final boolean skipNonExistingTiles;

		try
		{
			imageListFilepath = Paths.get( args[ 0 ] ).toAbsolutePath().toString();
			tileImagesFolder = args[ 1 ];
			pixelResolution = Conversions.parseDoubleArray( args[ 2 ].trim().split( "," ) );
			axisMappingStr = args[ 3 ].trim().split( "," );
			skipNonExistingTiles = args.length > 4 && args[ 4 ].equalsIgnoreCase( "--skip" );
		}
		catch ( final Exception e )
		{
			System.err.println(
					"Usage:" + System.lineSeparator() +
					"python parse-imagelist.py " + System.lineSeparator() +
					"  <path to ImageList.csv> " + System.lineSeparator() +
					"  <path to tile images directory> " + System.lineSeparator() +
					"  <pixel resolution, e.g. 0.097,0.097,0.18> " + System.lineSeparator() +
					"  <objective to pixel coordinates axis mapping, e.g. -y,x,z>" + System.lineSeparator() +
					"  [--skip, optional for skipping tiles that do not exist instead of failing]"
				);
			System.exit( 1 );
			return; // avoid initializing all final variables as null
		}

		if ( pixelResolution.length != 3 && axisMappingStr.length != 3 )
			throw new IllegalArgumentException( "expected three-dimensional pixelResolution and axisMapping" );

		final AxisMapping axisMapping = new AxisMapping( axisMappingStr );

		System.out.println( "Pixel resolution: " + Arrays.toString( pixelResolution ) );
		System.out.println( "Axis mapping: " + Arrays.toString( axisMappingStr ) );

		final String fileNamePattern = "^.*?_(\\d{3})nm_.*?_(\\d{3}x_\\d{3}y_\\d{3}z)_.*?\\.tif$";
		final String baseOutputFolder = Paths.get( imageListFilepath ).getParent().toString();

		final TreeMap< Integer, List< TileInfo > > tiles = new TreeMap<>();

		try ( final BufferedReader imageListReader = new BufferedReader( new FileReader( imageListFilepath ) ) )
		{
			String line = imageListReader.readLine();	//!< headers

			for ( line = imageListReader.readLine(); line != null; line = imageListReader.readLine() )
			{
				final String[] columns = line.split( "," );

				final String filepath;
				if ( tileImagesFolder == null || tileImagesFolder.isEmpty() )
				{
					// base images dir is not provided, use the filepath column
					filepath = columns[ 0 ];
				}
				else if ( columns.length >= NUM_COLUMNS_FULL )
				{
					// all columns are present, join base images dir and the filename column
					filepath = Paths.get( tileImagesFolder, columns[ 1 ] ).toString();
				}
				else
				{
					// workaround when the filename column is possibly omitted, join base images dir and filename obtained from the filepath column
					filepath = Paths.get( tileImagesFolder, Paths.get( columns[ 0 ] ).getFileName().toString() ).toString();
				}

				final String filename = Paths.get( filepath ).getFileName().toString();

				// get objective coordinates from the last three columns
				// FIXME: Utils.getTileCoordinates() still returns y/x/z grid coordinates. Instead, it needs to account for the axis mapping that is used here
				final double[] objCoords = new double[] {
						Double.parseDouble( columns[ columns.length - 3 ] ),
						Double.parseDouble( columns[ columns.length - 2 ] ),
						Double.parseDouble( columns[ columns.length - 1 ] )
					};

				final double[] pixelCoords = new double[ objCoords.length ];
				for ( int d = 0; d < pixelCoords.length; ++d )
					pixelCoords[ d ] = objCoords[ axisMapping.axisMapping[ d ] ] / pixelResolution[ axisMapping.axisMapping[ d ] ] * ( axisMapping.flip[ d ] ? -1 : 1 );

				final int nm = Integer.parseInt( filename.replaceAll( fileNamePattern, "$1" ) );

				if ( !tiles.containsKey( nm ) )
					tiles.put( nm, new ArrayList<>() );

				final TileInfo tile = new TileInfo( 3 );
				tile.setIndex( tiles.get( nm ).size() );
				tile.setFilePath( filepath );
				tile.setPosition( pixelCoords );
				tile.setSize( null );
				tile.setPixelResolution( pixelResolution.clone() );
				tiles.get( nm ).add( tile );
			}
		}

		System.out.println( "Parsed from " + Paths.get( imageListFilepath ).getFileName() + ":" );
		for ( final Entry< Integer, List< TileInfo > > entry : tiles.entrySet() )
			System.out.println( String.format( "  ch%d: %d tiles", entry.getKey(), entry.getValue().size() ) );

		// run metadata step
		try
		{
			PipelineMetadataStepExecutor.process( tiles, skipNonExistingTiles );
		}
		catch ( final NonExistingTilesException e )
		{
			System.exit( 1 );
		}

		// check that tile configuration forms a single graph
		// NOTE: may fail with StackOverflowError. Pass -Xss to the JVM
		final TileInfo[] tileInfos = tiles.firstEntry().getValue().toArray( new TileInfo[ 0 ] );
		final List< Integer > connectedComponentsSize = CheckConnectedGraphs.connectedComponentsSize( tileInfos );
		if ( connectedComponentsSize.size() > 1 )
			throw new Exception( "Expected single graph, got several components of size " + connectedComponentsSize );

		// trace overlaps to ensure that tile positions are accurate
		System.out.println();
		final long[] tileSize = tiles.firstEntry().getValue().get( 0 ).getSize();
		System.out.println( "tile size in px = " + Arrays.toString( tileSize ) );
		final List< TilePair > overlappingPairs = TileOperations.findOverlappingTiles( tileInfos );
		for ( int d = 0; d < tiles.firstEntry().getValue().get( 0 ).numDimensions(); ++d )
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

			// verify against grid coordinates
			final Set< TilePair > adjacentPairsDimSet = new HashSet<>( adjacentPairsDim );
			for ( final TilePair pair : overlappingPairs )
			{
				final TreeMap< Integer, Integer > diff = new TreeMap<>();
				for ( int k = 0; k < Math.max( pair.getA().numDimensions(), pair.getB().numDimensions() ); ++k )
					if ( Utils.getTileCoordinates( pair.getB() )[ k ] != Utils.getTileCoordinates( pair.getA() )[ k ] )
						diff.put( k, Utils.getTileCoordinates( pair.getB() )[ k ] - Utils.getTileCoordinates( pair.getA() )[ k ] );
				if ( diff.size() == 1 && diff.firstKey().intValue() == d && Math.abs( diff.firstEntry().getValue().intValue() ) == 1 )
				{
					if ( !adjacentPairsDimSet.contains( pair ) )
						throw new RuntimeException( "adjacent pairs don't match with grid coordinates" );
					adjacentPairsDimSet.remove( pair );
				}
			}
			if ( !adjacentPairsDimSet.isEmpty() )
				throw new RuntimeException( "some extra pairs have been added that are not adjacent in the grid" );
		}
		System.out.println();

		// finally save the configurations as JSON files
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		for ( final int channel : tiles.keySet() )
			TileInfoJSONProvider.saveTilesConfiguration( tiles.get( channel ).toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter( Paths.get( baseOutputFolder, channel + "nm.json" ).toString() ) );

		System.out.println( "Done" );
	}
}
