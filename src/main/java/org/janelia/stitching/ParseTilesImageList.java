package org.janelia.stitching;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
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
import org.janelia.stitching.analysis.CheckConnectedGraphs;
import org.janelia.stitching.analysis.FilterAdjacentShifts;

public class ParseTilesImageList
{
	public static void main( final String[] args ) throws Exception
	{
		final String imageListFilepath = args[ 0 ];

		final double[] pixelResolution;
		if ( args.length > 1 )
		{
			final String voxelDimensions = args[ 1 ];
			final String[] tokens = voxelDimensions.trim().split( "," );
			pixelResolution = new double[ tokens.length ];
			for ( int i = 0; i < pixelResolution.length; i++ )
				pixelResolution[ i ] = Double.parseDouble( tokens[ i ] );
		}
		else
		{
			pixelResolution = new double[] { 0.097, 0.097, 0.18 };
		}
		System.out.println( "Pixel resolution: " + Arrays.toString( pixelResolution ) );

		final String fileNamePattern = "^.*?_(\\d{3})nm_.*?_(\\d{3}x_\\d{3}y_\\d{3}z)_.*?\\.tif$";
		final String baseOutputFolder = Paths.get( imageListFilepath ).getParent().toString();

		final TreeMap< Integer, List< TileInfo > > tiles = new TreeMap<>();

		try ( final BufferedReader imageListReader = new BufferedReader( new FileReader( imageListFilepath ) ) )
		{
			String line = imageListReader.readLine();	//!< headers

			for ( line = imageListReader.readLine(); line != null; line = imageListReader.readLine() )
			{
				final String[] columns = line.split( "," );

				final String filepath = columns[ 0 ];
				final String filename = Paths.get( filepath ).getFileName().toString();

				// swap X and Y
				final double[] objCoords = new double[] {
						Double.parseDouble( columns[ 6 ] ),
						Double.parseDouble( columns[ 5 ] ),
						Double.parseDouble( columns[ 7 ] )
					};

				// flip X
				objCoords[ 0 ] *= -1;

				for ( int d = 0; d < objCoords.length; ++d )
					objCoords[ d ] /= pixelResolution[ d ];

				final int nm = Integer.parseInt( filename.replaceAll( fileNamePattern, "$1" ) );

				if ( !tiles.containsKey( nm ) )
					tiles.put( nm, new ArrayList<>() );

				final TileInfo tile = new TileInfo( 3 );
				tile.setIndex( tiles.get( nm ).size() );
				tile.setFilePath( filepath );
				tile.setStagePosition( objCoords );
				tile.setSize( null );
				tile.setPixelResolution( pixelResolution.clone() );
				tiles.get( nm ).add( tile );
			}
		}

		System.out.println( "Parsed from " + Paths.get( imageListFilepath ).getFileName() + ":" );
		for ( final Entry< Integer, List< TileInfo > > entry : tiles.entrySet() )
			System.out.println( String.format( "  ch%d: %d tiles", entry.getKey(), entry.getValue().size() ) );

		// run metadata step
		PipelineMetadataStepExecutor.process( tiles );

		// check that tile configuration forms a single graph
		// NOTE: may fail with StackOverflowError. Pass -Xss to the JVM
		final List< TilePair > overlappingPairs = TileOperations.findOverlappingTiles( tiles.firstEntry().getValue().toArray( new TileInfo[ 0 ] ) );
		final List< Integer > connectedComponentsSize = CheckConnectedGraphs.connectedComponentsSize( overlappingPairs );
		if ( connectedComponentsSize.size() > 1 )
			throw new Exception( "Expected single graph, got several components of size " + connectedComponentsSize );

		// trace overlaps to ensure that tile positions are accurate
		System.out.println();
		final long[] tileSize = tiles.firstEntry().getValue().get( 0 ).getSize();
		System.out.println( "tile size = " + Arrays.toString( tileSize ) );
		for ( int d = 0; d < tiles.firstEntry().getValue().get( 0 ).numDimensions(); ++d )
		{
			final List< TilePair > adjacentPairsDim = FilterAdjacentShifts.filterAdjacentPairs( overlappingPairs, d );

			if ( !adjacentPairsDim.isEmpty() )
			{
				double minOverlap = Double.POSITIVE_INFINITY, maxOverlap = Double.NEGATIVE_INFINITY, avgOverlap = 0;
				for ( final TilePair pair : adjacentPairsDim )
				{
					final double overlap = tileSize[ d ] - Math.abs( pair.getB().getStagePosition( d ) - pair.getA().getStagePosition( d ) );
					minOverlap = Math.min( overlap, minOverlap );
					maxOverlap = Math.max( overlap, maxOverlap );
					avgOverlap += overlap;
				}
				avgOverlap /= adjacentPairsDim.size();
				System.out.println( String.format( "Overlaps for [%c] adjacent pairs: min=%d, max=%d, avg=%d",
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
			TileInfoJSONProvider.saveTilesConfiguration( tiles.get( channel ).toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter( URI.create( Paths.get( baseOutputFolder, channel + "nm.json" ).toString() ) ) );

		System.out.println( "Done" );
	}
}
