package org.janelia.stitching;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.janelia.stitching.analysis.CheckConnectedGraphs;

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

		final String fileNamePattern = "^.*?_ch(\\d?)_.*?_(\\d{3}x_\\d{3}y_\\d{3}z)_.*?\\.tif$";
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

				final int ch = Integer.parseInt( filename.replaceAll( fileNamePattern, "$1" ) );

				if ( !tiles.containsKey( ch ) )
					tiles.put( ch, new ArrayList<>() );

				final TileInfo tile = new TileInfo( 3 );
				tile.setIndex( tiles.get( ch ).size() );
				tile.setFilePath( filepath );
				tile.setPosition( objCoords );
				tile.setSize( null );
				tile.setPixelResolution( pixelResolution.clone() );
				tiles.get( ch ).add( tile );
			}
		}

		System.out.println( "Parsed from ImageList.csv:" );
		for ( final Entry< Integer, List< TileInfo > > entry : tiles.entrySet() )
			System.out.println( String.format( "  ch%d: %d tiles", entry.getKey(), entry.getValue().size() ) );

		// run metadata step
		final List< TileInfo[] > tileChannels = new ArrayList<>();
		for ( final List< TileInfo > tilesList : tiles.values() )
			tileChannels.add( tilesList.toArray( new TileInfo[ 0 ] ) );

		PipelineMetadataStepExecutor.process( tileChannels );

		// check that tile configuration forms a single graph
		// NOTE: may fail with StackOverflowError. Pass -Xss to the JVM
		final List< TilePair > overlappingPairs = TileOperations.findOverlappingTiles( tileChannels.get( 0 ) );
		final List< Integer > connectedComponentsSize = CheckConnectedGraphs.connectedComponentsSize( overlappingPairs );
		if ( connectedComponentsSize.size() > 1 )
			throw new Exception( "Expected single graph, got several components of size " + connectedComponentsSize );

		// finally save the configurations as JSON files
		for ( int channel = 0; channel < tileChannels.size(); ++channel )
			TileInfoJSONProvider.saveTilesConfiguration( tileChannels.get( channel ), baseOutputFolder + "/ch" + channel + ".json" );
	}
}
