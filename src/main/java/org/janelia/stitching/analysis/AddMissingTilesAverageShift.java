package org.janelia.stitching.analysis;

import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.Utils;

public class AddMissingTilesAverageShift
{
	static Map< Integer, int[] > coordinates;
	static void parseCoordinates( final Collection< TileInfo > tiles ) throws Exception
	{
		coordinates = new TreeMap<>();
		final String coordsPatternStr = ".*(\\d{3})x_(\\d{3})y_(\\d{3})z.*";
		final Pattern coordsPattern = Pattern.compile( coordsPatternStr );
		Matcher matcher = null;
		for ( final TileInfo tile : tiles )
		{
			matcher = coordsPattern.matcher( Paths.get( tile.getFilePath() ).getFileName().toString() );
			if ( !matcher.find() )
				throw new Exception( "Can't parse coordinates string" );

			coordinates.put( tile.getIndex(), new int[] { Integer.parseInt( matcher.group( 2 ) ), Integer.parseInt( matcher.group( 1 ) ), Integer.parseInt( matcher.group( 3 ) ) } );
		}
	}

	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		// Read inputs
		final TreeMap< Integer, TileInfo > tilesInfoOriginal = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) ) );
		final TreeMap< Integer, TileInfo > tilesInfoFinal = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) ) );

		parseCoordinates( tilesInfoOriginal.values() );

		// Translate original tiles set into the relative coordinate space of any tile that is present in the final set
		final int pivotTile = tilesInfoFinal.firstKey();
		final double[] offset = new double[ tilesInfoFinal.get( pivotTile ).numDimensions() ];
		for ( int d = 0; d < offset.length; d++ )
			offset[ d ] = tilesInfoFinal.get( pivotTile ).getPosition( d ) - tilesInfoOriginal.get( pivotTile ).getPosition( d );
		TileOperations.translateTiles( tilesInfoOriginal.values().toArray( new TileInfo[0] ), offset );

		// Find missing tiles
		final TreeMap< Integer, TileInfo > missingTilesInfo = new TreeMap<>( tilesInfoOriginal );
		missingTilesInfo.keySet().removeAll( tilesInfoFinal.keySet() );

		System.out.println( "Number of missing tiles: " + missingTilesInfo.size() );

		// Start adding missing tiles one by one
		for ( final TileInfo tileToAdd : missingTilesInfo.values() )
		{
			final TreeMap< Integer, List< TileInfo > > tilesAround = new TreeMap<>();
			for ( final TileInfo tile : tilesInfoFinal.values() )
			{
				int maxStepDiff = 0;
				for ( int d = 0; d < tile.numDimensions(); d++ )
					maxStepDiff = Math.max( Math.abs( coordinates.get( tile.getIndex() )[ d ] - coordinates.get( tileToAdd.getIndex() )[ d ] ), maxStepDiff );

				if ( !tilesAround.containsKey( maxStepDiff ) )
					tilesAround.put( maxStepDiff, new ArrayList<>() );
				tilesAround.get( maxStepDiff ).add( tile );
			}

			final double[] estimatedShift = new double[ tileToAdd.numDimensions() ];
			int level = 0, count = 0;
			for ( final Entry< Integer, List< TileInfo > > entry : tilesAround.entrySet() )
			{
				for ( final TileInfo tile : entry.getValue() )
					for ( int d = 0; d < estimatedShift.length; d++ )
						estimatedShift[ d ] += tilesInfoFinal.get( tile.getIndex() ).getPosition( d ) - tilesInfoOriginal.get( tile.getIndex() ).getPosition( d );

				level = entry.getKey();
				count += entry.getValue().size();

				if ( count > 1 )
					break;
			}

			for ( int d = 0; d < estimatedShift.length; d++ )
				estimatedShift[ d ] /= count;

			//System.out.println( "level: " + (level==tilesAround.firstKey().intValue() ? level+"     " : Arrays.toString(new int[] {tilesAround.firstKey().intValue(), level} ))  + "   " + count + " neighbors,  " + (count<10?" ":"") + Arrays.toString(estimatedShift) );

			for ( int d = 0; d < estimatedShift.length; d++ )
				tileToAdd.setPosition( d, tileToAdd.getPosition(d) + estimatedShift[ d ] );
		}

		tilesInfoFinal.putAll( missingTilesInfo );
		System.out.println( tilesInfoFinal.size() == tilesInfoOriginal.size() ? "Size OK" : "Size mismatch" );

		TileOperations.translateTilesToOrigin( tilesInfoFinal.values().toArray( new TileInfo[ 0 ] ) );
		//for ( final TileInfo tileFinal : tilesInfoFinal.values() )
		//	System.out.println( (missingTilesInfo.containsKey(tileFinal.getIndex())?"***":"") + Arrays.toString(tileFinal.getPosition()));

		TileInfoJSONProvider.saveTilesConfiguration( tilesInfoFinal.values().toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 1 ], "_all" ) ) ) );

		System.out.println( "Done" );
	}
}