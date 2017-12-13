package org.janelia.stitching.analysis;

import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;

public class EstimateErrors
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final Map< Integer, TileInfo > originalTiles = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) ) );
		final TileInfo[] resultingTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) );

		parseCoordinates( resultingTiles );
		parseTimestamps( resultingTiles );
		Arrays.sort( resultingTiles, new TileTimestampComparator() );

		// subtract the first timestamp so we think the imaging starts at t=0
		/*final long timeStart = timestamps.get( resultingTiles[ 0 ].getIndex() );
		for ( final TileInfo tile : resultingTiles )
			timestamps.put( tile.getIndex(), timestamps.get( tile.getIndex() ) - timeStart );*/

		printConsecutivePairs( originalTiles, resultingTiles );
		//printAllPairs( originalTiles, resultingTiles );
	}


	static void printAllPairs( final Map< Integer, TileInfo > originalTiles, final TileInfo[] resultingTiles ) throws Exception
	{
		final PrintWriter writer = new PrintWriter("/groups/saalfeld/saalfeldlab/betzigcollab/plots-20160923/Sample9/ch1-errors-allpairs.txt", "UTF-8");

		int processed = 0;
		System.out.println( "Total number of tiles: " + resultingTiles.length );
		for ( int i = 0; i < resultingTiles.length; i++ )
		{
			for ( int j = i + 1; j < resultingTiles.length; j++ )
			{
				final TilePair pair = new TilePair( resultingTiles[ i ], resultingTiles[ j ] );

				String s = "";
				s += originalTiles.get( pair.getA().getIndex() ).getPosition( 0 ) + " " + originalTiles.get( pair.getA().getIndex() ).getPosition( 1 ) + " " + originalTiles.get( pair.getA().getIndex() ).getPosition( 2 ) + " ";
				s += originalTiles.get( pair.getB().getIndex() ).getPosition( 0 ) + " " + originalTiles.get( pair.getB().getIndex() ).getPosition( 1 ) + " " + originalTiles.get( pair.getB().getIndex() ).getPosition( 2 ) + " ";

				s += pair.getA().getPosition( 0 ) + " " + pair.getA().getPosition( 1 ) + " " + pair.getA().getPosition( 2 ) + " ";
				s += pair.getB().getPosition( 0 ) + " " + pair.getB().getPosition( 1 ) + " " + pair.getB().getPosition( 2 ) + " ";

				s += timestamps.get( pair.getA().getIndex() ) + " " + timestamps.get( pair.getB().getIndex() );

				processed++;
				writer.println( s );
			}
		}
		System.out.println( "Processed: " + processed );
		writer.close();
	}



	static void printConsecutivePairs( final Map< Integer, TileInfo > originalTiles, final TileInfo[] resultingTiles ) throws Exception
	{
		final List< TilePair > consecutivePairs = new ArrayList<>();
		TileInfo prevTile = null;
		for ( final TileInfo tile : resultingTiles )
		{
			if ( prevTile != null && timestamps.get( tile.getIndex() ) - timestamps.get( prevTile.getIndex() ) <= consecutiveDeltaTime )
				consecutivePairs.add( new TilePair( prevTile, tile ) );
			prevTile = tile;
		}

		System.out.println( consecutivePairs.size() + " consecutive pairs out of " + resultingTiles.length + " tiles total" );

		final PrintWriter forwardsWriter = new PrintWriter("/groups/saalfeld/saalfeldlab/betzigcollab/plots-20160923/Sample9/ch1-errors-z-forwards.txt", "UTF-8");
		final PrintWriter backwardsWriter = new PrintWriter("/groups/saalfeld/saalfeldlab/betzigcollab/plots-20160923/Sample9/ch1-errors-z-backwards.txt", "UTF-8");

		int processed = 0;
		for ( final TilePair pair : consecutivePairs )
		{
			if ( coordinates.get( pair.getA().getIndex() ).x != coordinates.get( pair.getB().getIndex() ).x || coordinates.get( pair.getA().getIndex() ).y != coordinates.get( pair.getB().getIndex() ).y )
				continue;

			String s = "";
			s += originalTiles.get( pair.getA().getIndex() ).getPosition( 0 ) + " " + originalTiles.get( pair.getA().getIndex() ).getPosition( 1 ) + " " + originalTiles.get( pair.getA().getIndex() ).getPosition( 2 ) + " ";
			s += originalTiles.get( pair.getB().getIndex() ).getPosition( 0 ) + " " + originalTiles.get( pair.getB().getIndex() ).getPosition( 1 ) + " " + originalTiles.get( pair.getB().getIndex() ).getPosition( 2 ) + " ";

			s += pair.getA().getPosition( 0 ) + " " + pair.getA().getPosition( 1 ) + " " + pair.getA().getPosition( 2 ) + " ";
			s += pair.getB().getPosition( 0 ) + " " + pair.getB().getPosition( 1 ) + " " + pair.getB().getPosition( 2 ) + " ";

			processed++;
			if ( coordinates.get( pair.getA().getIndex() ).z < coordinates.get( pair.getB().getIndex() ).z )
				forwardsWriter.println( s );
			else
				backwardsWriter.println( s );
		}
		System.out.println( "Processed: " + processed );
		forwardsWriter.close();
		backwardsWriter.close();
	}




	static Map< Integer, Coordinates > coordinates;
	static void parseCoordinates( final TileInfo[] tiles ) throws Exception
	{
		coordinates = new HashMap<>();
		final String coordsPatternStr = ".*(\\d{3})x_(\\d{3})y_(\\d{3})z.*";
		final Pattern coordsPattern = Pattern.compile( coordsPatternStr );
		for ( final TileInfo tile : tiles )
		{
			final String filename = Paths.get( tile.getFilePath() ).getFileName().toString();
			final Matcher matcher = coordsPattern.matcher( filename );
			if ( !matcher.find() )
				throw new Exception( "Can't parse coordinates" );

			// swap X and Y axes
			coordinates.put( tile.getIndex(), new Coordinates(
					Integer.parseInt( matcher.group( 2 ) ),
					Integer.parseInt( matcher.group( 1 ) ),
					Integer.parseInt( matcher.group( 3 ) )
					) );
		}
	}
	static class Coordinates
	{
		public final int x,y,z;
		public Coordinates( final int x, final int y, final int z )
		{
			this.x = x;
			this.y = y;
			this.z = z;
		}
	}


	static final long consecutiveDeltaTime = 12000;
	static Map< Integer, Long > timestamps;
	static void parseTimestamps( final TileInfo[] tiles ) throws Exception
	{
		timestamps = new HashMap<>();
		final String timePatternStr = ".*_(\\d*)msecAbs.*";
		final Pattern timePattern = Pattern.compile( timePatternStr );
		for ( final TileInfo tile : tiles )
		{
			final String filename = Paths.get( tile.getFilePath() ).getFileName().toString();
			final Matcher matcher = timePattern.matcher( filename );
			if ( !matcher.find() )
				throw new Exception( "Can't parse timestamp" );

			final long timestamp = Long.parseLong( matcher.group( 1 ) );
			timestamps.put( tile.getIndex(), timestamp );
		}
	}

	static class TileTimestampComparator implements Comparator< TileInfo >
	{
		@Override
		public int compare( final TileInfo t1, final TileInfo t2 )
		{
			return Long.compare( timestamps.get( t1.getIndex() ), timestamps.get( t2.getIndex() ) );
		}
	}
}
