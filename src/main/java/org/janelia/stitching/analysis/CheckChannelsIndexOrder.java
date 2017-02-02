package org.janelia.stitching.analysis;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;

public class CheckChannelsIndexOrder
{
	public static void main(final String[] args) throws Exception
	{
		final List< TileInfo[] > channels = new ArrayList<>();
		for (final String s : args)
			channels.add( TileInfoJSONProvider.loadTilesConfiguration(s) );

		final int n = channels.get(0).length;
		for (int c = 0; c < channels.size(); c++ )
			if ( channels.get(c).length != n )
				throw new Exception("number of tiles doesn't match");

		final String coordsPatternStr = ".*(\\d{3})x_(\\d{3})y_(\\d{3})z.*";
		final Pattern coordsPattern = Pattern.compile( coordsPatternStr );
		Matcher matcher = null;
		for ( int i = 0; i < n; i++ )
		{
			int[] coords = null;

			for ( int c = 0; c < channels.size(); c++ )
			{
				final TileInfo tile = channels.get(c)[ i ];
				if ( tile.getIndex() != i )
					throw new Exception("Tile indices mismatch: i="+i+", tileIndex="+tile.getIndex());

				matcher = coordsPattern.matcher( Paths.get( tile.getFilePath() ).getFileName().toString() );
				if ( !matcher.find() )
					throw new Exception( "Can't parse coordinates string" );

				final int[] thisCoords = new int[] { Integer.parseInt( matcher.group( 2 ) ), Integer.parseInt( matcher.group( 1 ) ), Integer.parseInt( matcher.group( 3 ) ) };
				if ( coords == null )
					coords = thisCoords.clone();
				else if ( coords[0] != thisCoords[0] || coords[1] != thisCoords[1] || coords[2] != thisCoords[2] )
					throw new Exception("Coords mismatch");
			}
		}
		System.out.println("All good");
	}
}
