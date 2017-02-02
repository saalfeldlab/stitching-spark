package org.janelia.stitching.analysis;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;

public class TraceConfigurationsDistance
{
	public static void main( final String[] args ) throws Exception
	{
		final TileInfo[] tilesBefore = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final Map< Integer, TileInfo > tilesMapAfter = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( args[ 1 ] ) );

		//final Integer dim = ;

		final String outputFolder = args[ args.length - 1 ];
		final String outFilepath = outputFolder + "/" + "distances.txt";
		final PrintWriter writer = new PrintWriter(outFilepath, "UTF-8");

		final List< TilePair > overlappingTilesBefore = TileOperations.findOverlappingTiles( tilesBefore);
		int after = 0;
		System.out.println( "Overlapping pairs before: " + overlappingTilesBefore.size() );

		for ( final TilePair pair : overlappingTilesBefore )
		{
			if ( !tilesMapAfter.containsKey( pair.getA().getIndex() ) || !tilesMapAfter.containsKey( pair.getB().getIndex() ) )
				continue;

			after++;

			final double dist[] = new double[ 3 ];
			for ( int d = 0; d < 3; d++ )
				dist[d] = (pair.getB().getPosition( d ) - pair.getA().getPosition( d )) - (tilesMapAfter.get( pair.getB().getIndex() ).getPosition( d ) - tilesMapAfter.get( pair.getA().getIndex() ).getPosition( d ));

			writer.println( dist[0] + " " + dist[1] + " " + dist[2] );
		}

		writer.close();

		System.out.println( "After: " + after );

		System.out.println( "Created: " + outFilepath );
	}
}
