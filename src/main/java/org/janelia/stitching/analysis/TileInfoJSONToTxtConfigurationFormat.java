package org.janelia.stitching.analysis;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.Arrays;

import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;

public class TileInfoJSONToTxtConfigurationFormat
{
	public static void main( final String[] args ) throws IOException
	{
		final String inputFilepath = args[ 0 ];
		final String outputFilepath = inputFilepath.substring( 0, inputFilepath.lastIndexOf( "." ) ) + ".txt";

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilepath );
		TileOperations.translateTilesToOriginReal( tiles );

		try ( final PrintWriter writer = new PrintWriter( outputFilepath ) )
		{
			writer.println( "# Define the number of dimensions we are working on" );
			writer.println( "dim = " + tiles[ 0 ].numDimensions() );
			writer.println();
			writer.println( "# Define the image coordinates" );
			for ( final TileInfo tile : tiles )
			{
				final String tilePositionStr = Arrays.toString( tile.getPosition() );
				writer.println( String.format( "%s; ; (%s)", Paths.get( tile.getFilePath() ).getFileName().toString(), tilePositionStr.substring( 1, tilePositionStr.length() - 1 ) ) );
			}
		}
	}
}
