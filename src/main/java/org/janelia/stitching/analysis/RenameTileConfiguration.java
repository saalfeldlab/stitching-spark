package org.janelia.stitching.analysis;

import java.io.File;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

/**
 * @author Igor Pisarev
 */

public class RenameTileConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tilesOrig = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( args[ 0 ] ) );
		final TileInfo[] tilesToRename = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( args[ 1 ] ) );
		final String suffix = args.length > 2 ? args[ 2 ] : "";

		if ( tilesOrig.length != tilesToRename.length )
			throw new Exception( "Tiles count mismatch" );

		final String coordsPatternStr = ".*(\\d{3}x_\\d{3}y_\\d{3}z).*";
		final Pattern coordsPattern = Pattern.compile( coordsPatternStr );
		for ( int i = 0; i < tilesOrig.length; i++ )
		{
			final String origFilename = Paths.get( tilesOrig[ i ].getFilePath() ).getFileName().toString();
			final Matcher matcher = coordsPattern.matcher( origFilename );
			if ( !matcher.find() )
				throw new Exception( "Can't parse coordinates string" );

			final String coords = matcher.group( 1 );
			System.out.println( coords );
			final String currFolder = Paths.get( tilesToRename[ i ].getFilePath() ).getParent().toString();
			final String newPath = currFolder + "/" + coords + suffix + ".tif";

			new File( tilesToRename[ i ].getFilePath() ).renameTo( new File( newPath ) );
			tilesToRename[ i ].setFilePath( newPath );
		}

		TileInfoJSONProvider.saveTilesConfiguration( tilesToRename, dataProvider.getJsonWriter( Utils.addFilenameSuffix( args[ 1 ], "_renamed" ) ) );

		System.out.println( "Done" );
	}
}
