package org.janelia.dataaccess;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.List;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;

public abstract class AbstractJSONDataProvider implements DataProvider
{
	@Override
	public Reader getJsonReader( final String link ) throws IOException
	{
		return new InputStreamReader( getInputStream( link ) );
	}

	@Override
	public Writer getJsonWriter( final String link ) throws IOException
	{
		return new OutputStreamWriter( getOutputStream( link ) );
	}

	@Override
	public TileInfo[] loadTiles( final String link ) throws IOException
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( getJsonReader( link ) );

		// ensure absolute paths
		for ( final TileInfo tile : tiles )
		{
			final boolean isAbsolutePath = getType() == DataProviderType.FILESYSTEM ? Paths.get( tile.getFilePath() ).isAbsolute() : CloudURI.isCloudURI( link );
			if ( !isAbsolutePath )
				tile.setFilePath( PathResolver.get( PathResolver.getParent( link ), tile.getFilePath() ) );
		}

		return tiles;
	}

	@Override
	public void saveTiles( final TileInfo[] tiles, final String link ) throws IOException
	{
		TileInfoJSONProvider.saveTilesConfiguration( tiles, getJsonWriter( link ) );
	}

	@Override
	public List< SerializablePairWiseStitchingResult > loadPairwiseShifts( final String link ) throws IOException
	{
		return TileInfoJSONProvider.loadPairwiseShifts( getJsonReader( link ) );
	}

	@Override
	public void savePairwiseShifts( final List< SerializablePairWiseStitchingResult > pairwiseShifts, final String link ) throws IOException
	{
		TileInfoJSONProvider.savePairwiseShifts( pairwiseShifts, getJsonWriter( link ) );
	}
}
