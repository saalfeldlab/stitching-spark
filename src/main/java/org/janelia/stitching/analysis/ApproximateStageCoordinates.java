package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.Arrays;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SubdividedTileBox;
import org.janelia.stitching.SubdividedTileOperations;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TransformedTileOperations;
import org.janelia.stitching.Utils;

import net.imglib2.RealInterval;
import net.imglib2.util.Intervals;

public class ApproximateStageCoordinates
{
	public static void main( final String[] args ) throws Exception
	{
		final String tilesConfigPath = args[ 0 ];
		final DataProvider dataProvider  = DataProviderFactory.createFSDataProvider();
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( tilesConfigPath ) ) );
		for ( final TileInfo tile : tiles )
		{
			final int[] noSubdivisionGrid = new int[ tile.numDimensions() ];
			Arrays.fill( noSubdivisionGrid, 1 );
			final SubdividedTileBox tileBox = SubdividedTileOperations.subdivideTiles( new TileInfo[] { tile }, noSubdivisionGrid ).iterator().next();
			final double[] transformedMiddlePoint = TransformedTileOperations.transformTileBoxMiddlePoint( tileBox, false );
			final RealInterval approximatedTileInterval = SubdividedTileOperations.getTileBoxInterval( transformedMiddlePoint, tile.getSize() );
			final double[] approximatedStagePosition = Intervals.minAsDoubleArray( approximatedTileInterval );
			tile.setStagePosition( approximatedStagePosition );
		}
		final String tilesConfigOutputPath = Utils.addFilenameSuffix( tilesConfigPath, "_approximated-stage-coords" );
		TileInfoJSONProvider.saveTilesConfiguration( tiles, dataProvider.getJsonWriter( URI.create( tilesConfigOutputPath ) ) );
		System.out.println( "Done" );
	}
}