package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class RetainPairwiseShiftsForConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final String tilesPath = args[ 0 ], pairwisePath = args[ 1 ];
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( tilesPath ) ) ) );
		final List< SerializablePairWiseStitchingResult > pairwise = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwisePath ) ) );

		final List< SerializablePairWiseStitchingResult > retainedPairwise = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : pairwise )
			if ( tilesMap.containsKey( shift.getSubTilePair().getFullTilePair().getA().getIndex() ) && tilesMap.containsKey( shift.getSubTilePair().getFullTilePair().getB().getIndex() ) )
				retainedPairwise.add( shift );

		System.out.println( "was " + pairwise.size() + " shifts, retained " + retainedPairwise.size() + " shifts" );

		TileInfoJSONProvider.savePairwiseShifts( retainedPairwise, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( pairwisePath, "_retained" ) ) ) );
	}
}
