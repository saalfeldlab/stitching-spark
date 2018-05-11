package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

/**
 * @author Igor Pisarev
 */

@Deprecated
public class RenamePairwise
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) ) );
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) );

		final Set< Integer > validation = new HashSet<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			validation.add( shift.getTileBoxPair().getOriginalTilePair().getA().getIndex() );
			validation.add( shift.getTileBoxPair().getOriginalTilePair().getB().getIndex() );
		}
		if ( tilesMap.size() != validation.size() )
			throw new Exception( "Different sets of tiles" );

		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			shift.getTileBoxPair().getOriginalTilePair().getA().setFilePath( tilesMap.get( shift.getTileBoxPair().getOriginalTilePair().getA().getIndex() ).getFilePath() );
			shift.getTileBoxPair().getOriginalTilePair().getB().setFilePath( tilesMap.get( shift.getTileBoxPair().getOriginalTilePair().getB().getIndex() ).getFilePath() );
		}

		TileInfoJSONProvider.savePairwiseShifts( shifts, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 1 ], "_renamed" ) ) ) );

		System.out.println( "Done" );
	}
}
