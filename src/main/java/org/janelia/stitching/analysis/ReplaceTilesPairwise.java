package org.janelia.stitching.analysis;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;

public class ReplaceTilesPairwise
{
	public static void main( final String[] args ) throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List<SerializablePairWiseStitchingResult> shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final Map< Integer, TileInfo > tiles = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) ) );

		final List<SerializablePairWiseStitchingResult> newShifts = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			final TilePair newTilePair = new TilePair(
					tiles.get( shift.getTilePair().getA().getIndex() ),
					tiles.get( shift.getTilePair().getB().getIndex() ) );
			final SerializablePairWiseStitchingResult newShift = new SerializablePairWiseStitchingResult(
					newTilePair, shift.getOffset().clone(), shift.getCrossCorrelation(), shift.getPhaseCorrelation() );
			newShifts.add( newShift );
		}
		TileInfoJSONProvider.savePairwiseShifts( newShifts, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix(args[1], "_pairwise") ) ) );
	}
}
