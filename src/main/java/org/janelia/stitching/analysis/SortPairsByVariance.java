package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;

public class SortPairsByVariance
{
	public static void main( final String[] args ) throws Exception
	{
		final String pairwisePath = args[ 0 ];
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final List< SerializablePairWiseStitchingResult > pairwise = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwisePath ) ) );
		pairwise.sort( ( a, b ) -> -Double.compare( a.getVariance() != null ? a.getVariance() : 0, b.getVariance() != null ? b.getVariance() : 0 ) );
		for ( final SerializablePairWiseStitchingResult pair : pairwise )
			System.out.println( String.format( "variance=%.2f %s cr.corr=%.2f, ph.corr=%.8f", pair.getVariance(), pair.getTileBoxPair().getOriginalTilePair(), pair.getCrossCorrelation(), pair.getPhaseCorrelation() ) );
	}
}
