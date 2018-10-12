package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.stitching.PairwiseShiftsIndexFilter;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;

public class GenerateUsedPairwiseShiftsFileForStitchedConfiguration
{
	public static void main( final String[] args ) throws Exception
	{
		final String stitchedTileConfigurationFolderPath = args[ 0 ];
		// stitched config folder path is iterX/resulting-tile-configurations/cross.corr=X.XX,variance=XX.XX/

		// reach iterX/pairwise.json
		final String pairwiseConfigPath = PathResolver.get( PathResolver.getParent( PathResolver.getParent( stitchedTileConfigurationFolderPath ) ), "pairwise.json" );

		// reach iterX/resulting-tile-configurations/cross.corr=X.XX,variance=XX.XX/pairwise-used-indexes.json
		final String usedPairwiseIndexesPath = PathResolver.get( stitchedTileConfigurationFolderPath, "pairwise-used-indexes.json" );

		final DataProvider dataProvider = DataProviderFactory.createByURI( URI.create( stitchedTileConfigurationFolderPath ) );
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwiseConfigPath ) ) );

		final PairwiseShiftsIndexFilter usedPairwiseShiftsIndexes = PairwiseShiftsIndexFilter.loadFromFile( dataProvider, usedPairwiseIndexesPath );
		final List< SerializablePairWiseStitchingResult > usedPairwiseShifts = usedPairwiseShiftsIndexes.filterPairwiseShifts( pairwiseShifts );
		final String usedPairwiseShiftsPath = PathResolver.get( stitchedTileConfigurationFolderPath, "pairwise-used.json" );
		TileInfoJSONProvider.savePairwiseShifts( usedPairwiseShifts, dataProvider.getJsonWriter( URI.create( usedPairwiseShiftsPath ) ) );

		System.out.println( "Done" );
	}
}
