package org.janelia.stitching.analysis;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class FixTilePosition
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final int tileIndex = Integer.parseInt( args[ 1 ] );
		final int dimToFix = Integer.parseInt( args[ 2 ] );
		final double newVal = Double.parseDouble( args[ 3 ] );

		for ( final TileInfo tile : tiles )
			if ( tile.getIndex() == tileIndex )
				tile.setPosition( dimToFix, newVal );

		final String outFilename = Utils.addFilenameSuffix( args[ 0 ], "_fixed_" + tileIndex + "_" + (dimToFix==0?"x":(dimToFix==1?"y":"z")) );
		TileInfoJSONProvider.saveTilesConfiguration( tiles, dataProvider.getJsonWriter( URI.create( outFilename ) ) );

		try
		{
			final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( Utils.addFilenameSuffix( args[ 0 ], "_pairwise" ) ) ) );
			final List< SerializablePairWiseStitchingResult > fixedShifts = new ArrayList<>();
			for ( final SerializablePairWiseStitchingResult shift : shifts )
				if ( shift.getTilePair().getA().getIndex() != tileIndex && shift.getTilePair().getB().getIndex() != tileIndex )
					fixedShifts.add( shift );
			TileInfoJSONProvider.savePairwiseShifts( fixedShifts, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( outFilename, "_pairwise" ) ) ) );
		}
		catch ( final IOException e )
		{
			System.out.println( "No pairwise shifts file" );
		}
	}
}
