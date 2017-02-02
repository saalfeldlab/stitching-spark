package org.janelia.stitching.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

public class FixTilePosition
{
	public static void main( final String[] args ) throws Exception
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final int tileIndex = Integer.parseInt( args[ 1 ] );
		final int dimToFix = Integer.parseInt( args[ 2 ] );
		final double newVal = Double.parseDouble( args[ 3 ] );

		for ( final TileInfo tile : tiles )
			if ( tile.getIndex() == tileIndex )
				tile.setPosition( dimToFix, newVal );

		final String outFilename = Utils.addFilenameSuffix( args[ 0 ], "_fixed_" + tileIndex + "_" + (dimToFix==0?"x":(dimToFix==1?"y":"z")) );
		TileInfoJSONProvider.saveTilesConfiguration( tiles, outFilename );

		try
		{
			final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( Utils.addFilenameSuffix( args[ 0 ], "_pairwise" ) );
			final List< SerializablePairWiseStitchingResult > fixedShifts = new ArrayList<>();
			for ( final SerializablePairWiseStitchingResult shift : shifts )
				if ( shift.getTilePair().getA().getIndex() != tileIndex && shift.getTilePair().getB().getIndex() != tileIndex )
					fixedShifts.add( shift );
			TileInfoJSONProvider.savePairwiseShifts( fixedShifts, Utils.addFilenameSuffix( outFilename, "_pairwise" ) );
		}
		catch ( final IOException e )
		{
			System.out.println( "No pairwise shifts file" );
		}
	}
}
