package org.janelia.stitching.analysis;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import ij.IJ;
import ij.ImagePlus;
import ij.plugin.ZProjector;

public class TilesPairwiseCoverage
{
	public static void main( final String[] args ) throws Exception
	{
		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] ) );
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args[ 1 ] );

		int validShifts = 0;
		final Map< Integer, TileInfo > uncoveredTiles = Utils.createTilesMap( shifts );
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			if ( !shift.getIsValidOverlap() )
				continue;

			validShifts++;
			for ( final TileInfo tile : shift.getTilePair().toArray() )
				uncoveredTiles.remove( tile.getIndex() );
		}

		System.out.println( "There are " + validShifts + " valid shifts of " + shifts.size() );
		System.out.println( uncoveredTiles.size() + " tiles uncovered: " + uncoveredTiles.keySet() );

		if ( !uncoveredTiles.isEmpty() && args.length > 2 && args[2].equals( "--showuncovered" ))
		{
			final String folder = "uncovered-"+Paths.get( args[1]).getFileName().toString();
			new File( folder ).mkdirs();
			for ( final TileInfo tile : uncoveredTiles.values() )
			{
				System.out.println( "Processing tile " + tile.getIndex() );
				final ImagePlus imp = IJ.openImage(tile.getFilePath());
				Utils.workaroundImagePlusNSlices( imp );

				final ZProjector p = new ZProjector(imp);
				p.setMethod(ZProjector.MAX_METHOD);
				p.doProjection();
				IJ.saveAsTiff(p.getProjection(), folder + "/" + Paths.get( tilesMap.get( tile.getIndex() ).getFilePath() ).getFileName()+".maxi.tif");
			}
			System.out.println( "Done!" );
		}
	}
}
