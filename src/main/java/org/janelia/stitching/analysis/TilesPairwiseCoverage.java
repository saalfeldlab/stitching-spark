package org.janelia.stitching.analysis;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import ij.IJ;
import ij.ImagePlus;
import ij.plugin.ZProjector;
import net.imglib2.multithreading.SimpleMultiThreading;

@Deprecated
public class TilesPairwiseCoverage
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) ) );
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) );

		int validShifts = 0;
		//final Map< Integer, TileInfo > uncoveredTiles = Utils.createTilesMap( shifts );
		final Map< Integer, TileInfo > uncoveredTiles = new TreeMap<>( tilesMap );
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			if ( !shift.getIsValidOverlap() )
				continue;

			validShifts++;
			for ( final TileInfo tile : shift.getSubTilePair().getFullTilePair().toArray() )
				uncoveredTiles.remove( tile.getIndex() );
		}

		System.out.println( "There are " + validShifts + " valid shifts of " + shifts.size() );
		System.out.println( uncoveredTiles.size() + " tiles uncovered: " + uncoveredTiles.keySet() );

		if ( !uncoveredTiles.isEmpty() && args.length > 2 && args[2].equals( "--showuncovered" ))
		{
			final String workingFolder = args.length > 3 ? args[3] : "";
			final String folder = (workingFolder.isEmpty()?"":workingFolder+"/") + "uncovered-"+Paths.get( args[0]).getFileName().toString();
			new File( folder ).mkdirs();

			final List< TileInfo > uncoveredTilesList = new ArrayList<>( uncoveredTiles.values() );

			final AtomicInteger ai = new AtomicInteger(0);
			final Thread[] threads = SimpleMultiThreading.newThreads(10);
			for ( int ithread = 0; ithread < threads.length; ++ithread )
				threads[ ithread ] = new Thread(() -> {
					final int myNumber = ai.getAndIncrement();

					for ( int i = 0; i < uncoveredTilesList.size(); i++ )
					{
						if ( i % threads.length == myNumber )
						{
							final TileInfo tile = uncoveredTilesList.get( i );
							System.out.println( "Processing tile " + tile.getIndex() + ": " + tile.getFilePath() );
							final ImagePlus imp = IJ.openImage(tile.getFilePath());
							Utils.workaroundImagePlusNSlices( imp );

							final ZProjector p = new ZProjector(imp);
							p.setMethod(ZProjector.MAX_METHOD);
							p.doProjection();

							final String outFilename = Paths.get( tilesMap.get( tile.getIndex() ).getFilePath() ).getFileName()+".maxi.tif";
							System.out.println( "Saving tile " + tile.getIndex() + " to " + outFilename );
							IJ.saveAsTiff(p.getProjection(), folder + "/" + outFilename);
						}
					}
				});

			SimpleMultiThreading.startAndJoin( threads );

			System.out.println( "Done!" );
		}
	}


	public static Map< Integer, TileInfo > getUncoveredTiles( final List< SerializablePairWiseStitchingResult > shifts )
	{
		final Map< Integer, TileInfo > tilesMap = Utils.createTilesMap( shifts, false );
		final Map< Integer, TileInfo > uncoveredTiles = new TreeMap<>( tilesMap );
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( shift.getIsValidOverlap() )
				for ( final TileInfo tile : shift.getSubTilePair().getFullTilePair().toArray() )
					uncoveredTiles.remove( tile.getIndex() );
		return uncoveredTiles;
	}
	public static Map< Integer, TileInfo > getRemainingTiles( final List< SerializablePairWiseStitchingResult > shifts )
	{
		final Map< Integer, TileInfo > remainingTiles = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			if ( shift.getIsValidOverlap() )
				for ( final TileInfo tile : shift.getSubTilePair().getFullTilePair().toArray() )
					remainingTiles.put( tile.getIndex(), tile );
		return remainingTiles;
	}
}
