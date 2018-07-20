package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

@Deprecated
public class ExtractShiftsForTile
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final int tileIndex = Integer.parseInt( args[ 1 ] );

		final List< SerializablePairWiseStitchingResult > shiftsForTile = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getSubTilePair().getFullTilePair().toArray() )
				if ( tile.getIndex() == tileIndex )
					shiftsForTile.add( shift );

		final TileInfo[] tiles = Utils.createTilesMap( shifts, true ).values().toArray( new TileInfo[ 0 ] );
		final TreeMap< Integer, int[] > coordinatesMap = Utils.getTilesCoordinatesMap( tiles );
		final TreeMap< Integer, Long > timestampsMap = Utils.getTilesTimestampsMap( tiles );

		for ( final SerializablePairWiseStitchingResult shift : shiftsForTile )
		{
			System.out.println("-----------------");
			System.out.println( String.format( "(%s,%s),   offset=%s, cr.corr=%f, ph.corr=%f:", shift.getSubTilePair().getFullTilePair().getA().getIndex(), shift.getSubTilePair().getFullTilePair().getB().getIndex(), Arrays.toString( shift.getOffset() ), shift.getCrossCorrelation(), shift.getPhaseCorrelation() ) );
			System.out.println( String.format( "   tile %s:  timestamp=%d,  coordinates=%s", shift.getSubTilePair().getFullTilePair().getA().getIndex(), timestampsMap.get( shift.getSubTilePair().getFullTilePair().getA().getIndex() ), Arrays.toString( coordinatesMap.get( shift.getSubTilePair().getFullTilePair().getA().getIndex() ) ) ) );
			System.out.println( String.format( "   tile %s:  timestamp=%d,  coordinates=%s", shift.getSubTilePair().getFullTilePair().getB().getIndex(), timestampsMap.get( shift.getSubTilePair().getFullTilePair().getB().getIndex() ), Arrays.toString( coordinatesMap.get( shift.getSubTilePair().getFullTilePair().getB().getIndex() ) ) ) );
		}
	}
}
