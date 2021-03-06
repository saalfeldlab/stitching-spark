package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.*;

public class ExtractShiftsForTile
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( args[ 0 ] ) );
		final AxisMapping axisMapping = new AxisMapping( args[ 1 ] );
		final int tileIndex = Integer.parseInt( args[ 2 ] );

		final List< SerializablePairWiseStitchingResult > shiftsForTile = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTilePair().toArray() )
				if ( tile.getIndex() == tileIndex )
					shiftsForTile.add( shift );

		final TileInfo[] tiles = Utils.createTilesMap( shifts, true ).values().toArray( new TileInfo[ 0 ] );
		final TreeMap< Integer, int[] > coordinatesMap = Utils.getTilesCoordinatesMap( tiles, axisMapping );
		final TreeMap< Integer, Long > timestampsMap = Utils.getTilesTimestampsMap( tiles );

		for ( final SerializablePairWiseStitchingResult shift : shiftsForTile )
		{
			System.out.println("-----------------");
			System.out.println( String.format( "(%s,%s),   offset=%s, cr.corr=%f, ph.corr=%f:", shift.getTilePair().getA().getIndex(), shift.getTilePair().getB().getIndex(), Arrays.toString( shift.getOffset() ), shift.getCrossCorrelation(), shift.getPhaseCorrelation() ) );
			System.out.println( String.format( "   tile %s:  timestamp=%d,  coordinates=%s", shift.getTilePair().getA().getIndex(), timestampsMap.get( shift.getTilePair().getA().getIndex() ), Arrays.toString( coordinatesMap.get( shift.getTilePair().getA().getIndex() ) ) ) );
			System.out.println( String.format( "   tile %s:  timestamp=%d,  coordinates=%s", shift.getTilePair().getB().getIndex(), timestampsMap.get( shift.getTilePair().getB().getIndex() ), Arrays.toString( coordinatesMap.get( shift.getTilePair().getB().getIndex() ) ) ) );
		}
	}
}
