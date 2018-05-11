package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

@Deprecated
public class ChooseSameSubset {

	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > origShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[0] ) ) );
		final TreeMap<Integer, TreeMap<Integer, SerializablePairWiseStitchingResult>> subsetShiftsMap = Utils.createPairwiseShiftsMap(TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[1] ) ) ), false);
		final ArrayList< SerializablePairWiseStitchingResult > finalShifts = new ArrayList<>();

		int totalCount = 0, finalCount = 0;
		for ( final SerializablePairWiseStitchingResult shift : origShifts )
		{
			if ( shift.getIsValidOverlap() )
			{
				totalCount++;

				final int ind1 = Math.min(shift.getTileBoxPair().getOriginalTilePair().getA().getIndex(), shift.getTileBoxPair().getOriginalTilePair().getB().getIndex());
				final int ind2 = Math.max(shift.getTileBoxPair().getOriginalTilePair().getA().getIndex(), shift.getTileBoxPair().getOriginalTilePair().getB().getIndex());

				if ( !subsetShiftsMap.containsKey(ind1) || !subsetShiftsMap.get(ind1).containsKey(ind2))
				{
					finalCount++;
					finalShifts.add(shift);
				}
			}
		}

		System.out.println("totalCount="+totalCount);
		System.out.println("finalCount="+finalCount);

		TileInfoJSONProvider.savePairwiseShifts( finalShifts, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix(args[0], "_subset") ) ) );
	}
}
