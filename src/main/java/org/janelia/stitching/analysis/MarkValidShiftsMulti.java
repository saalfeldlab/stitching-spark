package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.List;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

@Deprecated
public class MarkValidShiftsMulti
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shiftsFinal = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[0] ) ) );
		final List< SerializablePairWiseStitchingResult[] > shiftsMulti = TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( URI.create( args[1] ) ) );

		final TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult > > shiftsFinalValidMap = Utils.createPairwiseShiftsMap( shiftsFinal, true );



		int valid = 0;
		for ( final TreeMap< Integer, SerializablePairWiseStitchingResult[] > entry : Utils.createPairwiseShiftsMultiMap( shiftsMulti, true ).values() )
			valid += entry.size();
		System.out.println( "Valid initial multi shifts = " + valid );
		valid = 0;
		for ( final SerializablePairWiseStitchingResult shift : shiftsFinal )
			if ( shift.getIsValidOverlap() )
				valid++;
		System.out.println( "Valid final shifts = " + valid );
		for ( final TreeMap< Integer, SerializablePairWiseStitchingResult > entry : shiftsFinalValidMap.values() )
			valid -= entry.size();
		if ( valid != 0 )
			throw new Exception( "valid shifts count mismatch" );




		valid = 0;
		for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMulti )
		{
			final int ind1 = Math.min( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().getIndex(), shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getB().getIndex() );
			final int ind2 = Math.max( shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getA().getIndex(), shiftMulti[ 0 ].getTileBoxPair().getOriginalTilePair().getB().getIndex() );

			final boolean validShift = ( shiftsFinalValidMap.containsKey( ind1 ) && shiftsFinalValidMap.get( ind1 ).containsKey( ind2 ) );
			if ( validShift )
				valid++;
			for ( final SerializablePairWiseStitchingResult shift : shiftMulti )
				shift.setIsValidOverlap( validShift );
		}

		System.out.println( "Marked " + valid + " shifts as valid" );

		TileInfoJSONProvider.savePairwiseShiftsMulti( shiftsMulti, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[1], "_final" ) ) ) );
	}
}
