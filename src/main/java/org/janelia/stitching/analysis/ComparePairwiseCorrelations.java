package org.janelia.stitching.analysis;

import javafx.util.Pair;
import net.imglib2.util.SerializablePair;
import net.imglib2.util.ValuePair;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import java.io.IOException;
import java.util.*;

public class ComparePairwiseCorrelations
{
    public static void main( final String[] args ) throws Exception
    {
        if ( args.length != 2 )
            throw new IllegalArgumentException( "Can compare only two pairwise configuration files" );
        final String configA = args[ 0 ], configB = args[ 1 ];
        final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( configA ) );
        final List< SerializablePairWiseStitchingResult > shiftsA = loadPairwiseShifts( dataProvider, configA );
        final List< SerializablePairWiseStitchingResult > shiftsB = loadPairwiseShifts( dataProvider, configB );
        System.out.println( "A size: "  + shiftsA.size() + ", B size: " + shiftsB.size() + System.lineSeparator() );

        final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > shiftsMapB = Utils.createPairwiseShiftsMap( shiftsB, false );
        for ( final SerializablePairWiseStitchingResult shiftA : shiftsA )
        {
            final int ind1 = Math.min( shiftA.getTilePair().getA().getIndex(), shiftA.getTilePair().getB().getIndex() );
            final int ind2 = Math.max( shiftA.getTilePair().getA().getIndex(), shiftA.getTilePair().getB().getIndex() );

            if ( !shiftsMapB.containsKey( ind1 ) || !shiftsMapB.get( ind1 ).containsKey( ind2 ) )
            {
                System.out.println( shiftA.getTilePair() + ": shift not found in config 2" );
                continue;
            }

            final SerializablePairWiseStitchingResult shiftB = shiftsMapB.get( ind1 ).get( ind2 );
            System.out.println( String.format(
                    "%s:   config1 cr.corr=%.2f, valid=%b   config2 cr.corr=%.2f, valid=%b",
                    shiftA.getTilePair().toString(),
                    shiftA.getCrossCorrelation(),
                    shiftA.getIsValidOverlap(),
                    shiftB.getCrossCorrelation(),
                    shiftB.getIsValidOverlap()
                ) );
        }

        System.out.println( System.lineSeparator() + "-----------------------" + System.lineSeparator() + "Done" );
    }

    private static List< SerializablePairWiseStitchingResult > loadPairwiseShifts( final DataProvider dataProvider, final String pairwiseShiftsPath ) throws IOException
    {
        // pairwise shifts are currently as array (several different peaks are supported for a tile pair),
        // but the stitching code currently exports only one peak
        final List< SerializablePairWiseStitchingResult > shifts = new ArrayList<>();
        final List< SerializablePairWiseStitchingResult[] > pairwiseShiftsMultiPeaks = TileInfoJSONProvider.loadPairwiseShiftsMulti( dataProvider.getJsonReader( pairwiseShiftsPath ) );
        for ( final SerializablePairWiseStitchingResult[] pairwiseShiftMultiPeaks : pairwiseShiftsMultiPeaks )
            shifts.add( pairwiseShiftMultiPeaks[ 0 ] ); // simply take the first peak
        return shifts;
    }
}
