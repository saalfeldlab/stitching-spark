import java.util.ArrayList;
import java.util.List;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;

import mpicbg.models.PointMatch;
import mpicbg.models.SimilarityModel3D;
import mpicbg.models.TranslationModel3D;

public class SimilarityModelSinglePairTest
{
	public static void main( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult[] > shiftsMulti = TileInfoJSONProvider.loadPairwiseShiftsMulti( "/nrs/saalfeld/igor/MB_310C_run2/test-similarity-model/iter0/pairwise.json" );
		final SerializablePairWiseStitchingResult[] shiftMulti = shiftsMulti.get( 0 );

		testSimilarityModel( shiftMulti );
		testTranslationModel( shiftMulti );
	}

	private static void testSimilarityModel( final SerializablePairWiseStitchingResult[] shiftMulti ) throws Exception
	{
		final List< PointMatch > matches = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shiftMulti )
			matches.add( new PointMatch( shift.getPointPair().getA(), shift.getPointPair().getB(), shift.getCrossCorrelation() ) );

		final SimilarityModel3D model = new SimilarityModel3D();
		model.fit( matches );
		PointMatch.apply( matches, model );

		System.out.println();
		System.out.println( "-- SimilarityModel3D --" );
		System.out.println( model );
		System.out.println( String.format( "Error: %.2f px", PointMatch.maxDistance( matches ) ) );
	}

	private static void testTranslationModel( final SerializablePairWiseStitchingResult[] shiftMulti ) throws Exception
	{
		final List< PointMatch > matches = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult shift : shiftMulti )
			matches.add( new PointMatch( shift.getPointPair().getA(), shift.getPointPair().getB(), shift.getCrossCorrelation() ) );

		final TranslationModel3D model = new TranslationModel3D();
		model.fit( matches );
		PointMatch.apply( matches, model );

		System.out.println();
		System.out.println( "-- TranslationModel3D --" );
		System.out.println( model );
		System.out.println( String.format( "Error: %.2f px", PointMatch.maxDistance( matches ) ) );
	}
}
