package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.Boundaries;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.Utils;

/**
 * Finds particular pairwise shift between specified tile indices.
 *
 * @author Igor Pisarev
 */

public class FindParticularShift
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );

		final int i1 = Integer.parseInt( args[ 1 ] ), i2 = Integer.parseInt( args[ 2 ] );
		System.out.println( "Tiles " + i1 + " and " + i2 + ":" );

		int hits = 0;
		SerializablePairWiseStitchingResult shift = null;
		for ( final SerializablePairWiseStitchingResult s : shifts ) {
			if ( (s.getTilePair().getA().getIndex() == i1 && s.getTilePair().getB().getIndex() == i2) ||
					(s.getTilePair().getA().getIndex() == i2 && s.getTilePair().getB().getIndex() == i1) )
			{
				shift = s;
				hits++;
			}
		}

		if ( hits == 0 )
			throw new Exception( "Not found" );
		else if ( hits != 1)
			throw new Exception( "Impossible: present more than once" );

		System.out.println( "Found:" );
		System.out.println( shift.getTilePair().getA().getIndex() + ": " + shift.getTilePair().getA().getFilePath() );
		System.out.println( shift.getTilePair().getB().getIndex() + ": " + shift.getTilePair().getB().getFilePath() );

		System.out.println( "------------------");
		System.out.println( "offset=" + Arrays.toString( shift.getOffset() ) );
		System.out.println( "cross correlation=" + shift.getCrossCorrelation() );
		System.out.println( "phase correlation=" + shift.getPhaseCorrelation() );
		System.out.println( "------------------");

		final TileInfo t1 = shift.getTilePair().getA();
		final TileInfo t2 = shift.getTilePair().getB();

		Boundaries overlap = TileOperations.getOverlappingRegionGlobal( t1, t2 );
		System.out.println( "Initial overlap at " + Arrays.toString( overlap.getMin() ) + " with dimensions " + Arrays.toString( overlap.getDimensions() ) );

		TileInfoJSONProvider.saveTilesConfiguration( new TileInfo[] { t1, t2 }, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[0], "_ORIGINAL" ) ) ) );

		for ( int d = 0; d < shift.getNumDimensions(); d++ )
			t2.setPosition( d, t1.getPosition( d ) + shift.getOffset( d ) );

		overlap = TileOperations.getOverlappingRegionGlobal( t1, t2 );
		if ( overlap != null)
			System.out.println( "Overlap after applying the offset at " + Arrays.toString( overlap.getMin() ) + " with dimensions " + Arrays.toString( overlap.getDimensions() ) );
		else
			System.out.println( "*** No overlap after applying the offset! ***" );

		TileInfoJSONProvider.saveTilesConfiguration( new TileInfo[] { t1, t2 }, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[0], "_SHIFTED" ) ) ) );
	}
}
