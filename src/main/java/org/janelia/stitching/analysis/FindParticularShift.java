package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.Utils;

import net.imglib2.Interval;
import net.imglib2.util.Intervals;

/**
 * Finds particular pairwise shift between specified tile indices.
 *
 * @author Igor Pisarev
 */

@Deprecated
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
			if ( (s.getSubTilePair().getFullTilePair().getA().getIndex() == i1 && s.getSubTilePair().getFullTilePair().getB().getIndex() == i2) ||
					(s.getSubTilePair().getFullTilePair().getA().getIndex() == i2 && s.getSubTilePair().getFullTilePair().getB().getIndex() == i1) )
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
		System.out.println( shift.getSubTilePair().getFullTilePair().getA().getIndex() + ": " + shift.getSubTilePair().getFullTilePair().getA().getFilePath() );
		System.out.println( shift.getSubTilePair().getFullTilePair().getB().getIndex() + ": " + shift.getSubTilePair().getFullTilePair().getB().getFilePath() );

		System.out.println( "------------------");
		System.out.println( "offset=" + Arrays.toString( shift.getOffset() ) );
		System.out.println( "cross correlation=" + shift.getCrossCorrelation() );
		System.out.println( "phase correlation=" + shift.getPhaseCorrelation() );
		System.out.println( "------------------");

		final TileInfo t1 = shift.getSubTilePair().getFullTilePair().getA();
		final TileInfo t2 = shift.getSubTilePair().getFullTilePair().getB();

		Interval overlap = TileOperations.getOverlappingRegionGlobal( t1, t2 );
		System.out.println( "Initial overlap at " + Arrays.toString( Intervals.minAsLongArray( overlap ) ) + " with dimensions " + Arrays.toString( Intervals.dimensionsAsLongArray( overlap ) ) );

		TileInfoJSONProvider.saveTilesConfiguration( new TileInfo[] { t1, t2 }, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[0], "_ORIGINAL" ) ) ) );

		for ( int d = 0; d < shift.getNumDimensions(); d++ )
			t2.setStagePosition( d, t1.getStagePosition( d ) + shift.getOffset( d ) );

		overlap = TileOperations.getOverlappingRegionGlobal( t1, t2 );
		if ( overlap != null)
			System.out.println( "Overlap after applying the offset at " + Arrays.toString( Intervals.minAsLongArray( overlap )  ) + " with dimensions " + Arrays.toString( Intervals.dimensionsAsLongArray( overlap ) ) );
		else
			System.out.println( "*** No overlap after applying the offset! ***" );

		TileInfoJSONProvider.saveTilesConfiguration( new TileInfo[] { t1, t2 }, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[0], "_SHIFTED" ) ) ) );
	}
}
