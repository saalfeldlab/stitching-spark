package org.janelia.stitching.analysis;

import java.util.ArrayList;
import java.util.List;

import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileModelFactory;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;

import mpicbg.models.Model;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;

public class FilterOutliers
{
	public static void main( final String[] args ) throws Exception
	{
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( args[ 0 ] );

		final Point zero = new Point( new double[ shifts.get( 0 ).getNumDimensions() ] );
		final List< PairwiseShiftPointMatch > matches = new ArrayList<>(), inliers = new ArrayList<>();

		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			if ( !shift.getIsValidOverlap() )
				continue;

			shift.setIsValidOverlap( false );
			final TilePair pair = shift.getTilePair();
			final double dist[] = new double[ shift.getNumDimensions() ];

			for ( int d = 0; d < shift.getNumDimensions(); d++ )
				dist[d] = (pair.second().getPosition( d ) - pair.first().getPosition( d )) - shift.getOffset( d );

			matches.add( new PairwiseShiftPointMatch( shift, new Point( dist ), zero ) );
		}

		final Model< ? > model = TileModelFactory.createDefaultModel( shifts.get( 0 ).getNumDimensions() );
		model.filter( matches, inliers, 3 );

		System.out.println( matches.size() + " matches, " + inliers.size() + " inliers" );


		// output filtered distances
		/*final PrintWriter writer = new PrintWriter("distances.txt", "UTF-8");
		for ( final PairwiseShiftPointMatch match : inliers )
		{
			final SerializablePairWiseStitchingResult shift = match.getShift();
			final double dist[] = new double[ shift.getNumDimensions() ];
			for ( int d = 0; d < shift.getNumDimensions(); d++ )
				dist[d] = (shift.getTilePair().second().getPosition( d ) - shift.getTilePair().first().getPosition( d )) - shift.getOffset( d );
			writer.println( dist[0] + " " + dist[1] + " " + dist[2] );
		}
		writer.close();*/

		for ( final PairwiseShiftPointMatch match : inliers )
			match.getShift().setIsValidOverlap( true );
		TileInfoJSONProvider.savePairwiseShifts( shifts, Utils.addFilenameSuffix( args[ 0 ], "_inliers" ) );
	}




	static class PairwiseShiftPointMatch extends PointMatch
	{
		private static final long serialVersionUID = 5906291009263858005L;

		private final SerializablePairWiseStitchingResult shift;

		public PairwiseShiftPointMatch( final SerializablePairWiseStitchingResult shift, final Point p1, final Point p2 )
		{
			super( p1, p2 );
			this.shift = shift;
		}

		public SerializablePairWiseStitchingResult getShift() { return shift; }
	}
}
