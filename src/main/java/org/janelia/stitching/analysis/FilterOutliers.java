package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import mpicbg.models.Point;
import mpicbg.models.PointMatch;

@Deprecated
public class FilterOutliers
{
	static double dist(final double[] a)
	{
		return Math.sqrt( a[0]*a[0] + a[1]*a[1] + a[2]*a[2] );
	}


	public static void main_single_channel( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );

		int before = 0, after = 0;
		for ( int i = 0; i < shifts.size(); i++ )
		{
			if ( !shifts.get( i ).getIsValidOverlap() )
				continue;

			before++;
			shifts.get( i ).setIsValidOverlap( false );

			final double[] diff = new double[ 3 ];

			for ( int d = 0; d < 3; d++ )
				diff[d] = (shifts.get(i).getTileBoxPair().getOriginalTilePair().getB().getPosition( d ) - shifts.get(i).getTileBoxPair().getOriginalTilePair().getA().getPosition( d )) - shifts.get(i).getOffset( d );

			if ( dist(diff) <= 40 && shifts.get(i).getCrossCorrelation() >= 0.75 )
			{
				shifts.get( i ).setIsValidOverlap( true );
				after++;
			}
		}

		System.out.println( "before="+before + ", after="+after);

		TileInfoJSONProvider.savePairwiseShifts( shifts, dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ 0 ], "_inliers" ) ) ) );
	}



	public static void main/*_channels_discrepancy*/( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult >[] shifts = new List[2];
		for ( int j = 0; j < 2; j++ )
			shifts[j]=TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( args[ j ] ) ) );

		//final Point zero = new Point( new double[ ch0Shifts.get( 0 ).getNumDimensions() ] );
		//final List< PairwiseShiftPointMatch > matches = new ArrayList<>(), inliers = new ArrayList<>();

		int before = 0, after = 0;
		for ( int i = 0; i < shifts[0].size(); i++ )
		{
			if ( !shifts[0].get( i ).getIsValidOverlap() || !shifts[1].get( i ).getIsValidOverlap() )
				continue;

			before++;
			for ( int j = 0; j < 2; j++ )
				shifts[j].get( i ).setIsValidOverlap( false );

			final double[][] diff = new double[ 2 ][ 3 ];

			for ( int j = 0; j < 2; j++ )
				for ( int d = 0; d < 3; d++ )
					diff[j][d] = (shifts[j].get(i).getTileBoxPair().getOriginalTilePair().getB().getPosition( d ) - shifts[j].get(i).getTileBoxPair().getOriginalTilePair().getA().getPosition( d )) - shifts[j].get(i).getOffset( d );

			//matches.add( new PairwiseShiftPointMatch( shift, new Point( dist ), zero ) );

			if ( Math.abs( dist(diff[0]) - dist(diff[1]) ) <= 4 && Math.min( shifts[0].get(i).getCrossCorrelation(), shifts[1].get(i).getCrossCorrelation()) >= 0.7 )
			{
				for ( int j = 0; j < 2; j++ )
					shifts[j].get( i ).setIsValidOverlap( true);
				after++;
			}
		}

		//final Model< ? > model = TileModelFactory.createDefaultModel( shifts.get( 0 ).getNumDimensions() );
		//model.filter( matches, inliers, 3 );

		//System.out.println( matches.size() + " matches, " + inliers.size() + " inliers" );
		System.out.println( "before="+before + ", after="+after);


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

		//for ( final PairwiseShiftPointMatch match : inliers )
		//	match.getShift().setIsValidOverlap( true );

		for ( int j = 0; j < 2; j++ )
			TileInfoJSONProvider.savePairwiseShifts( shifts[j], dataProvider.getJsonWriter( URI.create( Utils.addFilenameSuffix( args[ j ], "_inliers" ) ) ) );
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
