package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileModelFactory;
import org.janelia.stitching.TransformedTileOperations;
import org.janelia.stitching.Utils;

import mpicbg.models.Model;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.iterator.IntervalIterator;

public class EvaluateStitchingSolutionAgainstGroundtruth
{
	public static < M extends Model< M > > void main( final String[] args ) throws Exception
	{
		final String groundtruthTilesPath = args[ 0 ], stitchedTilesPath = args[ 1 ];
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > groundtruthTilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( groundtruthTilesPath ) ) ) );
		final Map< Integer, TileInfo > stitchedTilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( stitchedTilesPath ) ) ) );

		final Set< Integer > intersection = new HashSet<>();
		intersection.addAll( groundtruthTilesMap.keySet() );
		intersection.retainAll( stitchedTilesMap.keySet() );

		System.out.println( "groundtruth tiles: " + groundtruthTilesMap.size() );
		System.out.println( "stitched tiles: " + stitchedTilesMap.size() );
		System.out.println( "intersection: " + intersection.size() );

		groundtruthTilesMap.keySet().retainAll( intersection );
		stitchedTilesMap.keySet().retainAll( intersection );

		final Dimensions tileDimensions = new FinalDimensions( stitchedTilesMap.values().iterator().next().getSize() );

		final int[] sampledPointsCount = new int[ tileDimensions.numDimensions() ];
		Arrays.fill( sampledPointsCount, 5 );

		final Map< Integer, Set< PointMatch > > tileToPointMatches = new TreeMap<>();
		for ( final TileInfo groundtruthTile : groundtruthTilesMap.values() )
		{
			final TileInfo stitchedTile = stitchedTilesMap.get( groundtruthTile.getIndex() );
			final IntervalIterator sampledPointsIterator = new IntervalIterator( sampledPointsCount );
			while ( sampledPointsIterator.hasNext() )
			{
				sampledPointsIterator.fwd();

				final double[] localTilePosition = new double[ tileDimensions.numDimensions() ];
				for ( int d = 0; d < localTilePosition.length; ++d )
					localTilePosition[ d ] = sampledPointsIterator.getIntPosition( d ) * ( ( double ) tileDimensions.dimension( d ) / ( sampledPointsCount[ d ] - 1 ) );

				final double[] transformedStitchedTilePoint = new double[ localTilePosition.length ];
				stitchedTile.getTransform().apply( localTilePosition, transformedStitchedTilePoint );

				final double[] transformedGroundtruthTilePoint = new double[ localTilePosition.length ];
				TransformedTileOperations.getTileTransform( groundtruthTile, false ).apply( localTilePosition, transformedGroundtruthTilePoint );

				final PointMatch pointMatch = new PointMatch( new Point( transformedStitchedTilePoint ), new Point( transformedGroundtruthTilePoint ) );

				if ( !tileToPointMatches.containsKey( groundtruthTile.getIndex() ) )
					tileToPointMatches.put( groundtruthTile.getIndex(), new HashSet<>() );
				tileToPointMatches.get( groundtruthTile.getIndex() ).add( pointMatch );
			}
		}

		final List< PointMatch > pointMatches = new ArrayList<>();
		for ( final Set< PointMatch > tilePointMatches : tileToPointMatches.values() )
			pointMatches.addAll( tilePointMatches );

		System.out.println( "Fitting a stitched->groundtruth model using " + pointMatches.size() + " point matches" );

		final Model< M > model = TileModelFactory.createRigidModel( tileDimensions.numDimensions() );
		model.fit( pointMatches );
		PointMatch.apply( pointMatches, model );

		System.out.println( System.lineSeparator() + String.format(
				"Overall solution stats: avg.error=%.2f, max.error=%.2f",
				PointMatch.meanDistance( pointMatches ),
				PointMatch.maxDistance( pointMatches )
			) );

		System.out.println( System.lineSeparator() + "Per-tile stats:" );
		for ( final Entry< Integer, Set< PointMatch > > tilePointMatchesEntry : tileToPointMatches.entrySet() )
		{
			System.out.println( String.format(
					"  #%d: avg.error=%.2f, max.error=%.2f",
					tilePointMatchesEntry.getKey(),
					PointMatch.meanDistance( tilePointMatchesEntry.getValue() ),
					PointMatch.maxDistance( tilePointMatchesEntry.getValue() )
				) );
		}
	}
}
