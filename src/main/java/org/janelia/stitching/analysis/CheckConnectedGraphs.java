package org.janelia.stitching.analysis;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileModelFactory;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.TilePair;
import org.janelia.util.Conversions;

import ij.ImagePlus;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import mpicbg.models.Tile;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;

/**
 * @author Igor Pisarev
 */

public class CheckConnectedGraphs
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] tileInfos = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );

		final List< Integer > graphsSize = connectedComponentsSize( tileInfos );

		int graphSizesSum = 0;
		for ( final Integer graphSize : graphsSize )
			graphSizesSum += graphSize;

		System.out.println( "Tiles total = " + tileInfos.length + ",   graphs=" + graphsSize.size() + ",   sum="+graphSizesSum );
		System.out.println( graphsSize );

	}



	public static List< Integer > connectedComponentsSize( final TileInfo[] tileInfos )
	{
		final List< TilePair > overlappingPairs = TileOperations.findOverlappingTiles( tileInfos );
		final List< TilePair > adjPairs = FilterAdjacentShifts.filterAdjacentPairs( overlappingPairs );

		// Create fake tile objects so that they don't hold any image data
		final TreeMap< Integer, Tile< ? > > fakeTileImagesMap = new TreeMap<>();
		for ( final TileInfo tileInfo : tileInfos )
		{
			if ( !fakeTileImagesMap.containsKey( tileInfo.getIndex() ) )
			{
				try
				{
					final ImageCollectionElement e = new ImageCollectionElement( new File( tileInfo.getFilePath() ), tileInfo.getIndex() );
					e.setOffset( Conversions.toFloatArray( tileInfo.getPosition() ) );
					e.setDimensionality( tileInfo.numDimensions() );
					e.setModel( TileModelFactory.createDefaultModel( tileInfo.numDimensions() ) );
					final ImagePlus fakeImage = new ImagePlus( tileInfo.getIndex().toString(), (java.awt.Image)null );
					final Tile< ? > fakeTile = new ImagePlusTimePoint( fakeImage, e.getIndex(), 1, e.getModel(), e );
					fakeTileImagesMap.put( tileInfo.getIndex(), fakeTile );
				}
				catch ( final Exception e ) {
					e.printStackTrace();
				}
			}
		}

		final ArrayList< Tile< ? > > tiles = new ArrayList<>();
		final Set< Tile< ? > > tilesSet = new HashSet<>();
		for ( final TilePair pair : adjPairs )
		{
			final Tile< ? > t1 = fakeTileImagesMap.get( pair.getA().getIndex() );
			final Tile< ? > t2 = fakeTileImagesMap.get( pair.getB().getIndex() );

			final Point p1 = new Point( new double[ pair.getA().numDimensions() ] );
			final Point p2 = new Point( new double[ pair.getB().numDimensions() ] );

			t1.addMatch( new PointMatch( p1, p2 ) );
			t2.addMatch( new PointMatch( p2, p1 ) );

			t1.addConnectedTile( t2 );
			t2.addConnectedTile( t1 );

			if (!tilesSet.contains(t1))
			{
				tilesSet.add( t1 );
				tiles.add( t1 );
			}

			if (!tilesSet.contains(t2))
			{
				tilesSet.add( t2 );
				tiles.add( t2 );
			}
		}

		final ArrayList< Set< Tile< ? > > > graphs = Tile.identifyConnectedGraphs( tiles );
		final ArrayList< Integer > graphsSize = new ArrayList<>();
		for ( final Set< Tile< ? > > graph : graphs )
			graphsSize.add( graph.size() );

		for ( final Tile< ? > tile : fakeTileImagesMap.values() )
			if ( tile.getConnectedTiles().isEmpty() )
				graphsSize.add( 1 );

		Collections.sort( graphsSize );
		Collections.reverse( graphsSize );

		return graphsSize;
	}
}
