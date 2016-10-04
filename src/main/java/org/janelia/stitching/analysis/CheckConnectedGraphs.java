package org.janelia.stitching.analysis;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.janelia.stitching.Boundaries;
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
		final TileInfo[] tileInfos = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );

		System.out.println( "Finding overlapping pairs.." );
		final List< TilePair > overlappingPairs = TileOperations.findOverlappingTiles( tileInfos );
		System.out.println( "Total pairs = " + overlappingPairs.size() );

		final List< TilePair > adjPairs = new ArrayList<>();

		for ( final TilePair pair : overlappingPairs )
		{
			final Boundaries overlap = TileOperations.getOverlappingRegionGlobal( pair.first(), pair.second() );

			final boolean[] shortEdges = new boolean[overlap.numDimensions() ];
			for ( int d = 0; d < overlap.numDimensions(); d++ )
			{
				final int maxPossibleOverlap = ( int ) Math.min( pair.first().getSize( d ), pair.second().getSize( d ) );
				if ( overlap.dimension( d ) < maxPossibleOverlap / 2 )
					shortEdges[d] = true;
			}

			if (
					( !shortEdges[0] || shortEdges[1] || shortEdges[2] ) && // x
					( shortEdges[0] || !shortEdges[1] || shortEdges[2] ) && // y
					( shortEdges[0] || shortEdges[1] || !shortEdges[2] ) && // z

					true
					)
			{
				// non-adjacent shift
				continue;
			}
			else
			{
				adjPairs.add( pair );
			}
		}

		System.out.println( "Adjacent pairs = " + adjPairs.size() );







		// Create fake tile objects so that they don't hold any image data
		final TreeMap< Integer, Tile< ? > > fakeTileImagesMap = new TreeMap<>();
		for ( final TilePair pair : adjPairs )
		{
			for ( final TileInfo tileInfo : pair.toArray() )
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
		}






		final ArrayList< Tile< ? > > tiles = new ArrayList< >();
		final Set< Tile< ? > > tilesSet = new HashSet< >();
		for ( final TilePair pair : adjPairs )
		{
			final Tile t1 = fakeTileImagesMap.get( pair.first().getIndex() );
			final Tile t2 = fakeTileImagesMap.get( pair.second().getIndex() );

			final Point p1 = new Point( new double[ pair.first().numDimensions() ] );
			final Point p2 = new Point( new double[ pair.second().numDimensions() ] );

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
		int graphSizesSum = 0;
		for ( final Set< Tile< ? > > graph : graphs )
		{
			graphsSize.add( graph.size() );
			graphSizesSum += graph.size();
		}
		Collections.sort( graphsSize );
		Collections.reverse( graphsSize );
		System.out.println( "Tiles total = " + tileInfos.length + ",   graphs=" + graphs.size() + ",   sum="+graphSizesSum );
		System.out.println( graphsSize );
	}
}
