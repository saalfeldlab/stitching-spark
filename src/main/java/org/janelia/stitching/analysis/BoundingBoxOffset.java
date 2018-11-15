package org.janelia.stitching.analysis;

import java.util.Arrays;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileOperations;

import net.imglib2.RealInterval;
import net.imglib2.util.Intervals;

public class BoundingBoxOffset
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final TileInfo[] tilesWithZeroMin = dataProvider.loadTiles( args[ 0 ] );
		final TileInfo[] tilesWithOffset = dataProvider.loadTiles( args[ 1 ] );

		final RealInterval boundingBoxWithZeroMin = TileOperations.getRealCollectionBoundaries( tilesWithZeroMin );
		final RealInterval boundingBoxWithOffset = TileOperations.getRealCollectionBoundaries( tilesWithOffset );

		final double[] offset = new double[ Math.max( boundingBoxWithZeroMin.numDimensions(), boundingBoxWithOffset.numDimensions() ) ];
		for ( int d = 0; d < offset.length; ++d )
			offset[ d ] = boundingBoxWithOffset.realMin( d ) - boundingBoxWithZeroMin.realMin( d );

		System.out.println( "1st bounding box: " + Arrays.toString( Intervals.minAsDoubleArray( boundingBoxWithZeroMin ) ) );
		System.out.println( "2nd bounding box: " + Arrays.toString( Intervals.minAsDoubleArray( boundingBoxWithOffset ) ) );

		System.out.println( "Relative offset for the second tile configuration: " + Arrays.toString( offset ) );
	}
}
