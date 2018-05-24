package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Igor Pisarev
 */

public class TilesGroupOverlapTest {

	@Test
	public void test() {

		final TileInfo[] tiles = new TileInfo[ 5 ];

		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ] = new TileInfo();

		tiles[ 0 ].setStagePosition( new double[] { 0., 0., 0. } ); 													tiles[ 0 ].setSize( new long[] { 991, 992, 880 } );
		tiles[ 1 ].setStagePosition( new double[] { 716.932762003862, -694.0887500300357, -77.41783189603937 } );	tiles[ 1 ].setSize( new long[] { 991, 992, 953 } );
		tiles[ 2 ].setStagePosition( new double[] { 190.0026915705376, -765.0362103552288, -79.91642981085911 } );  	tiles[ 2 ].setSize( new long[] { 991, 992, 953 } );
		tiles[ 3 ].setStagePosition( new double[] { 644.9707923831013, -1160.9781698781098, -79.17565799404196 } ); 	tiles[ 3 ].setSize( new long[] { 991, 992, 953 } );
		tiles[ 4 ].setStagePosition( new double[] { 1419.1341448553956, -1337.414351167819, -76.84995500091385 } ); 	tiles[ 4 ].setSize( new long[] { 991, 992, 953 } );

		Assert.assertEquals( 7, TileOperations.findOverlappingTiles( tiles ).size() );
	}
}
