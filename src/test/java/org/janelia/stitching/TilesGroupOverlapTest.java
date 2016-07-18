package org.janelia.stitching;

import org.junit.Assert;
import org.junit.Test;

public class TilesGroupOverlapTest {
	
	@Test
	public void test() {
		
		final TileInfo[] tiles = new TileInfo[ 5 ];
	
		for ( int i = 0; i < tiles.length; i++ )
			tiles[ i ] = new TileInfo();
		
		tiles[ 0 ].setPosition( new float[] { 0.f, 0.f, 0.f } ); 												 tiles[ 0 ].setSize( new int[] { 991, 992, 880 } );
		tiles[ 1 ].setPosition( new float[] { 716.932762003862f, -694.0887500300357f, -77.41783189603937f } );   tiles[ 1 ].setSize( new int[] { 991, 992, 953 } );
		tiles[ 2 ].setPosition( new float[] { 190.0026915705376f, -765.0362103552288f, -79.91642981085911f } );  tiles[ 2 ].setSize( new int[] { 991, 992, 953 } );
		tiles[ 3 ].setPosition( new float[] { 644.9707923831013f, -1160.9781698781098f, -79.17565799404196f } ); tiles[ 3 ].setSize( new int[] { 991, 992, 953 } );
		tiles[ 4 ].setPosition( new float[] { 1419.1341448553956f, -1337.414351167819f, -76.84995500091385f } ); tiles[ 4 ].setSize( new int[] { 991, 992, 953 } );
		
		Assert.assertEquals( 7, Utils.findOverlappingTiles( tiles ).size() );
	}
}
