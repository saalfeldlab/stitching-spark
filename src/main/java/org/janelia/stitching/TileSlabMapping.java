package org.janelia.stitching;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import bdv.img.TpsTransformWrapper;
import net.imglib2.util.Util;

public class TileSlabMapping implements Serializable
{
	private static final long serialVersionUID = 5234507500482092480L;

	private final Map< String, double[] > slabsMin;
	private final Map< String, TpsTransformWrapper > slabsTransforms;

	private final Map< Integer, String > tileIndexToSlab;

	public TileSlabMapping(
			final List< Map< String, TileInfo[] > > slabsTilesChannels,
			final Map< String, double[] > slabsMin,
			final Map< String, TpsTransformWrapper > slabsTransforms )
	{
		this.slabsMin = slabsMin;
		this.slabsTransforms = slabsTransforms;

		tileIndexToSlab = new HashMap<>();
		for ( final Entry< String, TileInfo[] > slabTiles : slabsTilesChannels.get( 0 ).entrySet() )
			for ( final TileInfo slabTile : slabTiles.getValue() )
				tileIndexToSlab.put( slabTile.getIndex(), slabTiles.getKey() );
	}

	public String getSlab( final TileInfo tile )
	{
		return tileIndexToSlab.get( tile.getIndex() );
	}

	public double[] getSlabMin( final String slab )
	{
		// for C1, all slabMin are zeroes
		final double[] slabMin = slabsMin.get( slab );
		if ( !Util.isApproxEqual( Arrays.stream( slabMin ).min().getAsDouble(), 0, 1e-10 ) || !Util.isApproxEqual( Arrays.stream( slabMin ).max().getAsDouble(), 0, 1e-10 ) )
			throw new RuntimeException( "slabMin should be 0" );
		return slabMin;
	}

	public TpsTransformWrapper getSlabTransform( final String slab )
	{
		return slabsTransforms.get( slab );
	}
}
