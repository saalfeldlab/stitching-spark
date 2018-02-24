package org.janelia.stitching;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import bdv.img.TpsTransformWrapper;

public class TileSlabMapping implements Serializable
{
	private static final long serialVersionUID = 5234507500482092480L;

	/*public static TileSlabMapping instance;

	public static void createInstance(
			final List< Map< String, TileInfo[] > > slabsTilesChannels,
			final Map< String, double[] > slabsMin,
			final Map< String, TpsTransformWrapper > slabsTransforms )
	{
		instance = new TileSlabMapping( slabsTilesChannels, slabsMin, slabsTransforms );
	}

	public static TileSlabMapping getInstance()
	{
		return instance;
	}*/

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
		return slabsMin.get( slab );
	}

	public TpsTransformWrapper getSlabTransform( final String slab )
	{
		return slabsTransforms.get( slab );
	}
}
