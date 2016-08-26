package org.janelia.stitching;

import java.io.Serializable;
import java.util.ArrayList;

import mpicbg.models.PointMatch;

public class PairwiseMatches implements Serializable
{
	private static final long serialVersionUID = -5545968755784993282L;

	private TilePair tilePair;
	private ArrayList< PointMatch >[][] matches;

	public PairwiseMatches( final TilePair tilePair, final ArrayList< PointMatch >[][] matches )
	{
		this.tilePair = tilePair;
		this.matches = matches;
	}

	protected PairwiseMatches() { }

	public TilePair getTilePair() { return tilePair; }
	public ArrayList< PointMatch >[][] getMatches() { return matches; }
}
