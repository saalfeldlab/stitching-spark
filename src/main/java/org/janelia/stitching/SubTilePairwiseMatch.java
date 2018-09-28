package org.janelia.stitching;

import mpicbg.models.IndexedTile;

public class SubTilePairwiseMatch
{
	private final IndexedTile< ? > fixedTile, movingTile;
	private final SerializablePairWiseStitchingResult pairwiseResult;

	public SubTilePairwiseMatch( final IndexedTile< ? > fixedTile, final IndexedTile< ? > movingTile, final SerializablePairWiseStitchingResult pairwiseResult )
	{
		this.fixedTile = fixedTile;
		this.movingTile = movingTile;
		this.pairwiseResult = pairwiseResult;
	}

	public IndexedTile< ? > getFixedTile() { return fixedTile; }
	public IndexedTile< ? > getMovingTile() { return movingTile; }
	public SerializablePairWiseStitchingResult getPairwiseResult() { return pairwiseResult; }
}
