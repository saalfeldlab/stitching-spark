package org.janelia.stitching;

import java.io.Serializable;

import mpicbg.stitching.PairWiseStitchingResult;
import scala.Tuple2;

/**
 * @author pisarevi
 *
 */

public class SerializablePairWiseStitchingResult implements Serializable {

	private static final long serialVersionUID = -8084090265269616284L;
	
	private Tuple2< TileInfo, TileInfo > pairOfTiles;
	private float[] offset;
	private float crossCorrelation, phaseCorrelation;
	private boolean isValidOverlap = true;

	public SerializablePairWiseStitchingResult( final Tuple2< TileInfo, TileInfo > pairOfTiles, final float[] offset, final float crossCorrelation, final float phaseCorrelation ) {
		init( pairOfTiles, offset, crossCorrelation, phaseCorrelation );
	}
	
	public SerializablePairWiseStitchingResult( final Tuple2< TileInfo, TileInfo > pairOfTiles, PairWiseStitchingResult other ) {
		init( pairOfTiles, other.getOffset(), other.getCrossCorrelation(), other.getPhaseCorrelation() );
	}
	
	protected SerializablePairWiseStitchingResult() { }
	
	private void init( final Tuple2< TileInfo, TileInfo > pairOfTiles, final float[] offset, final float crossCorrelation, final float phaseCorrelation ) {
		this.pairOfTiles = pairOfTiles;
		this.offset = offset;
		this.crossCorrelation = crossCorrelation;
		this.phaseCorrelation = phaseCorrelation;
	}
	
	public Tuple2< TileInfo, TileInfo > getPairOfTiles() { return pairOfTiles; }
	public int getNumDimensions() { return offset.length; }
	public float[] getOffset() { return offset; }
	public float getOffset( final int dim ) { return offset[ dim ]; }
	public float getCrossCorrelation() { return crossCorrelation; }
	public float getPhaseCorrelation() { return phaseCorrelation; }
	
	public boolean getIsValidOverlap() { return isValidOverlap; }
	public void setIsValidOverlap( final boolean isValidOverlap ) { this.isValidOverlap = isValidOverlap; }
}
