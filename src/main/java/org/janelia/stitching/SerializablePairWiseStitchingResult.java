package org.janelia.stitching;

import java.io.Serializable;

public class SerializablePairWiseStitchingResult implements Serializable {

	private static final long serialVersionUID = -8084090265269616284L;

	private TilePair tilePair;
	private float[] offset;
	private float crossCorrelation;
	private Float phaseCorrelation;
	private boolean isValidOverlap = true;

	public SerializablePairWiseStitchingResult( final TilePair tilePair, final float[] offset, final float crossCorrelation ) {
		this( tilePair, offset, crossCorrelation, null );
	}

	public SerializablePairWiseStitchingResult( final TilePair tilePair, final float[] offset, final float crossCorrelation, final Float phaseCorrelation ) {
		this.tilePair = tilePair;
		this.offset = offset;
		this.crossCorrelation = crossCorrelation;
		this.phaseCorrelation = phaseCorrelation;
	}

//	public SerializablePairWiseStitchingResult( final TilePair pairOfTiles, final PairWiseStitchingResult other ) {
//		this( pairOfTiles, other.getOffset(), other.getCrossCorrelation(), other.getPhaseCorrelation() );
//	}

	protected SerializablePairWiseStitchingResult() { }

	public void setTilePair( final TilePair tilePair )
	{
		this.tilePair = tilePair;
	}

	public TilePair getTilePair() { return tilePair; }
	public int getNumDimensions() { return offset.length; }
	public float[] getOffset() { return offset; }
	public float getOffset( final int dim ) { return offset[ dim ]; }
	public float getCrossCorrelation() { return crossCorrelation; }
	public Float getPhaseCorrelation() { return phaseCorrelation; }

	public boolean getIsValidOverlap() { return isValidOverlap; }
	public void setIsValidOverlap( final boolean isValidOverlap ) { this.isValidOverlap = isValidOverlap; }

	public void swap()
	{
		tilePair.swap();
		for ( int d = 0; d < offset.length; d++ )
			offset[ d ] *= -1;
	}
}
