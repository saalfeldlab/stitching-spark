package org.janelia.stitching;

import java.io.Serializable;

public class SerializablePairWiseStitchingResult implements Serializable {

	private static final long serialVersionUID = -8084090265269616284L;

	private TilePair tilePair;
//	private PointPair pointPair;
	private float[] offset;
	private double[] displacement;
	private float crossCorrelation;
	private Float phaseCorrelation;
	private Double variance;
	private boolean isValidOverlap = true;

	public SerializablePairWiseStitchingResult( final TilePair tilePair, final float[] offset, final float crossCorrelation ) {
		this( tilePair, offset, crossCorrelation, null, null );
	}

	public SerializablePairWiseStitchingResult( final TilePair tilePair, final float[] offset, final float crossCorrelation, final Float phaseCorrelation ) {
		this( tilePair, offset, crossCorrelation, phaseCorrelation, null );
	}

	public SerializablePairWiseStitchingResult( final TilePair tilePair, final float[] offset, final float crossCorrelation, final Float phaseCorrelation, final Double variance ) {
		this.tilePair = tilePair;
		this.offset = offset;
		this.crossCorrelation = crossCorrelation;
		this.phaseCorrelation = phaseCorrelation;
		this.variance = variance;
	}

//	public SerializablePairWiseStitchingResult( final TilePair pairOfTiles, final PairWiseStitchingResult other ) {
//		this( pairOfTiles, other.getOffset(), other.getCrossCorrelation(), other.getPhaseCorrelation() );
//	}

	protected SerializablePairWiseStitchingResult() { }

	public int getNumDimensions() { return offset.length; }
	public float[] getOffset() { return offset; }
	public float getOffset( final int dim ) { return offset[ dim ]; }
	public float getCrossCorrelation() { return crossCorrelation; }
	public Float getPhaseCorrelation() { return phaseCorrelation; }

	public TilePair getTilePair() { return tilePair; }
	public void setTilePair( final TilePair tilePair ) { this.tilePair = tilePair; }

//	public PointPair getPointPair() { return pointPair; }
//	public void setPointPair( final PointPair pointPair ) { this.pointPair = pointPair; }

	public Double getVariance() { return variance; }
	public void setVariance( final Double variance ) { this.variance = variance; }

	public double[] getDisplacement() { return displacement; }
	public void setDisplacement( final double[] displacement ) { this.displacement = displacement; }

	public boolean getIsValidOverlap() { return isValidOverlap; }
	public void setIsValidOverlap( final boolean isValidOverlap ) { this.isValidOverlap = isValidOverlap; }

	public void swap()
	{
		tilePair.swap();
		for ( int d = 0; d < offset.length; d++ )
			offset[ d ] *= -1;
	}

	public boolean isNull()
	{
		return tilePair == null;
	}
}
