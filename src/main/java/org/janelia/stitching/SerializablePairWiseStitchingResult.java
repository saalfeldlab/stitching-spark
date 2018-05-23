package org.janelia.stitching;

import java.io.Serializable;

public class SerializablePairWiseStitchingResult implements Serializable {

	private static final long serialVersionUID = -8084090265269616284L;

	private SubdividedTileBoxPair tileBoxPair;
	private PointPair pointPair;
	private float[] offset;
	private double[] displacement;
	private float crossCorrelation;
	private Float phaseCorrelation;
	private Double variance;
	private boolean isValidOverlap = true;

	public SerializablePairWiseStitchingResult( final SubdividedTileBoxPair tileBoxPair, final float[] offset, final float crossCorrelation ) {
		this( tileBoxPair, offset, crossCorrelation, null, null );
	}

	public SerializablePairWiseStitchingResult( final SubdividedTileBoxPair tileBoxPair, final float[] offset, final float crossCorrelation, final Float phaseCorrelation ) {
		this( tileBoxPair, offset, crossCorrelation, phaseCorrelation, null );
	}

	public SerializablePairWiseStitchingResult( final SubdividedTileBoxPair tileBoxPair, final float[] offset, final float crossCorrelation, final Float phaseCorrelation, final Double variance ) {
		this.tileBoxPair = tileBoxPair;
		this.offset = offset;
		this.crossCorrelation = crossCorrelation;
		this.phaseCorrelation = phaseCorrelation;
		this.variance = variance;
	}

	protected SerializablePairWiseStitchingResult() { }

	public int getNumDimensions() { return offset.length; }

	public float[] getOffset() { return offset; }
	public void setOffset( final float[] offset )
	{
		for ( int d = 0; d < Math.max( this.offset.length, offset.length ); ++d )
			this.offset[ d ] = offset[ d ];
	}

	public float getOffset( final int dim ) { return offset[ dim ]; }
	public void setOffset( final float val, final int dim ) { offset[ dim ] = val; }

	public float getCrossCorrelation() { return crossCorrelation; }
	public Float getPhaseCorrelation() { return phaseCorrelation; }

	public SubdividedTileBoxPair getTileBoxPair() { return tileBoxPair; }
	public void setTileBoxPair( final SubdividedTileBoxPair tileBoxPair ) { this.tileBoxPair = tileBoxPair; }

	public PointPair getPointPair() { return pointPair; }
	public void setPointPair( final PointPair pointPair ) { this.pointPair = pointPair; }

	public Double getVariance() { return variance; }
	public void setVariance( final Double variance ) { this.variance = variance; }

	public double[] getDisplacement() { return displacement; }
	public void setDisplacement( final double[] displacement ) { this.displacement = displacement; }

	public boolean getIsValidOverlap() { return isValidOverlap; }
	public void setIsValidOverlap( final boolean isValidOverlap ) { this.isValidOverlap = isValidOverlap; }

	public void swap()
	{
		tileBoxPair.swap();
		for ( int d = 0; d < offset.length; d++ )
			offset[ d ] *= -1;
	}

	public boolean isNull()
	{
		return tileBoxPair == null;
	}
}
