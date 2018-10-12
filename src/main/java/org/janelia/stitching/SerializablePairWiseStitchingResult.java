package org.janelia.stitching;

import java.io.Serializable;

public class SerializablePairWiseStitchingResult implements Serializable {

	private static final long serialVersionUID = -8084090265269616284L;

	private SubTilePair subTilePair;
	private AffineTransformPair estimatedFullTileTransformPair;
	private double[] offset;
	private double crossCorrelation;
	private Double phaseCorrelation;
	private Double variance;
	private boolean isValidOverlap = true;

	public SerializablePairWiseStitchingResult( final SubTilePair subTilePair, final double[] offset, final double crossCorrelation ) {
		this( subTilePair, offset, crossCorrelation, null, null );
	}

	public SerializablePairWiseStitchingResult( final SubTilePair subTilePair, final double[] offset, final double crossCorrelation, final Double phaseCorrelation ) {
		this( subTilePair, offset, crossCorrelation, phaseCorrelation, null );
	}

	public SerializablePairWiseStitchingResult( final SubTilePair subTilePair, final double[] offset, final double crossCorrelation, final Double phaseCorrelation, final Double variance ) {
		this.subTilePair = subTilePair;
		this.offset = offset;
		this.crossCorrelation = crossCorrelation;
		this.phaseCorrelation = phaseCorrelation;
		this.variance = variance;

		isValidOverlap = offset != null;
	}

	public int getNumDimensions() { return offset.length; }

	public double[] getOffset() { return offset; }
	public void setOffset( final float[] offset )
	{
		for ( int d = 0; d < Math.max( this.offset.length, offset.length ); ++d )
			this.offset[ d ] = offset[ d ];
	}

	public double getOffset( final int dim ) { return offset[ dim ]; }
	public void setOffset( final double val, final int dim ) { offset[ dim ] = val; }

	public double getCrossCorrelation() { return crossCorrelation; }
	public Double getPhaseCorrelation() { return phaseCorrelation; }

	public SubTilePair getSubTilePair() { return subTilePair; }
	public void setSubTilePair( final SubTilePair subTilePair ) { this.subTilePair = subTilePair; }

	public AffineTransformPair getEstimatedFullTileTransformPair() { return estimatedFullTileTransformPair; }
	public void setEstimatedFullTileTransformPair( final AffineTransformPair estimatedFullTileTransformPair ) { this.estimatedFullTileTransformPair = estimatedFullTileTransformPair; }

	public Double getVariance() { return variance; }
	public void setVariance( final Double variance ) { this.variance = variance; }

	public boolean getIsValidOverlap() { return isValidOverlap; }
	public void setIsValidOverlap( final boolean isValidOverlap ) { this.isValidOverlap = isValidOverlap; }

	public void swap()
	{
		subTilePair.swap();
		for ( int d = 0; d < offset.length; d++ )
			offset[ d ] *= -1;
	}
}
