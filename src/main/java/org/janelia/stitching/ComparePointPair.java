package org.janelia.stitching;

import ij.ImagePlus;
import mpicbg.models.Model;
import mpicbg.stitching.ImagePlusTimePoint;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.util.Pair;

public class ComparePointPair
{
	private final ImagePlusTimePoint impA, impB;
	private float crossCorrelation;
	private boolean validOverlap;

	// the local shift of impB relative to impA
	private float[] relativeShift;

	private SubTilePair subTilePair;
	private Pair< AffineGet, AffineGet > estimatedFullTileTransformPair;

	public ComparePointPair( final ImagePlusTimePoint impA, final ImagePlusTimePoint impB )
	{
		this.impA = impA;
		this.impB = impB;
		this.validOverlap = true;
	}

	public ImagePlusTimePoint getTile1() { return impA; }
	public ImagePlusTimePoint getTile2() { return impB; }

	public ImagePlus getImagePlus1() { return impA.getImagePlus(); }
	public ImagePlus getImagePlus2() { return impB.getImagePlus(); }

	public int getTimePoint1() { return impA.getTimePoint(); }
	public int getTimePoint2() { return impB.getTimePoint(); }

	public Model< ? > getModel1() { return impA.getModel(); }
	public Model< ? > getModel2() { return impB.getModel(); }

	public void setCrossCorrelation( final float r ) { this.crossCorrelation = r; }
	public float getCrossCorrelation() { return crossCorrelation; }

	public void setRelativeShift( final float[] relativeShift ) { this.relativeShift = relativeShift; }
	public float[] getRelativeShift() { return relativeShift; }

	public void setIsValidOverlap( final boolean state ) { this.validOverlap = state; }
	public boolean getIsValidOverlap() { return validOverlap; }

	public void setSubTilePair( final SubTilePair subTilePair ) { this.subTilePair = subTilePair; }
	public SubTilePair getSubTilePair() { return subTilePair; }

	public void setEstimatedFullTileTransformPair( final Pair< AffineGet, AffineGet > estimatedFullTileTransformPair ) { this.estimatedFullTileTransformPair = estimatedFullTileTransformPair; }
	public Pair< AffineGet, AffineGet > getEstimatedFullTileTransformPair() { return estimatedFullTileTransformPair; }
}
