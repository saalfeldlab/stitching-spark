package org.janelia.stitching;

import java.util.Map;

import mpicbg.stitching.fusion.PixelFusion;
import net.imglib2.Dimensions;

public class CustomBlendingPixel implements PixelFusion
{
	public static double fractionBlended = 0.2;

	final Map< Integer, Dimensions > imageDimensions;
	final double percentScaling;

	double valueSum, weightSum;

	/**
	 * Instantiates the per-pixel blending
	 *
	 * @param images - all input images (the position in the list has to be the same as Id provided by addValue!)
	 */
	public CustomBlendingPixel( final Map< Integer, Dimensions > imageDimensions )
	{
		this( imageDimensions, fractionBlended );
	}
	/**
	 * Instantiates the per-pixel blending
	 *
	 * @param images - all input images (the position in the list has to be the same as Id provided by addValue!)
	 * @param percentScaling - which percentage of the image should be blended ( e.g. 0,3 means 15% on the left and 15% on the right)
	 */
	private CustomBlendingPixel( final Map< Integer, Dimensions > imageDimensions, final double fractionBlended )
	{
		this.imageDimensions = imageDimensions;
		this.percentScaling = fractionBlended;

		// reset
		clear();
	}

	@Override
	public void clear() { valueSum = weightSum = 0;	}

	@Override
	public void addValue( final double value, final int imageId, final double[] localPosition )
	{
		// we are always inside the image, so we do not want 0.0
		final double weight = Math.max( 0.00001, computeWeight( localPosition, imageDimensions.get( imageId ), percentScaling ) );

		weightSum += weight;
		valueSum += value * weight;
	}

	@Override
	public double getValue()
	{
		if ( weightSum == 0 )
			return 0;
		return ( valueSum / weightSum );
	}

	@Override
	public PixelFusion copy() { return new CustomBlendingPixel( imageDimensions ); }

	/**
	 * From SPIM Registration
	 *
	 *
	 * @param location
	 * @param imageDimensions
	 * @param percentScaling
	 * @return
	 */
	final public static double computeWeight( final double[] location, final Dimensions dimensions, final double percentScaling )
	{
		// compute multiplicative distance to the respective borders [0...1]
		double minDistance = 1;

		for ( int dim = 0; dim < location.length; ++dim )
		{
			// the position in the image
			final double localImgPos = location[ dim ];

			// the distance to the border that is closer
			double value = Math.max( 1, Math.min( localImgPos, dimensions.dimension( dim ) - 1 - localImgPos ) );

			final float imgAreaBlend = Math.round( percentScaling * 0.5f * ( dimensions.dimension( dim ) - 1 ) );

			if ( value < imgAreaBlend )
				value = value / imgAreaBlend;
			else
				value = 1;

			minDistance *= value;
		}

		if ( minDistance == 1 )
			return 1;
		else if ( minDistance <= 0 )
			return 0.0000001;
		else
			return ( Math.cos( (1 - minDistance) * Math.PI ) + 1 ) / 2;
	}

}
