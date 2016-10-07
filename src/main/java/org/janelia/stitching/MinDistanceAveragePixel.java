package org.janelia.stitching;

import java.util.Map;
import java.util.Map.Entry;

import mpicbg.stitching.fusion.PixelFusion;
import net.imglib2.RealInterval;

/**
 * Implements per-pixel fusion with min-distance strategy.
 * When there are multiple pixel values falling into the same cell, it takes an average of them.
 *
 * @author Igor Pisarev
 */

public class MinDistanceAveragePixel implements PixelFusion
{
	final Map< Integer, RealInterval > imagesLocation;

	double valueSum;
	int count;

	/**
	 * Instantiates the pixel strategy.
	 *
	 * @param imagesLocaiton - a map of imageId to its interval (in a global space)
	 */
	public MinDistanceAveragePixel( final Map< Integer, RealInterval > imagesLocation )
	{
		this.imagesLocation = imagesLocation;
		clear();
	}

	@Override
	public void clear() { valueSum = count = 0;	}

	@Override
	public PixelFusion copy() { return new MinDistanceAveragePixel( imagesLocation ); }

	@Override
	public double getValue()
	{
		if ( count == 0 )
			return 0;
		return ( valueSum / count );
	}

	@Override
	public void addValue( final double value, final int imageId, final double[] localPosition )
	{
		// transform it to a global position
		final double[] globalPosition = new double[ localPosition.length ];
		for ( int d = 0; d < globalPosition.length; d++ )
			globalPosition[ d ] = localPosition[ d ] + imagesLocation.get( imageId ).realMin( d );

		// identify overlaps
		int overlaps = 0;
		boolean needPutOverlappingPixel = false;
		for ( final Entry<Integer, RealInterval> entry : imagesLocation.entrySet() )
		{
			if ( entry.getKey().intValue() == imageId )
				continue;

			boolean overlap = true;
			for ( int d = 0; d < globalPosition.length; d++ )
			{
				if ( globalPosition[ d ] < entry.getValue().realMin( d ) || globalPosition[ d ] > entry.getValue().realMax( d ) )
				{
					overlap = false;
					break;
				}
			}

			if ( overlap )
			{
				overlaps++;

				final int[] inds = new int[] { imageId, entry.getKey().intValue() };
				double minDistance = Double.MAX_VALUE;
				int minDistanceTileIndex = -1;

				for ( int d = 0; d < globalPosition.length; d++ )
				{
					for ( int j = 0; j < 2; j++ )
					{
						final double distance = Math.min( globalPosition[ d ] - imagesLocation.get( inds[ j ] ).realMin( d ), imagesLocation.get( inds[ j ] ).realMax( d ) - globalPosition[ d ] );
						if ( minDistance >= distance )
						{
							minDistance = distance;
							minDistanceTileIndex = inds[ ( j + 1 ) % 2 ];
						}
					}
				}

				if ( minDistanceTileIndex == imageId )
				{
					needPutOverlappingPixel = true;
					break;
				}
			}
		}

		if ( overlaps == 0 || needPutOverlappingPixel )
		{
			valueSum += value;
			count++;
		}
	}
}
