package org.janelia.stitching;

import java.io.Serializable;

import mpicbg.models.Point;
import net.imglib2.util.Pair;

/**
 * Holds an ordered pair of {@link Point} objects.
 *
 * @author Igor Pisarev
 */

public class PointPair implements Pair< Point, Point >, Cloneable, Serializable
{
	private static final long serialVersionUID = -8431265615061089934L;

	private Point[] pointPair;

	public PointPair( final Point p1, final Point p2 )
	{
		pointPair = new Point[] { p1, p2 };
	}

	protected PointPair() { }

	@Override
	public Point getA()
	{
		return pointPair[ 0 ];
	}

	@Override
	public Point getB()
	{
		return pointPair[ 1 ];
	}

	public Point[] toArray()
	{
		return pointPair.clone();
	}

	@Override
	public PointPair clone()
	{
		return new PointPair( pointPair[ 0 ].clone(), pointPair[ 1 ].clone() );
	}
}
