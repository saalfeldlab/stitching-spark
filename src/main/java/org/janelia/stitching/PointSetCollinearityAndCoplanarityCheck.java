package org.janelia.stitching;

import java.util.Collection;
import java.util.Iterator;

import net.imglib2.RealPoint;
import net.imglib2.util.Util;

public class PointSetCollinearityAndCoplanarityCheck
{
	private static double EPSILON = 1e-5;

	public static boolean testCollinearity( final Collection< RealPoint > points )
	{
		// find the line equation in the two-point form
		final Iterator< RealPoint > pointsIterator = points.iterator();
		final RealPoint pointA = pointsIterator.next(), pointB = pointsIterator.next();
		final int dim = pointA.numDimensions();
		final double[] numerators = new double[ dim ], denominators = new double[ dim ];
		for ( int d = 0; d < dim; ++d )
		{
			numerators[ d ] = pointA.getDoublePosition( d );
			denominators[ d ] = pointB.getDoublePosition( d ) - pointA.getDoublePosition( d );
		}

		final double[] results = new double[ dim ];
		for ( final RealPoint point : points )
		{
			// substitute numbers into the equations and compute
			for ( int d = 0; d < dim; ++d )
			{
				if ( Util.isApproxEqual( denominators[ d ], 0, EPSILON ) )
				{
					// vertical/horizontal line
					if ( Util.isApproxEqual( point.getDoublePosition( d ), numerators[ d ], EPSILON ) )
						results[ d ] = denominators[ d ] > 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
					else
						return false;
				}
				else
				{
					results[ d ] = ( point.getDoublePosition( d ) - numerators[ d ] ) / denominators[ d ];
				}
			}

			// check the equality
			int lastFiniteResultIndex = -1;
			for ( int d = 0; d < dim; ++d )
			{
				if ( Double.isFinite( results[ d ] ) )
				{
					if ( lastFiniteResultIndex != -1 && !Util.isApproxEqual( results[ d ], results[ lastFiniteResultIndex ], EPSILON ) )
						return false;
					lastFiniteResultIndex = d;
				}
			}
		}

		return true;
	}
}
