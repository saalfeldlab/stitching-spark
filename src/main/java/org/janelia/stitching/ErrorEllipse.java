package org.janelia.stitching;

import java.util.Arrays;

import org.ojalgo.matrix.BasicMatrix;
import org.ojalgo.matrix.BasicMatrix.Builder;
import org.ojalgo.matrix.PrimitiveMatrix;

import mpicbg.imglib.custom.PointValidator;
import net.imglib2.FinalRealInterval;
import net.imglib2.RealInterval;
import net.imglib2.iterator.IntervalIterator;

public class ErrorEllipse implements PointValidator
{
	private final PrimitiveMatrix transformMatrix;
	private final PrimitiveMatrix inverseTransformMatrix;

	private final RealInterval boundingBox;
	private final double[] ellipseCenter;
	private final double[] ellipseRadius;
	private final double[][] uncertaintyVectors;

	public ErrorEllipse( final double[] ellipseCenter, final double[][] uncertaintyVectors )
	{
		this.ellipseCenter = ellipseCenter;
		this.uncertaintyVectors = uncertaintyVectors;

		ellipseRadius = new double[ ellipseCenter.length ];
		for ( int dRow = 0; dRow < ellipseRadius.length; ++dRow )
		{
			double sumSq = 0;
        	for ( int dCol = 0; dCol < ellipseRadius.length; ++dCol )
        		sumSq += uncertaintyVectors[ dRow ][ dCol ] * uncertaintyVectors[ dRow ][ dCol ];
        	ellipseRadius[ dRow ] = Math.sqrt( sumSq );
		}

        // build a transformation matrix for this ellipse
		final int dim = ellipseCenter.length;
        final BasicMatrix.Factory< PrimitiveMatrix > matrixFactory = PrimitiveMatrix.FACTORY;
        final Builder< PrimitiveMatrix > transformMatrixBuilder = matrixFactory.getBuilder( dim + 1, dim + 1 );
        for ( int dRow = 0; dRow < dim; ++dRow )
        	for ( int dCol = 0; dCol < dim; ++dCol )
        		transformMatrixBuilder.set( dRow, dCol, uncertaintyVectors[ dCol ][ dRow ] );
        for ( int dRow = 0; dRow < dim; ++dRow )
        {
        	transformMatrixBuilder.set( dRow, dim, ellipseCenter[ dRow ] );
        	transformMatrixBuilder.set( dim, dRow, 0 );
        }
        transformMatrixBuilder.set( dim, dim, 1 );

        this.transformMatrix = transformMatrixBuilder.get();
        this.inverseTransformMatrix = transformMatrix.invert();

        this.boundingBox = estimateBoundingBox( transformMatrix );
	}

	@Override
	public boolean testPoint( final double... coords )
	{
		// build a vector with point coordinates
		final BasicMatrix.Factory< PrimitiveMatrix > matrixFactory = PrimitiveMatrix.FACTORY;
        final Builder< PrimitiveMatrix > coordsVectorBuilder = matrixFactory.getBuilder( coords.length + 1, 1 );
        for ( int dRow = 0; dRow < coords.length; ++dRow )
        	coordsVectorBuilder.set( dRow, 0, coords[ dRow ] );
        coordsVectorBuilder.set( coords.length, 0, 1 );
        final PrimitiveMatrix coordsVector = coordsVectorBuilder.get();

        // get the inverse transform to map the the point to the unit sphere
        final PrimitiveMatrix transformedCoordsVector = inverseTransformMatrix.multiply( coordsVector );

        // check whether the transformed point lies within the unit sphere
        double coordsSumSquared = 0;
        for ( int dRow = 0; dRow < coords.length; ++dRow )
        	coordsSumSquared += Math.pow( transformedCoordsVector.get( dRow, 0 ), 2 );
        return coordsSumSquared <= 1;
	}

	public RealInterval getBoundingBox()
	{
		return boundingBox;
	}

	public double[] getEllipseCenter()
	{
		return ellipseCenter;
	}

	public double[] getEllipseRadius()
	{
		return ellipseRadius;
	}

	public double[][] getUncertaintyVectors()
	{
		return uncertaintyVectors;
	}

	public PrimitiveMatrix getTransformMatrix()
	{
		return transformMatrix;
	}

	public PrimitiveMatrix getInverseTransformMatrix()
	{
		return inverseTransformMatrix;
	}

	@Override
	public int numDimensions()
	{
		return ellipseCenter.length;
	}

	private RealInterval estimateBoundingBox( final PrimitiveMatrix transformMatrix )
	{
		// Estimate bounding box using corner points of the unit sphere bounding box:
        //   1. Get a rotated bounding box of the ellipse
        //   2. Get a bounding box (parallel to XYZ) of the rotated bounding box so the ellipse is fully contained inside the new box
		final double[] boundingBoxMin = new double[ numDimensions() ], boundingBoxMax = new double[ numDimensions() ];
		Arrays.fill( boundingBoxMin, Double.MAX_VALUE );
        Arrays.fill( boundingBoxMax, -Double.MAX_VALUE );

		final int[] cornerIntervalDimensions = new int[ numDimensions() ];
		Arrays.fill( cornerIntervalDimensions, 2 );
		final IntervalIterator cornerIntervalIterator = new IntervalIterator( cornerIntervalDimensions );
		final int[] cornerIntervalIteratorPosition = new int[ numDimensions() ], cornerPosition = new int[ numDimensions() ];
		while ( cornerIntervalIterator.hasNext() )
		{
			cornerIntervalIterator.fwd();
			cornerIntervalIterator.localize( cornerIntervalIteratorPosition );
			for ( int d = 0; d < cornerPosition.length; ++d )
				cornerPosition[ d ] = ( cornerIntervalIteratorPosition[ d ] == 0 ? -1 : 1 );

			final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( cornerPosition.length + 1, 1 );
        	for ( int dRow = 0; dRow < cornerPosition.length; ++dRow )
        		vectorBuilder.set( dRow, 0, cornerPosition[ dRow ] );
        	vectorBuilder.set( cornerPosition.length, 0, 1 );

			final PrimitiveMatrix transformedVector = transformMatrix.multiply( vectorBuilder.get() );
			for ( int dRow = 0; dRow < cornerPosition.length; ++dRow )
			{
				boundingBoxMin[ dRow ] = Math.min( transformedVector.get( dRow, 0 ), boundingBoxMin[ dRow ] );
				boundingBoxMax[ dRow ] = Math.max( transformedVector.get( dRow, 0 ), boundingBoxMax[ dRow ] );
			}
		}
        return new FinalRealInterval( boundingBoxMin, boundingBoxMax );
	}
}