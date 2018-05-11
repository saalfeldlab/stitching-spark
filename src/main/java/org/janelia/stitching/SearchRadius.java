package org.janelia.stitching;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.ojalgo.matrix.BasicMatrix;
import org.ojalgo.matrix.BasicMatrix.Builder;
import org.ojalgo.matrix.PrimitiveMatrix;
import org.ojalgo.matrix.decomposition.Eigenvalue;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;

import mpicbg.imglib.custom.OffsetValidator;
import net.imglib2.FinalRealInterval;
import net.imglib2.RealInterval;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.RealTransform;

public class SearchRadius implements OffsetValidator
{
	private final double[] offsetsMeanValues;
	private final double[][] offsetsCovarianceMatrix;

	private final List< Integer > usedPointsIndexes;
	private final double[] stagePosition;

	private final double[] eigenValues;
	private final double[][] eigenVectors;

	private final PrimitiveMatrix transformMatrix;
	private final PrimitiveMatrix inverseTransformMatrix;

	private final RealInterval boundingBox;
	private final double[] ellipseCenter;
	private final double[] ellipseRadius;
	private final double[][] uncertaintyVectors;

	private RealTransform offsetTransform;

	public SearchRadius(
			final double searchRadiusMultiplier,
			final double[] offsetsMeanValues,
			final double[][] offsetsCovarianceMatrix ) throws PipelineExecutionException
	{
		this( searchRadiusMultiplier, offsetsMeanValues, offsetsCovarianceMatrix, null );
	}

	public SearchRadius(
			final double searchRadiusMultiplier,
			final double[] offsetsMeanValues,
			final double[][] offsetsCovarianceMatrix,
			final List< Integer > usedPointsIndexes ) throws PipelineExecutionException
	{
		this( searchRadiusMultiplier, offsetsMeanValues, offsetsCovarianceMatrix, usedPointsIndexes, new double[ offsetsMeanValues.length ] );
	}

	public SearchRadius(
			final double searchRadiusMultiplier,
			final double[] offsetsMeanValues,
			final double[][] offsetsCovarianceMatrix,
			final List< Integer > usedPointsIndexes,
			final double[] stagePosition ) throws PipelineExecutionException
	{
		this.offsetsMeanValues = offsetsMeanValues;
		this.offsetsCovarianceMatrix = offsetsCovarianceMatrix;
		this.usedPointsIndexes = usedPointsIndexes;
		this.stagePosition = stagePosition;

		final PhysicalStore.Factory< Double, PrimitiveDenseStore > storeFactory = PrimitiveDenseStore.FACTORY;
		final PrimitiveDenseStore matrixStore = storeFactory.makeEye( offsetsCovarianceMatrix.length, offsetsCovarianceMatrix.length );
        for ( int dRow = 0; dRow < offsetsCovarianceMatrix.length; ++dRow )
        	for ( int dCol = 0; dCol < offsetsCovarianceMatrix.length; ++dCol )
        		matrixStore.set( dRow, dCol, offsetsCovarianceMatrix[ dRow ][ dCol ] );

        final Eigenvalue.Factory< Double > eigenFactory = Eigenvalue.PRIMITIVE;
        final Eigenvalue< Double > eigen = eigenFactory.make( matrixStore );
        if ( !eigen.decompose( matrixStore ) )
        	throw new PipelineExecutionException( "Failed to calculate eigenvectors" );

        eigenValues = new double[ offsetsCovarianceMatrix.length ];
        eigen.getEigenvalues( eigenValues, Optional.ofNullable( null ) );

        ellipseRadius = new double[ eigenValues.length ];
        for ( int i = 0; i < ellipseRadius.length; ++i )
        	ellipseRadius[ i ] = Math.sqrt( eigenValues[ i ] ) * searchRadiusMultiplier;

        final MatrixStore< Double > eigenVectorsStore = eigen.getV();
        eigenVectors = new double[ ( int ) eigenVectorsStore.countRows() ][ ( int ) eigenVectorsStore.countColumns() ];
        uncertaintyVectors = new double[ ( int ) eigenVectorsStore.countRows() ][ ( int ) eigenVectorsStore.countColumns() ];
        for ( int dRow = 0; dRow < eigenVectors.length; ++dRow )
        {
        	for ( int dCol = 0; dCol < eigenVectors[ dRow ].length; ++dCol )
        	{
        		eigenVectors[ dCol ][ dRow ] = eigenVectorsStore.get( dRow, dCol );
        		uncertaintyVectors[ dCol ][ dRow ] = eigenVectorsStore.get( dRow, dCol ) * ellipseRadius[ dCol ];
        	}
        }

		for ( int dRow = 0; dRow < numDimensions(); ++dRow )
		{
			double sumSq = 0;
        	for ( int dCol = 0; dCol < numDimensions(); ++dCol )
        		sumSq += uncertaintyVectors[ dRow ][ dCol ] * uncertaintyVectors[ dRow ][ dCol ];
        	if ( Math.abs( Math.sqrt( sumSq ) - ellipseRadius[ dRow ] ) > 1e-8 )
        		throw new RuntimeException( "radius validation failed" );
		}

        ellipseCenter = new double[ offsetsMeanValues.length ];
        for ( int d = 0; d < ellipseCenter.length; ++d )
        	ellipseCenter[ d ] = offsetsMeanValues[ d ];

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

	/**
	 * Estimates bounding box using corner points of the unit sphere bounding box:
	 * 1. Get a rotated bounding box of the ellipse
	 * 2. Get a bounding box (parallel to XYZ) of the rotated bounding box so the ellipse is fully contained inside the new box
	 *
	 * @param transformMatrix
	 * @return
	 */
	private RealInterval estimateBoundingBox( final PrimitiveMatrix transformMatrix )
	{
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

	@Override
	public boolean testOffset( final double... offset )
	{
		return getUnitSphereCoordinates( offset ) <= 1;
	}

	double getUnitSphereCoordinates( final double... offset )
	{
		// if transformation is present, convert the offset to the user-specified coordinate space in which the error ellipse is defined
		final double[] transformedOffset = offset.clone();
		if ( offsetTransform != null )
			offsetTransform.apply( transformedOffset, transformedOffset );

		// build a vector with point coordinates
		final BasicMatrix.Factory< PrimitiveMatrix > matrixFactory = PrimitiveMatrix.FACTORY;
        final Builder< PrimitiveMatrix > coordsVectorBuilder = matrixFactory.getBuilder( transformedOffset.length + 1, 1 );
        for ( int dRow = 0; dRow < transformedOffset.length; ++dRow )
        	coordsVectorBuilder.set( dRow, 0, transformedOffset[ dRow ] );
        coordsVectorBuilder.set( transformedOffset.length, 0, 1 );
        final PrimitiveMatrix coordsVector = coordsVectorBuilder.get();

        // get the inverse transform to map the the point to the unit sphere
        final PrimitiveMatrix transformedCoordsVector = inverseTransformMatrix.multiply( coordsVector );

        // calculate unit sphere coordinates
        double coordsSumSquared = 0;
        for ( int dRow = 0; dRow < transformedOffset.length; ++dRow )
        	coordsSumSquared += Math.pow( transformedCoordsVector.get( dRow, 0 ), 2 );

        return Math.sqrt( coordsSumSquared );
	}

	public RealTransform getOffsetTransform()
	{
		return offsetTransform;
	}

	public void setOffsetTransform( final RealTransform offsetTransform )
	{
		this.offsetTransform = offsetTransform;
	}

	@Override
	public int numDimensions()
	{
		return offsetsMeanValues.length;
	}

	public double[] getStagePosition()
	{
		return stagePosition;
	}

	public List< Integer > getUsedPointsIndexes()
	{
		return usedPointsIndexes;
	}

	public double[] getOffsetsMeanValues()
	{
		return offsetsMeanValues;
	}

	public double[][] getOffsetsCovarianceMatrix()
	{
		return offsetsCovarianceMatrix;
	}

	public double[] getEigenValues()
	{
		return eigenValues;
	}

	public double[][] getEigenVectors()
	{
		return eigenVectors;
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
}
