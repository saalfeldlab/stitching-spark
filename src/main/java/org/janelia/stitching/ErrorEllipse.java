package org.janelia.stitching;

import java.util.Arrays;
import java.util.Optional;

import org.ojalgo.matrix.decomposition.Eigenvalue;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;

import mpicbg.imglib.custom.OffsetValidator;
import net.imglib2.FinalRealInterval;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.AffineTransform;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;

public class ErrorEllipse implements OffsetValidator
{
	private final double[] offsetsMeanValues;
	private final double[][] offsetsCovarianceMatrix;

	private final double[] eigenValues;
	private final double[][] eigenVectors;

	private final AffineGet transform;

	private InvertibleRealTransform preTransform, postTransform;

	private final double[] ellipseCenter;
	private final double[] ellipseRadius;
	private final double[][] uncertaintyVectors;

	public ErrorEllipse(
			final double searchRadiusMultiplier,
			final double[] offsetsMeanValues,
			final double[][] offsetsCovarianceMatrix ) throws PipelineExecutionException
	{
		this.offsetsMeanValues = offsetsMeanValues;
		this.offsetsCovarianceMatrix = offsetsCovarianceMatrix;

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

        this.transform = buildTransform();
	}

    @SuppressWarnings( "unchecked" )
	private < A extends AffineGet & AffineSet > A buildTransform()
    {
    	final int dim = numDimensions();

    	final A transform;
    	switch ( dim )
        {
    	case 2:
    		transform = ( A ) new AffineTransform2D();
        	break;
    	case 3:
    		transform = ( A ) new AffineTransform3D();
        	break;
    	default:
    		transform = ( A ) new AffineTransform( dim );
        	break;
        }

    	// affine part
    	for ( int dRow = 0; dRow < dim; ++dRow )
        	for ( int dCol = 0; dCol < dim; ++dCol )
        		transform.set( uncertaintyVectors[ dCol ][ dRow ], dRow, dCol );

    	// translation part
        for ( int dRow = 0; dRow < dim; ++dRow )
        	transform.set( ellipseCenter[ dRow ], dRow, dim );

    	return transform;
    }

	/**
	 * Estimates bounding box using corner points of the unit sphere bounding box:
	 * 1. Get a rotated bounding box of the ellipse
	 * 2. Get a bounding box (parallel to XYZ) of the rotated bounding box so the ellipse is fully contained inside the new box
	 *
	 * @return
	 */
	public RealInterval estimateBoundingBox()
	{
        final double[] unitMin = new double[ numDimensions() ], unitMax = new double[ numDimensions() ];
        Arrays.fill( unitMin, -1 );
        Arrays.fill( unitMax, 1 );
        final RealInterval unitInterval = new FinalRealInterval( unitMin, unitMax );
        return TileOperations.getTransformedBoundingBoxReal( unitInterval, getDecoratedTransform() );
	}

	@Override
	public boolean testOffset( final double... offset )
	{
		return getUnitSphereCoordinates( offset ) <= 1;
	}

	double getUnitSphereCoordinates( final double... offset )
	{
		final double[] transformedOffset = offset.clone();
		getDecoratedTransform().applyInverse( transformedOffset, transformedOffset );

        // calculate unit sphere coordinates
        double coordsSumSquared = 0;
        for ( int d = 0; d < transformedOffset.length; ++d )
        	coordsSumSquared += Math.pow( transformedOffset[ d ], 2 );

        return Math.sqrt( coordsSumSquared );
	}

	@Override
	public int numDimensions()
	{
		return offsetsMeanValues.length;
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

	public void setPreTransform( final InvertibleRealTransform preTransform )
	{
		this.preTransform = preTransform;
	}

	public InvertibleRealTransform getPreTransform()
	{
		return preTransform;
	}

	public void setPostTransform( final InvertibleRealTransform postTransform )
	{
		this.postTransform = postTransform;
	}

	public InvertibleRealTransform getPostTransform()
	{
		return postTransform;
	}

	private InvertibleRealTransform getDecoratedTransform()
	{
		final InvertibleRealTransformSequence decoratedTransform = new InvertibleRealTransformSequence();
		if ( preTransform != null ) decoratedTransform.add( preTransform );
		decoratedTransform.add( transform );
		if ( postTransform != null ) decoratedTransform.add( postTransform );
        return decoratedTransform;
	}
}
