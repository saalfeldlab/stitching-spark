package org.janelia.stitching;

import java.util.List;
import java.util.Optional;

import org.ojalgo.matrix.PrimitiveMatrix;
import org.ojalgo.matrix.decomposition.Eigenvalue;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;

import mpicbg.imglib.custom.PointValidator;
import net.imglib2.RealInterval;

public class SearchRadius implements PointValidator
{
	private final ErrorEllipse errorEllipse;

	private final double[] offsetsMeanValues;
	private final double[][] offsetsCovarianceMatrix;

	private final List< Integer > usedPointsIndexes;
	private final double[] stagePosition;

	private final double[] eigenValues;
	private final double[][] eigenVectors;

	public SearchRadius( final double searchRadiusMultiplier, final double[] offsetsMeanValues, final double[][] offsetsCovarianceMatrix ) throws PipelineExecutionException
	{
		this( searchRadiusMultiplier, offsetsMeanValues, offsetsCovarianceMatrix, null );
	}
	public SearchRadius( final double searchRadiusMultiplier, final double[] offsetsMeanValues, final double[][] offsetsCovarianceMatrix, final List< Integer > usedPointsIndexes ) throws PipelineExecutionException
	{
		this( searchRadiusMultiplier, offsetsMeanValues, offsetsCovarianceMatrix, usedPointsIndexes, null );
	}
	public SearchRadius( final double searchRadiusMultiplier, final double[] offsetsMeanValues, final double[][] offsetsCovarianceMatrix, final List< Integer > usedPointsIndexes, final double[] stagePosition ) throws PipelineExecutionException
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

        final double[] ellipseRadius = new double[ eigenValues.length ];
        for ( int i = 0; i < ellipseRadius.length; ++i )
        	ellipseRadius[ i ] = Math.sqrt( eigenValues[ i ] ) * searchRadiusMultiplier;

        final MatrixStore< Double > eigenVectorsStore = eigen.getV();
        eigenVectors = new double[ ( int) eigenVectorsStore.countRows() ][ ( int ) eigenVectorsStore.countColumns() ];
        final double[][] uncertaintyVectors = new double[ ( int) eigenVectorsStore.countRows() ][ ( int ) eigenVectorsStore.countColumns() ];
        for ( int dRow = 0; dRow < eigenVectors.length; ++dRow )
        {
        	for ( int dCol = 0; dCol < eigenVectors[ dRow ].length; ++dCol )
        	{
        		eigenVectors[ dCol ][ dRow ] = eigenVectorsStore.get( dRow, dCol );
        		uncertaintyVectors[ dCol ][ dRow ] = eigenVectorsStore.get( dRow, dCol ) * ellipseRadius[ dCol ];
        	}
        }

        final double[] ellipseCenter = new double[ offsetsMeanValues.length ];
        for ( int d = 0; d < ellipseCenter.length; ++d )
        	ellipseCenter[ d ] = offsetsMeanValues[ d ] + ( stagePosition != null ? stagePosition[ d ] : 0 );

        errorEllipse = new ErrorEllipse( ellipseCenter, uncertaintyVectors );
	}

	@Override
	public boolean testPoint( final double... coords )
	{
		return errorEllipse.testPoint( coords );
	}

	public RealInterval getBoundingBox()
	{
		return errorEllipse.getBoundingBox();
	}

	public double[] getEllipseCenter()
	{
		return errorEllipse.getEllipseCenter();
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

	public double[] getEllipseRadius()
	{
		return errorEllipse.getEllipseRadius();
	}

	public double[][] getUncertaintyVectors()
	{
		return errorEllipse.getUncertaintyVectors();
	}

	public PrimitiveMatrix getTransformMatrix()
	{
		return errorEllipse.getTransformMatrix();
	}

	public PrimitiveMatrix getInverseTransformMatrix()
	{
		return errorEllipse.getInverseTransformMatrix();
	}

	@Override
	public int numDimensions()
	{
		return offsetsMeanValues.length;
	}
}
