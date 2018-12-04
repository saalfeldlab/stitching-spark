package org.janelia.stitching;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.janelia.stitching.math.WeightedCovariance;
import org.ojalgo.matrix.decomposition.Eigenvalue;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;

import mpicbg.models.PointMatch;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;

public class PointMatchSpatialLayout
{
	private static final double THRESHOLD = 1e-1;

	public static enum SpatialLayout
	{
		Collinear,
		Coplanar,
		Other
	}

	public static SpatialLayout determine( final List< PointMatch > pointMatches )
	{
		final double[] weights = new double[ pointMatches.size() ];
		final Pair< List< double[] >, List< double[] > > samples = new ValuePair<>( new ArrayList<>(), new ArrayList<>() );
		for ( int i = 0; i < pointMatches.size(); ++i )
		{
			samples.getA().add( pointMatches.get( i ).getP1().getL() );
			samples.getB().add( pointMatches.get( i ).getP2().getW() );
			weights[ i ] = pointMatches.get( i ).getWeight();
		}

		final Pair< SpatialLayout, SpatialLayout > layouts = new ValuePair<>(
				determine( samples.getA(), weights ),
				determine( samples.getB(), weights )
			);

		return pickMostRestrictedLayout( layouts.getA(), layouts.getB() );
	}

	public static SpatialLayout determine( final List< double[] > samples, final double[] weights )
	{
		if ( samples.isEmpty() )
			return SpatialLayout.Collinear;

		final int dim = samples.iterator().next().length;

		final WeightedCovariance covariance = new WeightedCovariance( samples, weights );
		final double[][] covarianceMatrix = covariance.getWeightedCovarianceMatrix();

		final PhysicalStore.Factory< Double, PrimitiveDenseStore > storeFactory = PrimitiveDenseStore.FACTORY;
		final PrimitiveDenseStore matrixStore = storeFactory.makeEye( dim, dim );
		for ( int row = 0; row < dim; ++row )
			for ( int col = 0; col < dim; ++col )
				matrixStore.set( row, col, covarianceMatrix[ row ][ col ] );

		final Eigenvalue.Factory< Double > eigenFactory = Eigenvalue.PRIMITIVE;
		final Eigenvalue< Double > eigen = eigenFactory.make( matrixStore );
		if ( !eigen.decompose( matrixStore ) )
			throw new RuntimeException( "Failed to calculate eigenvectors" );

		final double[] eigenValues = new double[ dim ];
		eigen.getEigenvalues( eigenValues, Optional.ofNullable( null ) );

		int numZeroEigenValues = 0;
		for ( final double eigenValue : eigenValues )
			if ( Util.isApproxEqual( eigenValue, 0, THRESHOLD ) )
				++numZeroEigenValues;

		if ( dim == 2 )
		{
			return numZeroEigenValues > 0 ? SpatialLayout.Collinear : SpatialLayout.Coplanar;
		}
		else if ( dim == 3 )
		{
			switch ( numZeroEigenValues )
			{
			case 3:
			case 2:
				return SpatialLayout.Collinear;
			case 1:
				return SpatialLayout.Coplanar;
			default:
				return SpatialLayout.Other;
			}
		}
		else
		{
			throw new RuntimeException( "expected 2d/3d data points, got " + dim + "d" );
		}
	}

	public static SpatialLayout pickMostRestrictedLayout( final SpatialLayout... layouts )
	{
		SpatialLayout ret = null;
		for ( final SpatialLayout layout : layouts )
		{
			if ( ret == SpatialLayout.Collinear || layout == SpatialLayout.Collinear )
				ret = SpatialLayout.Collinear;
			else if ( ret == SpatialLayout.Coplanar || layout == SpatialLayout.Coplanar )
				ret = SpatialLayout.Coplanar;
			else
				ret = layout;
		}
		return ret;
	}
}
