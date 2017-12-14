package org.janelia.stitching;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.analysis.FilterAdjacentShifts;
import org.janelia.util.Conversions;
import org.janelia.util.concurrent.MultithreadedExecutor;
import org.junit.Assert;
import org.junit.Test;
import org.ojalgo.matrix.BasicMatrix.Builder;
import org.ojalgo.matrix.PrimitiveMatrix;

import ij.ImageJ;
import ij.ImagePlus;
import ij.process.FloatProcessor;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.imageplus.FloatImagePlus;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.img.imageplus.IntImagePlus;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.ComparablePair;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class SearchRadiusEstimatorTest
{
	private final static double SEARCH_RADIUS_MULTIPLIER = 3;
	private final static double EPSILON = 1e-3;

	final Random rnd = new Random();

	private SearchRadiusEstimator createSearchRadiusEstimator( final List< double[] > offsets )
	{
		final Map< Integer, double[] > stage = new HashMap<>(), stitched = new HashMap<>();
		for ( int i = 0; i < offsets.size(); ++i )
		{
			final double[] offset = offsets.get( i );
			final double[] stageArr = new double[ offset.length ], stitchedArr = new double[ offset.length ];
			for ( int d = 0; d < offset.length; ++d )
			{
				stageArr[ d ] = ( rnd.nextDouble() - 0.5 ) * 1e5;
				stitchedArr[ d ] = stageArr[ d ] + offset[ d ];
			}
			stage.put( i, stageArr );
			stitched.put( i, stitchedArr );
		}
		final double[] estimationWindowSize = new double[ offsets.get( 0 ).length ];
		Arrays.fill( estimationWindowSize, 1e10 );
		return new SearchRadiusEstimator( stage, stitched, estimationWindowSize, SEARCH_RADIUS_MULTIPLIER );
	}

	private double getVectorLength( final double[] vector )
	{
		double s = 0;
		for ( int d = 0; d < vector.length; ++d )
			s += vector[ d ] * vector[ d ];
		return Math.sqrt( s );
	}

	@Test
	public void testSphereSearchRadius() throws PipelineExecutionException
	{
		final double[] offsetsMeanValues = new double[] { 100, 200, 300 };
		final double[][] offsetsCovarianceMatrix = new double[][] { new double[] { 1, 0, 0 }, new double[] { 0, 1, 0 }, new double[] { 0, 0, 1 } };
		final double sphereRadiusPixels = 50;
		final SearchRadius searchRadius = new SearchRadius( sphereRadiusPixels, offsetsMeanValues, offsetsCovarianceMatrix );

		Assert.assertArrayEquals( new double[] { 100, 200, 300 }, searchRadius.getEllipseCenter(), EPSILON );
		Assert.assertArrayEquals( new double[] { sphereRadiusPixels, sphereRadiusPixels, sphereRadiusPixels }, searchRadius.getEllipseRadius(), EPSILON );

		Assert.assertArrayEquals( new double[] { 1, 1, 1 }, searchRadius.getEigenValues(), EPSILON );

		Assert.assertArrayEquals( new double[] { 1, 0, 0 }, searchRadius.getEigenVectors()[ 0 ], EPSILON );
		Assert.assertArrayEquals( new double[] { 0, 1, 0 }, searchRadius.getEigenVectors()[ 1 ], EPSILON );
		Assert.assertArrayEquals( new double[] { 0, 0, 1 }, searchRadius.getEigenVectors()[ 2 ], EPSILON );

		final Interval boundingBox = Intervals.smallestContainingInterval( searchRadius.getBoundingBox() );
		Assert.assertArrayEquals( new long[] {  50, 150, 250 }, Intervals.minAsLongArray( boundingBox ) );
		Assert.assertArrayEquals( new long[] { 150, 250, 350 }, Intervals.maxAsLongArray( boundingBox ) );

		Assert.assertTrue( searchRadius.testPoint( 100, 200, 300 ) );
		Assert.assertTrue( searchRadius.testPoint( 51, 200, 300 ) );
		Assert.assertTrue( searchRadius.testPoint( 149, 200, 300 ) );
		Assert.assertTrue( searchRadius.testPoint( 100, 151, 300 ) );
		Assert.assertTrue( searchRadius.testPoint( 100, 249, 300 ) );
		Assert.assertTrue( searchRadius.testPoint( 100, 200, 251 ) );
		Assert.assertTrue( searchRadius.testPoint( 100, 200, 349 ) );

		Assert.assertFalse( searchRadius.testPoint( 50, 150, 250 ) );
		Assert.assertFalse( searchRadius.testPoint( 50, 150, 350 ) );
		Assert.assertFalse( searchRadius.testPoint( 50, 250, 250 ) );
		Assert.assertFalse( searchRadius.testPoint( 50, 250, 350 ) );
		Assert.assertFalse( searchRadius.testPoint( 150, 150, 250 ) );
		Assert.assertFalse( searchRadius.testPoint( 150, 150, 350 ) );
		Assert.assertFalse( searchRadius.testPoint( 150, 250, 250 ) );
		Assert.assertFalse( searchRadius.testPoint( 150, 250, 350 ) );
	}


	@Test
	public void testEigen() throws PipelineExecutionException
	{
		final SearchRadius radius = new SearchRadius(
				SEARCH_RADIUS_MULTIPLIER,
				new double[ 3 ],
				new double[][] {
					new double[] { 88.5333, -33.6, -5.33333 },
					new double[] { -33.6, 15.4424, 2.66667 },
					new double[] { -5.33333, 2.66667, 0.484848 } }
			);

		final double[] eigenValues = radius.getEigenValues();

		// sort eigen values by magnitude while keeping track of their indexes
		final List< ComparablePair< Double, Integer > > eigenValuesIndexes = new ArrayList<>();
		for ( int i = 0; i < eigenValues.length; ++i )
			eigenValuesIndexes.add( new ComparablePair<>( eigenValues[ i ], i ) );
		Collections.sort( eigenValuesIndexes );
		final double[] eigenValuesSorted = new double[ eigenValuesIndexes.size() ];
		for ( int i = 0; i < eigenValuesIndexes.size(); ++i )
			eigenValuesSorted[ i ] = eigenValuesIndexes.get( i ).getA();
		Assert.assertArrayEquals( new double[] { 0.00954627, 2.47174, 101.979  }, eigenValuesSorted, EPSILON );

		final double[][] eigenVectors = radius.getEigenVectors();
		Assert.assertEquals( 3, eigenVectors.length );
		final double[][] eigenVectorsSorted = new double[ eigenVectors.length ][ eigenVectors.length ];
		for ( int i = 0; i < eigenVectors.length; ++i )
			eigenVectorsSorted[ i ] = eigenVectors[ eigenValuesIndexes.get( i ).getB() ];

		// if a vector has more than half negative entries, flip all entries in it
		for ( final double[] eigenVector : eigenVectorsSorted )
		{
			int neg = 0;
			for ( int i = 0; i < eigenVector.length; ++i )
				if ( eigenVector[ i ] < 0 )
					++neg;
			if ( neg > eigenVector.length / 2 )
				for ( int i = 0; i < eigenVector.length; ++i )
					eigenVector[ i ] *= -1;
		}

		Assert.assertArrayEquals( new double[] { 0.0298783, 0.233007, -0.972016 }, eigenVectorsSorted[ 0 ], EPSILON );
		Assert.assertArrayEquals( new double[] { 0.366347, 0.902228, 0.227538   }, eigenVectorsSorted[ 1 ], EPSILON );
		Assert.assertArrayEquals( new double[] { -0.929998, 0.362894, 0.0584043 }, eigenVectorsSorted[ 2 ], EPSILON );

		Assert.assertEquals( 1.0, getVectorLength( eigenVectorsSorted[ 0 ] ), EPSILON );
		Assert.assertEquals( 1.0, getVectorLength( eigenVectorsSorted[ 1 ] ), EPSILON );
		Assert.assertEquals( 1.0, getVectorLength( eigenVectorsSorted[ 2 ] ), EPSILON );
	}

//	@Test
	public void testSearchRadius2D() throws PipelineExecutionException, InterruptedException
	{
		final int width = 1000;
		final int height = 1000;

		final double rotationAngle = rnd.nextDouble() * Math.PI - Math.PI / 4;
//		final double rotationAngle = 30. / ( 180. / Math.PI );
		final double cos = Math.cos( rotationAngle );
		final double sin = Math.sin( rotationAngle );

//		rotationAngle = 0.03653782557510701, scaleX = 0.014153596138995916, scaleY = 0.46138822276340974

		final double scaleX = rnd.nextDouble(), scaleY = rnd.nextDouble();
//		final double scaleX = 0.5, scaleY = 0.5;
		System.out.println( "rotationAngle = " + rotationAngle * ( 180. / Math.PI ) + ", scaleX = " + scaleX + ", scaleY = " + scaleY );

		final Builder< PrimitiveMatrix > rotationMatrixBuilder = PrimitiveMatrix.FACTORY.getBuilder( 2, 2 );
		rotationMatrixBuilder.set( 0, 0, cos );
		rotationMatrixBuilder.set( 0, 1, -sin );
		rotationMatrixBuilder.set( 1, 0, sin );
		rotationMatrixBuilder.set( 1, 1, cos );
		final PrimitiveMatrix rotationMatrix = rotationMatrixBuilder.get();

		final double[] translation = new double[] { ( rnd.nextDouble() - 0.5 ) * 3, ( rnd.nextDouble() - 0.5 ) * 3 };

		final int numPoints = 10000;

		final List< double[] > offsets = new ArrayList<>();
		for ( int i = 0; i < numPoints; ++i )
		{
			final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 2, 1 );

			vectorBuilder.set( 0, 0, rnd.nextGaussian() * scaleX );
			vectorBuilder.set( 1, 0, rnd.nextGaussian() * scaleY );

			final PrimitiveMatrix rotatedVector = rotationMatrix.multiply( vectorBuilder.get() );

			offsets.add( new double[] { translation[ 0 ] + rotatedVector.get( 0, 0 ), translation[ 1 ] + rotatedVector.get( 1, 0 ) } );
		}

		final SearchRadiusEstimator estimator = createSearchRadiusEstimator( offsets );
		final SearchRadius radius = estimator.getSearchRadiusWithinEstimationWindow( new double[ 2 ] );

		Assert.assertEquals( numPoints, radius.getUsedPointsIndexes().size() );
		Assert.assertArrayEquals( translation, radius.getOffsetsMeanValues(), 1e-1 );

		// check the angle between the largest eigenvector and the X axis
		final int largestEigenVectorIndex = 0;//radius.getEigenValues()[ 0 ] > radius.getEigenValues()[ 1 ] ? 0 : 1;
		final double[] largestEigenVector = radius.getEigenVectors()[ largestEigenVectorIndex ];
		final double testRotationAngle = Math.atan2( largestEigenVector[ 1 ], largestEigenVector[ 0 ] );

		System.out.println( String.format( "Rotation angle: %.2f,  angle of eigenvector: %.2f", rotationAngle * ( 180. / Math.PI ), testRotationAngle * ( 180. / Math.PI ) ) );
		System.out.println( "Eigen values: " + Arrays.toString( radius.getEigenValues() ) );
		System.out.println( "Eigen vectors: " + Arrays.deepToString( radius.getEigenVectors() ) );

//		Assert.assertEquals( rotationAngle, testRotationAngle, 1e-1 );


		final FloatProcessor ip = new FloatProcessor( width, height );

		final Builder< PrimitiveMatrix > scalingMatrixBuilder = PrimitiveMatrix.FACTORY.getBuilder( 2, 2 );
		scalingMatrixBuilder.set(0, 0, width / 5 );
		scalingMatrixBuilder.set(1, 0, 0);
		scalingMatrixBuilder.set(0, 1, 0);
		scalingMatrixBuilder.set(1, 1, height / 5 );
		final PrimitiveMatrix scalingMatrix = scalingMatrixBuilder.get();
		final PrimitiveMatrix inverseScalingMatrix = scalingMatrix.invert();

		// test ellipse
		for ( int xDisplay = 0; xDisplay < width; ++xDisplay )
		{
			for ( int yDisplay = 0; yDisplay < height; ++yDisplay )
			{
				final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 2, 1 );
				vectorBuilder.set( 0, 0, xDisplay - width  / 2 );
				vectorBuilder.set( 1, 0, yDisplay - height / 2 );
				final PrimitiveMatrix inverseScaledVector = inverseScalingMatrix.multiply( vectorBuilder.get() );

				final double x = inverseScaledVector.get(0, 0), y = inverseScaledVector.get(1, 0);
				if ( radius.testPoint( x + radius.getStagePosition()[ 0 ], y + radius.getStagePosition()[ 1 ] ) )
					ip.setf( xDisplay, yDisplay, 1 );
			}
		}

		// draw the origin
		ip.setf( width / 2, height / 2, 8 );
		ip.setf( width / 2 + 1, height / 2, 7 );
		ip.setf( width / 2 - 1, height / 2, 7 );
		ip.setf( width / 2, height / 2 + 1, 7 );
		ip.setf( width / 2, height / 2 - 1, 7 );

		// draw initial point distribution
		for ( final double[] offset : offsets )
		{
			final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 2, 1 );
			vectorBuilder.set( 0, 0, offset[ 0 ] );
			vectorBuilder.set( 1, 0, offset[ 1 ] );
			final PrimitiveMatrix scaledVector = scalingMatrix.multiply( vectorBuilder.get() );

			final int xDisplay = ( int ) Math.round( scaledVector.get(0, 0) ) + width  / 2;
			final int yDisplay = ( int ) Math.round( scaledVector.get(1, 0) ) + height / 2;

			if ( xDisplay >= 0 && xDisplay < width && yDisplay >= 0 && yDisplay < height )
				ip.setf( xDisplay, yDisplay, 5 );
		}

		new ImageJ();
		final ImagePlus imp = new ImagePlus( "", ip );
		imp.show();
		Thread.sleep( 5 * 1000 );
	}




//	@Test
	public void testSearchRadius3D() throws PipelineExecutionException, InterruptedException, ImgLibException
	{
		final int width = 300;
		final int height = 300;
		final int depth = 300;

		final double rotationAngleX = rnd.nextDouble() * Math.PI - Math.PI / 4;
		final double rotationAngleY = rnd.nextDouble() * Math.PI - Math.PI / 4;
		final double rotationAngleZ = rnd.nextDouble() * Math.PI - Math.PI / 4;
		final double scaleX = rnd.nextDouble(), scaleY = rnd.nextDouble(), scaleZ = rnd.nextDouble();

		final Builder< PrimitiveMatrix > rotationMatrixBuilderX = PrimitiveMatrix.FACTORY.getBuilder( 3, 3 );
		rotationMatrixBuilderX.set( 0, 0, 1 );
		rotationMatrixBuilderX.set( 1, 1, Math.cos( rotationAngleX ) );
		rotationMatrixBuilderX.set( 1, 2, -Math.sin( rotationAngleX ) );
		rotationMatrixBuilderX.set( 2, 1, Math.sin( rotationAngleX ) );
		rotationMatrixBuilderX.set( 2, 2, Math.cos( rotationAngleX ) );
		final PrimitiveMatrix rotationMatrixX = rotationMatrixBuilderX.get();

		final Builder< PrimitiveMatrix > rotationMatrixBuilderY = PrimitiveMatrix.FACTORY.getBuilder( 3, 3 );
		rotationMatrixBuilderY.set( 1, 1, 1 );
		rotationMatrixBuilderY.set( 0, 0, Math.cos( rotationAngleY ) );
		rotationMatrixBuilderY.set( 0, 2, Math.sin( rotationAngleY ) );
		rotationMatrixBuilderY.set( 2, 0, -Math.sin( rotationAngleY ) );
		rotationMatrixBuilderY.set( 2, 2, Math.cos( rotationAngleY ) );
		final PrimitiveMatrix rotationMatrixY = rotationMatrixBuilderY.get();

		final Builder< PrimitiveMatrix > rotationMatrixBuilderZ = PrimitiveMatrix.FACTORY.getBuilder( 3, 3 );
		rotationMatrixBuilderZ.set( 2, 2, 1 );
		rotationMatrixBuilderZ.set( 0, 0, Math.cos( rotationAngleZ ) );
		rotationMatrixBuilderZ.set( 0, 1, -Math.sin( rotationAngleZ ) );
		rotationMatrixBuilderZ.set( 1, 0, Math.sin( rotationAngleZ ) );
		rotationMatrixBuilderZ.set( 1, 1, Math.cos( rotationAngleZ ) );
		final PrimitiveMatrix rotationMatrixZ = rotationMatrixBuilderZ.get();


		final PrimitiveMatrix rotationMatrix = rotationMatrixX.multiply( rotationMatrixY ).multiply( rotationMatrixZ );


		final double[] translation = new double[] { ( rnd.nextDouble() - 0.5 ) * 3, ( rnd.nextDouble() - 0.5 ) * 3, ( rnd.nextDouble() - 0.5 ) * 3 };

		final int numPoints = 10000;

		final List< double[] > offsets = new ArrayList<>();
		for ( int i = 0; i < numPoints; ++i )
		{
			final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 3, 1 );

			vectorBuilder.set( 0, 0, rnd.nextGaussian() * scaleX );
			vectorBuilder.set( 1, 0, rnd.nextGaussian() * scaleY );
			vectorBuilder.set( 2, 0, rnd.nextGaussian() * scaleZ );

			final PrimitiveMatrix rotatedVector = rotationMatrix.multiply( vectorBuilder.get() );

			offsets.add( new double[] { translation[ 0 ] + rotatedVector.get( 0, 0 ), translation[ 1 ] + rotatedVector.get( 1, 0 ), translation[ 2 ] + rotatedVector.get( 2, 0 ) } );
		}

		final SearchRadiusEstimator estimator = createSearchRadiusEstimator( offsets );
		final SearchRadius radius = estimator.getSearchRadiusWithinEstimationWindow( new double[ 3 ] );

		Assert.assertEquals( numPoints, radius.getUsedPointsIndexes().size() );
		Assert.assertArrayEquals( translation, radius.getOffsetsMeanValues(), 1e-1 );

		System.out.println( "Eigen values: " + Arrays.toString( radius.getEigenValues() ) );
		System.out.println( "Eigen vectors: " + Arrays.deepToString( radius.getEigenVectors() ) );

		final FloatImagePlus< FloatType > img = ImagePlusImgs.floats( width, height, depth );
		final RandomAccess< FloatType > imgRandomAccess = img.randomAccess();
		final Cursor< FloatType > imgCursor = img.localizingCursor();

		final Builder< PrimitiveMatrix > scalingMatrixBuilder = PrimitiveMatrix.FACTORY.getBuilder( 3, 3 );
		scalingMatrixBuilder.set(0, 0, width  / 5 );
		scalingMatrixBuilder.set(1, 1, height / 5 );
		scalingMatrixBuilder.set(2, 2, depth  / 5 );
		final PrimitiveMatrix scalingMatrix = scalingMatrixBuilder.get();
		final PrimitiveMatrix inverseScalingMatrix = scalingMatrix.invert();

		// test ellipse
		System.out.println( "Testing display points..." );
		final int[] posDisplay = new int[ 3 ];
		while ( imgCursor.hasNext() )
		{
			imgCursor.fwd();
			imgCursor.localize( posDisplay );

			final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 3, 1 );
			vectorBuilder.set( 0, 0, posDisplay[ 0 ] - width  / 2 );
			vectorBuilder.set( 1, 0, posDisplay[ 1 ] - height / 2 );
			vectorBuilder.set( 2, 0, posDisplay[ 2 ] - depth  / 2 );
			final PrimitiveMatrix inverseScaledVector = inverseScalingMatrix.multiply( vectorBuilder.get() );

			final double x = inverseScaledVector.get(0, 0), y = inverseScaledVector.get(1, 0), z = inverseScaledVector.get(2, 0);
			if ( radius.testPoint( x + radius.getStagePosition()[ 0 ], y + radius.getStagePosition()[ 1 ], z + radius.getStagePosition()[ 2 ] ) )
				imgCursor.get().set( 1 );
		}

		// draw the origin
		imgRandomAccess.setPosition( new int[] { width / 2, height / 2, depth / 2 } );
		imgRandomAccess.get().set( 8 );
		for ( int d = 0; d < 3; ++d )
		{
			for ( final int p : new int[] { -1, 1 } )
			{
				final int[] pos = new int[] { width / 2, height / 2, depth / 2 };
				pos[ d ] += p;
				imgRandomAccess.setPosition( pos );
				imgRandomAccess.get().set( 7 );
			}
		}

		// draw initial point distribution
		System.out.println( "Drawing distribution points..." );
		for ( final double[] offset : offsets )
		{
			final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 3, 1 );
			vectorBuilder.set( 0, 0, offset[ 0 ] );
			vectorBuilder.set( 1, 0, offset[ 1 ] );
			vectorBuilder.set( 2, 0, offset[ 2 ] );
			final PrimitiveMatrix scaledVector = scalingMatrix.multiply( vectorBuilder.get() );

			final int xDisplay = ( int ) Math.round( scaledVector.get(0, 0) ) + width  / 2;
			final int yDisplay = ( int ) Math.round( scaledVector.get(1, 0) ) + height / 2;
			final int zDisplay = ( int ) Math.round( scaledVector.get(2, 0) ) + depth  / 2;

			if ( xDisplay >= 0 && xDisplay < width && yDisplay >= 0 && yDisplay < height && zDisplay >= 0 && zDisplay < depth )
			{
				final int[] pos = new int[] { xDisplay, yDisplay, zDisplay };
				imgRandomAccess.setPosition( pos );
				imgRandomAccess.get().set( 5 );
			}
		}

		System.out.println( "Opening an image..." );
		final ImagePlus imp = img.getImagePlus();
		Utils.workaroundImagePlusNSlices( imp );

		new ImageJ();
		imp.show();
		Thread.sleep( 5 * 60 * 1000 );
	}






//	@Test
	public void testTiles2D() throws PipelineExecutionException, InterruptedException, ImgLibException, IOException, ExecutionException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final int width = 1200;
		final int height = 1200;

		final TileInfo[] stageTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/nrs/saalfeld/igor/170210_SomatoYFP_MBP_Caspr/stitching/flip-x/less-blur,smaller-radius/5z/restitching/ch0_mirroredX_5z.json" ) ) );
		final TileInfo[] stitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/nrs/saalfeld/igor/170210_SomatoYFP_MBP_Caspr/stitching/flip-x/less-blur,smaller-radius/5z/restitching/ch0_mirroredX_5z-final.json" ) ) );
		System.out.println( "Stage tiles = " + stageTiles.length + ", stitched tiles = " + stitchedTiles.length );

		final int dx = 0, dy = 1;

		// make them 2D by dropping 'dz' dimension
		for ( int i = 0; i < stageTiles.length; ++i )
		{
			final TileInfo tile2D = new TileInfo( 2 ), tile3D = stageTiles[ i ];
			tile2D.setIndex( tile3D.getIndex() );
			tile2D.setFilePath( tile3D.getFilePath() );
			tile2D.setPixelResolution( tile3D.getPixelResolution() );
			tile2D.setType( tile3D.getType() );
			tile2D.setPosition( new double[] { tile3D.getPosition( dx ), tile3D.getPosition( dy ) } );
			tile2D.setSize( new long[] { tile3D.getSize( dx ), tile3D.getSize( dy ) } );
			stageTiles[ i ] = tile2D;
		}
		for ( int i = 0; i < stitchedTiles.length; ++i )
		{
			final TileInfo tile2D = new TileInfo( 2 ), tile3D = stitchedTiles[ i ];
			tile2D.setIndex( tile3D.getIndex() );
			tile2D.setFilePath( tile3D.getFilePath() );
			tile2D.setPixelResolution( tile3D.getPixelResolution() );
			tile2D.setType( tile3D.getType() );
			tile2D.setPosition( new double[] { tile3D.getPosition( dx ), tile3D.getPosition( dy ) } );
			tile2D.setSize( new long[] { tile3D.getSize( dx ), tile3D.getSize( dy ) } );
			stitchedTiles[ i ] = tile2D;
		}

		final TileInfo testStageTile = stageTiles[ rnd.nextInt( stageTiles.length ) ];

		final double[] estimationWindowSize = new double[ 2 ];
		Arrays.fill( estimationWindowSize, 1e10 );

		final TileSearchRadiusEstimator estimator = new TileSearchRadiusEstimator( stageTiles, stitchedTiles/*, estimationWindowSize*/, SEARCH_RADIUS_MULTIPLIER );
		final SearchRadius radius = estimator.getSearchRadiusWithinEstimationWindow( testStageTile );

		// test exhaustive search against KD-tree search
		final SearchRadius radiusTree = estimator.getSearchRadiusTreeWithinEstimationWindow( testStageTile );
		final List< Integer > usedPointsExhaustive = new ArrayList<>( radius.getUsedPointsIndexes() );
		final List< Integer > usedPointsTree = new ArrayList<>( radiusTree.getUsedPointsIndexes() );
		Collections.sort( usedPointsExhaustive );
		Collections.sort( usedPointsTree );
		if ( usedPointsExhaustive.size() != new TreeSet<>( usedPointsExhaustive ).size() )
			Assert.fail( "Some points were included more than once" );
		Assert.assertArrayEquals( usedPointsExhaustive.toArray(), usedPointsTree.toArray() );

		System.out.println();
		System.out.println( "Estimated search radius using " + usedPointsTree.size() + " neighboring points" );

//		Assert.assertEquals( stitchedTiles.length, radius.getNumUsedPoints() );
		System.out.println( "Ellipse center = " + Arrays.toString( radius.getEllipseCenter() ) );

		System.out.println( "Eigen values: " + Arrays.toString( radius.getEigenValues() ) );
		System.out.println( "Eigen vectors: " + Arrays.deepToString( radius.getEigenVectors() ) );

		final FloatImagePlus< FloatType > img = ImagePlusImgs.floats( width, height );
		final RandomAccess< FloatType > imgRandomAccess = img.randomAccess();
		final Cursor< FloatType > imgCursor = img.localizingCursor();

		final Builder< PrimitiveMatrix > scalingMatrixBuilder = PrimitiveMatrix.FACTORY.getBuilder( 2, 2 );
		scalingMatrixBuilder.set(0, 0, width  / 400 );
		scalingMatrixBuilder.set(1, 1, height / 400 );
		final PrimitiveMatrix scalingMatrix = scalingMatrixBuilder.get();
		final PrimitiveMatrix inverseScalingMatrix = scalingMatrix.invert();

		// test ellipse
		System.out.println( "Testing display points..." );
		final int[] dim = new int[] { width, height };
		final boolean[] insideEllipse = new boolean[ ( int ) Intervals.numElements( dim ) ];
		try ( final MultithreadedExecutor threadPool = new MultithreadedExecutor() )
		{
			threadPool.run( pix ->
				{
					final int[] pos = new int[ 2 ];
					IntervalIndexer.indexToPosition( pix, dim, pos );

					final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 2, 1 );
					vectorBuilder.set( 0, 0, pos[ 0 ] - dim[ 0 ] / 2 );
					vectorBuilder.set( 1, 0, pos[ 1 ] - dim[ 1 ] / 2 );
					final PrimitiveMatrix inverseScaledVector = inverseScalingMatrix.multiply( vectorBuilder.get() );

					final double x = inverseScaledVector.get(0, 0), y = inverseScaledVector.get(1, 0);
					insideEllipse[ pix ] = radius.testPoint( x + radius.getStagePosition()[ 0 ], y + radius.getStagePosition()[ 1 ] );
				},
				insideEllipse.length );
		}

		final int[] posDisplay = new int[ 2 ];
		while ( imgCursor.hasNext() )
		{
			imgCursor.fwd();
			imgCursor.localize( posDisplay );

			final int pix = IntervalIndexer.positionToIndex( posDisplay, dim );
			if ( insideEllipse[ pix ] )
				imgCursor.get().set( 3 );
		}

		// draw the origin
		imgRandomAccess.setPosition( new int[] { width / 2, height / 2 } );
		imgRandomAccess.get().set( 8 );
		for ( int d = 0; d < 2; ++d )
		{
			for ( final int p : new int[] { -1, 1 } )
			{
				final int[] pos = new int[] { width / 2, height / 2 };
				pos[ d ] += p;
				imgRandomAccess.setPosition( pos );
				imgRandomAccess.get().set( 7 );
			}
		}

		// draw initial point distribution
		System.out.println( "Drawing distribution points..." );
		final Map< Integer, double[] > tileOffsets = estimator.getTileOffsets();
		final Set< Integer > usedPointsIndexesSet = new HashSet<>( radius.getUsedPointsIndexes() );
		for ( final Entry< Integer, double[] > entry : tileOffsets.entrySet() )
		{
			final double[] offset = entry.getValue();

			final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 2, 1 );
			vectorBuilder.set( 0, 0, offset[ 0 ] );
			vectorBuilder.set( 1, 0, offset[ 1 ] );
			final PrimitiveMatrix scaledVector = scalingMatrix.multiply( vectorBuilder.get() );

			final int xDisplay = ( int ) Math.round( scaledVector.get(0, 0) ) + width  / 2;
			final int yDisplay = ( int ) Math.round( scaledVector.get(1, 0) ) + height / 2;

			if ( xDisplay >= 0 && xDisplay < width && yDisplay >= 0 && yDisplay < height )
			{
				final int[] pos = new int[] { xDisplay, yDisplay };
				imgRandomAccess.setPosition( pos );
				imgRandomAccess.get().set( usedPointsIndexesSet.contains( entry.getKey() ) ? 8 : 5 );
			}
		}

		// draw the bounding box corners of the error ellipse
		final double[][] boundingBoxCorners = new double[][] { Intervals.minAsDoubleArray( radius.getBoundingBox() ), Intervals.maxAsDoubleArray( radius.getBoundingBox() ) };
		for ( int i = 0; i < 2; ++i )
			for ( int d = 0; d < 2; ++d )
				boundingBoxCorners[ i ][ d ] -= radius.getStagePosition()[ d ];
		final int[][] boundingBoxCornersDisplay = new int[ 2 ][ 2 ];
		for ( int i = 0; i < 2; ++i )
		{
			final double[] point = boundingBoxCorners[ i ];
			for ( int d = 0; d < point.length; ++d )
				point[ d ] = i == 0 ? Math.floor( point[ d ] ) : Math.ceil( point[ d ] );

			final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 2, 1 );
			vectorBuilder.set( 0, 0, point[ 0 ] );
			vectorBuilder.set( 1, 0, point[ 1 ] );
			final PrimitiveMatrix scaledVector = scalingMatrix.multiply( vectorBuilder.get() );

			final int xDisplay = ( int ) Math.round( scaledVector.get(0, 0) ) + width  / 2;
			final int yDisplay = ( int ) Math.round( scaledVector.get(1, 0) ) + height / 2;

			boundingBoxCornersDisplay[ i ][ 0 ] = xDisplay;
			boundingBoxCornersDisplay[ i ][ 1 ] = yDisplay;
		}
		for ( int dFixed = 0; dFixed < 2; ++dFixed )
		{
			for ( int dMove = 0; dMove < 2; ++dMove )
			{
				if ( dFixed != dMove)
				{
					for ( int i = 0; i < 2; ++i )
					{
						posDisplay[ dFixed ] = boundingBoxCornersDisplay[ i ][ dFixed ];
						for ( int c = boundingBoxCornersDisplay[ 0 ][ dMove ]; c <= boundingBoxCornersDisplay[ 1 ][ dMove ]; ++c )
						{
							posDisplay[ dMove ] = c;
							if ( posDisplay[ 0 ] >= 0 && posDisplay[ 0 ] < width && posDisplay[ 1 ] >= 0 && posDisplay[ 1 ] < height )
							{
								imgRandomAccess.setPosition( posDisplay );
								imgRandomAccess.get().set( 9 );
							}
						}
					}
				}
			}
		}

		System.out.println( "Opening an image..." );
		final ImagePlus imp = img.getImagePlus();
		Utils.workaroundImagePlusNSlices( imp );

		new ImageJ();
		imp.show();
		Thread.sleep( 5 * 1000 );
	}



//	@Test
	public void testTiles3D() throws PipelineExecutionException, InterruptedException, ImgLibException, IOException, ExecutionException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final int width = 600;
		final int height = 600;
		final int depth = 600;

		final TileInfo[] stageTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/nrs/saalfeld/igor/170210_SomatoYFP_MBP_Caspr/stitching/flip-x/less-blur,smaller-radius/5z/restitching/ch0_mirroredX_5z.json" ) ) );
		final TileInfo[] stitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/nrs/saalfeld/igor/170210_SomatoYFP_MBP_Caspr/stitching/flip-x/less-blur,smaller-radius/5z/restitching/ch0_mirroredX_5z-final.json" ) ) );
		System.out.println( "Stage tiles = " + stageTiles.length + ", stitched tiles = " + stitchedTiles.length );

		final double[] estimationWindowSize = new double[ 3 ];
		Arrays.fill( estimationWindowSize, 1e10 );

		final TileInfo testStageTile = stageTiles[ rnd.nextInt( stageTiles.length ) ];

		final TileSearchRadiusEstimator estimator = new TileSearchRadiusEstimator( stageTiles, stitchedTiles/*, estimationWindowSize*/, SEARCH_RADIUS_MULTIPLIER );
		final SearchRadius radius = estimator.getSearchRadiusWithinEstimationWindow( testStageTile );

		// test exhaustive search against KD-tree search
		final SearchRadius radiusTree = estimator.getSearchRadiusTreeWithinEstimationWindow( testStageTile );
		final List< Integer > usedPointsExhaustive = new ArrayList<>( radius.getUsedPointsIndexes() );
		final List< Integer > usedPointsTree = new ArrayList<>( radiusTree.getUsedPointsIndexes() );
		Collections.sort( usedPointsExhaustive );
		Collections.sort( usedPointsTree );
		if ( usedPointsExhaustive.size() != new TreeSet<>( usedPointsExhaustive ).size() )
			Assert.fail( "Some points were included more than once" );
		Assert.assertArrayEquals( usedPointsExhaustive.toArray(), usedPointsTree.toArray() );

		System.out.println();
		System.out.println( "Estimated search radius using " + usedPointsTree.size() + " neighboring points" );

//		Assert.assertEquals( stitchedTiles.length, radius.getNumUsedPoints() );
		System.out.println( "Ellipse center = " + Arrays.toString( radius.getEllipseCenter() ) );

		System.out.println( "Eigen values: " + Arrays.toString( radius.getEigenValues() ) );
		System.out.println( "Eigen vectors: " + Arrays.deepToString( radius.getEigenVectors() ) );

		final FloatImagePlus< FloatType > img = ImagePlusImgs.floats( width, height, depth );
		final RandomAccess< FloatType > imgRandomAccess = img.randomAccess();
		final Cursor< FloatType > imgCursor = img.localizingCursor();

		final Builder< PrimitiveMatrix > scalingMatrixBuilder = PrimitiveMatrix.FACTORY.getBuilder( 3, 3 );
		scalingMatrixBuilder.set(0, 0, width  / 400 );
		scalingMatrixBuilder.set(1, 1, height / 400 );
		scalingMatrixBuilder.set(2, 2, depth  / 400 );
		final PrimitiveMatrix scalingMatrix = scalingMatrixBuilder.get();
		final PrimitiveMatrix inverseScalingMatrix = scalingMatrix.invert();

		// test ellipse
		System.out.println( "Testing display points..." );
		final int[] dim = new int[] { width, height, depth };
		final boolean[] insideEllipse = new boolean[ ( int ) Intervals.numElements( dim ) ];
		try ( final MultithreadedExecutor threadPool = new MultithreadedExecutor() )
		{
			threadPool.run( pix ->
				{
					final int[] pos = new int[ 3 ];
					IntervalIndexer.indexToPosition( pix, dim, pos );

					final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 3, 1 );
					vectorBuilder.set( 0, 0, pos[ 0 ] - dim[ 0 ] / 2 );
					vectorBuilder.set( 1, 0, pos[ 1 ] - dim[ 1 ] / 2 );
					vectorBuilder.set( 2, 0, pos[ 2 ] - dim[ 2 ] / 2 );
					final PrimitiveMatrix inverseScaledVector = inverseScalingMatrix.multiply( vectorBuilder.get() );

					final double x = inverseScaledVector.get(0, 0), y = inverseScaledVector.get(1, 0), z = inverseScaledVector.get(2, 0);
					insideEllipse[ pix ] = radius.testPoint( x + radius.getStagePosition()[ 0 ], y + radius.getStagePosition()[ 1 ], z + radius.getStagePosition()[ 2 ] );
				},
				insideEllipse.length );
		}

		final int[] posDisplay = new int[ 3 ];
		while ( imgCursor.hasNext() )
		{
			imgCursor.fwd();
			imgCursor.localize( posDisplay );

			final int pix = IntervalIndexer.positionToIndex( posDisplay, dim );
			if ( insideEllipse[ pix ] )
				imgCursor.get().set( 1 );
		}

		// draw the origin
		imgRandomAccess.setPosition( new int[] { width / 2, height / 2, depth / 2 } );
		imgRandomAccess.get().set( 8 );
		for ( int d = 0; d < 3; ++d )
		{
			for ( final int p : new int[] { -1, 1 } )
			{
				final int[] pos = new int[] { width / 2, height / 2, depth / 2 };
				pos[ d ] += p;
				imgRandomAccess.setPosition( pos );
				imgRandomAccess.get().set( 7 );
			}
		}

		// draw initial point distribution
		System.out.println( "Drawing distribution points..." );
		final Map< Integer, double[] > tileOffsets = estimator.getTileOffsets();
		for ( final double[] offset : tileOffsets.values() )
		{
			final Builder< PrimitiveMatrix > vectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( 3, 1 );
			vectorBuilder.set( 0, 0, offset[ 0 ] );
			vectorBuilder.set( 1, 0, offset[ 1 ] );
			vectorBuilder.set( 2, 0, offset[ 2 ] );
			final PrimitiveMatrix scaledVector = scalingMatrix.multiply( vectorBuilder.get() );

			final int xDisplay = ( int ) Math.round( scaledVector.get(0, 0) ) + width  / 2;
			final int yDisplay = ( int ) Math.round( scaledVector.get(1, 0) ) + height / 2;
			final int zDisplay = ( int ) Math.round( scaledVector.get(2, 0) ) + depth  / 2;

			if ( xDisplay >= 0 && xDisplay < width && yDisplay >= 0 && yDisplay < height && zDisplay >= 0 && zDisplay < depth )
			{
				final int[] pos = new int[] { xDisplay, yDisplay, zDisplay };
				imgRandomAccess.setPosition( pos );
				imgRandomAccess.get().set( 5 );
			}
		}

		System.out.println( "Opening an image..." );
		final ImagePlus imp = img.getImagePlus();
		Utils.workaroundImagePlusNSlices( imp );

		new ImageJ();
		imp.show();
		Thread.sleep( 5 * 60 * 1000 );
	}





//	@Test
	public void testCombinedEllipseError3D() throws PipelineExecutionException, IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] stageTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/nrs/saalfeld/igor/170210_SomatoYFP_MBP_Caspr/stitching/flip-x/less-blur,smaller-radius/5z/restitching/ch0_mirroredX_5z.json" ) ) );
		final TileInfo[] stitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/nrs/saalfeld/igor/170210_SomatoYFP_MBP_Caspr/stitching/flip-x/less-blur,smaller-radius/5z/restitching/ch0_mirroredX_5z-final.json" ) ) );
		System.out.println( "Stage tiles = " + stageTiles.length + ", stitched tiles = " + stitchedTiles.length );

		final TileSearchRadiusEstimator estimator = new TileSearchRadiusEstimator( stageTiles, stitchedTiles, SEARCH_RADIUS_MULTIPLIER );

		final List< TilePair > overlappingTilePairs = TileOperations.findOverlappingTiles( stageTiles );
		TilePair tilePair = null;
		SearchRadius fixedTileSearchRadius = null, movingTileSearchRadius = null;
		boolean foundNeighborhood = false;
		final int minNeighborhood = 20;
		while ( !foundNeighborhood )
		{
			tilePair = overlappingTilePairs.get( rnd.nextInt( overlappingTilePairs.size() ) );
			fixedTileSearchRadius = estimator.getSearchRadiusTreeWithinEstimationWindow( tilePair.getA() );
			movingTileSearchRadius = estimator.getSearchRadiusTreeWithinEstimationWindow( tilePair.getB() );
			if ( fixedTileSearchRadius.getUsedPointsIndexes().size() >= minNeighborhood && movingTileSearchRadius.getUsedPointsIndexes().size() >= minNeighborhood )
				foundNeighborhood = true;
		}
		System.out.println( "Found overlapping tile pair with neighborhood: fixedTile=" + fixedTileSearchRadius.getUsedPointsIndexes().size() + " points, movingTile=" + movingTileSearchRadius.getUsedPointsIndexes().size() + " points" );

		final SearchRadius combinedErrorEllipse = estimator.getCombinedCovariancesSearchRadius( fixedTileSearchRadius, movingTileSearchRadius );

		final Interval fixedTileSearchRadiusBoundingBox = Intervals.smallestContainingInterval( fixedTileSearchRadius.getBoundingBox() );
		final Interval movingTileSearchRadiusBoundingBox = Intervals.smallestContainingInterval( movingTileSearchRadius.getBoundingBox() );
		final Interval combinedErrorEllipseBoundingBox = Intervals.smallestContainingInterval( combinedErrorEllipse.getBoundingBox() );
		System.out.println( "Fixed tile search radius: min=" + Arrays.toString( Intervals.minAsIntArray( fixedTileSearchRadiusBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( fixedTileSearchRadiusBoundingBox ) ) + ",  size=" + Arrays.toString( Intervals.dimensionsAsIntArray( fixedTileSearchRadiusBoundingBox ) ) );
		System.out.println( "Moving tile search radius: min=" + Arrays.toString( Intervals.minAsIntArray( movingTileSearchRadiusBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( movingTileSearchRadiusBoundingBox ) ) + ",  size=" + Arrays.toString( Intervals.dimensionsAsIntArray( movingTileSearchRadiusBoundingBox ) ) );
		System.out.println( "Combined error ellipse: min=" + Arrays.toString( Intervals.minAsIntArray( combinedErrorEllipseBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( combinedErrorEllipseBoundingBox ) ) + ",  size=" + Arrays.toString( Intervals.dimensionsAsIntArray( combinedErrorEllipseBoundingBox ) ) );

		final int iterations = 10000;
		for ( int iter = 0; iter < iterations; ++iter )
		{
			// 1. Sample a point within unit sphere for the fixed tile
			final double[] fixedTileUnitSpherePoint = new double[ fixedTileSearchRadius.numDimensions() ];
			boolean fixedTilePointWithinUnitSphere = false;
			while ( !fixedTilePointWithinUnitSphere )
			{
				double sumSq = 0;
				for ( int d = 0; d < fixedTileUnitSpherePoint.length; ++d )
				{
					fixedTileUnitSpherePoint[ d ] = ( rnd.nextDouble() - 0.5 ) * 2;
					sumSq += Math.pow( fixedTileUnitSpherePoint[ d ], 2 );
				}
				fixedTilePointWithinUnitSphere = ( sumSq <= 1 );
			}

			// 2. Transform it to the global coordinate system with respect to the fixed tile search radius
			final Builder< PrimitiveMatrix > fixedTileUnitSpherePointVectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( fixedTileUnitSpherePoint.length + 1, 1 );
			for ( int d = 0; d < fixedTileUnitSpherePoint.length; ++d )
				fixedTileUnitSpherePointVectorBuilder.set( d, 0, fixedTileUnitSpherePoint[ d ] );
			fixedTileUnitSpherePointVectorBuilder.set( fixedTileUnitSpherePoint.length, 0, 1 );
			final PrimitiveMatrix fixedTilePointVector = fixedTileSearchRadius.getTransformMatrix().multiply( fixedTileUnitSpherePointVectorBuilder.get() );
			final double[] fixedTilePoint = new double[ fixedTileUnitSpherePoint.length ];
			for ( int d = 0; d < fixedTilePoint.length; ++d )
				fixedTilePoint[ d ] = fixedTilePointVector.get( d, 0 );
			Assert.assertTrue( fixedTileSearchRadius.testPoint( fixedTilePoint ) );

			// 3. Sample a point within unit cube for the moving tile
			final double[] movingTileUnitCubePoint = new double[ movingTileSearchRadius.numDimensions() ];
			for ( int d = 0; d < movingTileUnitCubePoint.length; ++d )
				movingTileUnitCubePoint[ d ] = ( rnd.nextDouble() - 0.5 ) * 2;

			// 4. Transform it to the global coordinate system with respect to the moving tile search radius
			final Builder< PrimitiveMatrix > movingTileUnitCubePointVectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( movingTileUnitCubePoint.length + 1, 1 );
			for ( int d = 0; d < movingTileUnitCubePoint.length; ++d )
				movingTileUnitCubePointVectorBuilder.set( d, 0, movingTileUnitCubePoint[ d ] );
			movingTileUnitCubePointVectorBuilder.set( movingTileUnitCubePoint.length, 0, 1 );
			final PrimitiveMatrix movingTilePointVector = movingTileSearchRadius.getTransformMatrix().multiply( movingTileUnitCubePointVectorBuilder.get() );
			final double[] movingTilePoint = new double[ movingTileUnitCubePoint.length ];
			for ( int d = 0; d < movingTilePoint.length; ++d )
				movingTilePoint[ d ] = movingTilePointVector.get( d, 0 );

			// 5. Compute a vector between fixedTilePoint and movingTilePoint
			final double[] movingTileRelativeOffset = new double[ movingTilePoint.length ];
			for ( int d = 0; d < movingTileRelativeOffset.length; ++d )
				movingTileRelativeOffset[ d ] = movingTilePoint[ d ] - fixedTilePoint[ d ];

			// 6. Find new moving tile point in the coordinate space of the fixed tile (with respect to the combined search radius)
			final double[] movingTileRelativePoint = new double[ movingTilePoint.length ];
			for ( int d = 0; d < movingTileRelativePoint.length; ++d )
				movingTileRelativePoint[ d ] = tilePair.getA().getPosition( d ) + movingTileRelativeOffset[ d ];

			// 7. Find whether the moving tile point falls within moving tile search radius
			double sumSq = 0;
			for ( int d = 0; d < movingTileUnitCubePoint.length; ++d )
				sumSq += Math.pow( movingTileUnitCubePoint[ d ], 2 );
			final boolean movingTilePointWithinUnitSphere = ( sumSq <= 1 );
			Assert.assertEquals( movingTilePointWithinUnitSphere, movingTileSearchRadius.testPoint( movingTilePoint ) );

			// 8. Test the new relative moving tile point against the combined error ellipse and compare the result with the expected outcome
			try
			{
				Assert.assertEquals( movingTilePointWithinUnitSphere, combinedErrorEllipse.testPoint( movingTileRelativePoint ) );
			}
			catch ( final AssertionError e )
			{
				System.out.println();
				System.out.println( "Ouch! Testing relative moving tile point " + Arrays.toString( movingTileRelativePoint ) );
				throw e;
			}
		}
	}




//	@Test
	public void testCombinedEllipseError2D() throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] stageTiles3d = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/nrs/saalfeld/igor/sample1_c2/automated-stitching/restitching-incremental/ch0.json" ) ) );
		//final TileInfo[] stitchedTiles3d = TileInfoJSONProvider.loadTilesConfiguration( "/nrs/saalfeld/igor/170210_SomatoYFP_MBP_Caspr/stitching/flip-x/less-blur,smaller-radius/5z/restitching/ch0_mirroredX_5z-final.json" );
		final TileInfo[] stitchedTiles3d = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/nrs/saalfeld/igor/sample1_c2/automated-stitching/restitching-incremental/iter12/ch0-stitched.json" ) ) );

		// list all Z grid coordinates to randomly choose XY plane
		final Map< Integer, int[] > stageTilesCoordinatesMap = Utils.getTilesCoordinatesMap( stageTiles3d );
		final Map< Integer, List< TileInfo > > zCoordToStitchedTiles = new HashMap<>();
		for ( final TileInfo stitchedTile : stitchedTiles3d )
		{
			if ( stageTilesCoordinatesMap.containsKey( stitchedTile.getIndex() ) )
			{
				final int zCoord = stageTilesCoordinatesMap.get( stitchedTile.getIndex() )[ 2 ];
				if ( !zCoordToStitchedTiles.containsKey( zCoord ) )
					zCoordToStitchedTiles.put( zCoord, new ArrayList<>() );
				zCoordToStitchedTiles.get( zCoord ).add( stitchedTile );
			}
		}
		System.out.println( "List of z coords: " + zCoordToStitchedTiles.keySet() );

		final int zCoordChosen = zCoordToStitchedTiles.keySet().toArray( new Integer[ 0 ] )[ rnd.nextInt( zCoordToStitchedTiles.size() ) ];

		final List< TileInfo > zCoordStageTilesList = new ArrayList<>();
		for ( final TileInfo stageTile : stageTiles3d )
			if ( stageTilesCoordinatesMap.get( stageTile.getIndex() )[ 2 ] == zCoordChosen )
				zCoordStageTilesList.add( stageTile );

		final TileInfo[] stageTiles = zCoordStageTilesList.toArray( new TileInfo[ 0 ] );
		final TileInfo[] stitchedTiles = zCoordToStitchedTiles.get( zCoordChosen ).toArray( new TileInfo[ 0 ] );

		for ( int i = 0; i < stageTiles.length; ++i )
		{
			final TileInfo stageTile = stageTiles[ i ];
			final TileInfo tile2d = new TileInfo( 2 );
			tile2d.setIndex( stageTile.getIndex() );
			tile2d.setFilePath( stageTile.getFilePath() );
			tile2d.setPixelResolution( stageTile.getPixelResolution() );
			tile2d.setType( stageTile.getType() );
			tile2d.setPosition( new double[] { stageTile.getPosition( 0 ), stageTile.getPosition( 1 ) } );
			tile2d.setSize( new long[] { stageTile.getSize( 0 ), stageTile.getSize( 1 ) } );
			stageTiles[ i ] = tile2d;
		}
		for ( int i = 0; i < stitchedTiles.length; ++i )
		{
			final TileInfo stitchedTile = stitchedTiles[ i ];
			final TileInfo tile2d = new TileInfo( 2 );
			tile2d.setIndex( stitchedTile.getIndex() );
			tile2d.setFilePath( stitchedTile.getFilePath() );
			tile2d.setPixelResolution( stitchedTile.getPixelResolution() );
			tile2d.setType( stitchedTile.getType() );
			tile2d.setPosition( new double[] { stitchedTile.getPosition( 0 ), stitchedTile.getPosition( 1 ) } );
			tile2d.setSize( new long[] { stitchedTile.getSize( 0 ), stitchedTile.getSize( 1 ) } );
			stitchedTiles[ i ] = tile2d;
		}

		System.out.println( "Chosen Z index = " + zCoordChosen );
		System.out.println( "2d Stage tiles = " + stageTiles.length + ", 2d stitched tiles = " + stitchedTiles.length );

		final TileSearchRadiusEstimator estimator = new TileSearchRadiusEstimator( stageTiles, stitchedTiles, SEARCH_RADIUS_MULTIPLIER );
		System.out.println( "-- Created search radius estimator. Estimation window size (neighborhood): " + Arrays.toString( Intervals.dimensionsAsIntArray( estimator.getEstimationWindowSize() ) ) + " --" );

		final List< TilePair > overlappingTilePairs = FilterAdjacentShifts.filterAdjacentPairs( TileOperations.findOverlappingTiles( stageTiles ) );
		TilePair tilePair = null;
		SearchRadius fixedTileSearchRadius = null, movingTileSearchRadius = null;
		TileInfo fixedTile = null, movingTile = null;
		boolean foundNeighborhood = false;
		final int minNeighborhood = 5;
		while ( !foundNeighborhood )
		{
			tilePair = overlappingTilePairs.get( rnd.nextInt( overlappingTilePairs.size() ) );

			for ( int i = 0; i < overlappingTilePairs.size(); ++i )
				if ( overlappingTilePairs.get( i ).getA().getIndex().intValue() == 2291 && overlappingTilePairs.get( i ).getB().getIndex().intValue() == 2292 )
					tilePair = overlappingTilePairs.get( i );

			fixedTile = tilePair.getA();
			movingTile = tilePair.getB();

//			// FIXME: force to choose an X+ pair
//			if ( movingTile.getPosition( 0 ) - 5 < fixedTile.getPosition( 0 ) )
//				continue;

			fixedTileSearchRadius = estimator.getSearchRadiusTreeWithinEstimationWindow( fixedTile );
			movingTileSearchRadius = estimator.getSearchRadiusTreeWithinEstimationWindow( movingTile );
//			fixedTileSearchRadius = estimator.getSearchRadiusTreeUsingKNearestNeighbors( fixedTile, stitchedTiles.length );
//			movingTileSearchRadius = estimator.getSearchRadiusTreeUsingKNearestNeighbors( movingTile, stitchedTiles.length );
//			fixedTileSearchRadius = new SearchRadius( new double[] { 30, -15 }, new double[][] { new double[] { 15, 20 }, new double[] { 14, 24 } }, movingTileSearchRadius.getUsedPointsIndexes(), estimator.getStagePosition( fixedTile ) ); //estimator.getSearchRadiusTreeUsingKNearestNeighbors( fixedTile, 400 );
			if ( fixedTileSearchRadius.getUsedPointsIndexes().size() >= minNeighborhood && movingTileSearchRadius.getUsedPointsIndexes().size() >= minNeighborhood )
				foundNeighborhood = true;
		}
		System.out.println( "Found overlapping tile pair " + tilePair + " with neighborhood: fixedTile=" + fixedTileSearchRadius.getUsedPointsIndexes().size() + " points, movingTile=" + movingTileSearchRadius.getUsedPointsIndexes().size() + " points" );

		final SearchRadius combinedSearchRadius = estimator.getCombinedCovariancesSearchRadius( fixedTileSearchRadius, movingTileSearchRadius );

		final Interval fixedTileSearchRadiusBoundingBox = Intervals.smallestContainingInterval( fixedTileSearchRadius.getBoundingBox() );
		final Interval movingTileSearchRadiusBoundingBox = Intervals.smallestContainingInterval( movingTileSearchRadius.getBoundingBox() );
		final Interval combinedSearchRadiusBoundingBox = Intervals.smallestContainingInterval( combinedSearchRadius.getBoundingBox() );
		System.out.println( "Fixed tile search radius: min=" + Arrays.toString( Intervals.minAsIntArray( fixedTileSearchRadiusBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( fixedTileSearchRadiusBoundingBox ) ) + ",  size=" + Arrays.toString( Intervals.dimensionsAsIntArray( fixedTileSearchRadiusBoundingBox ) ) );
		System.out.println( "Moving tile search radius: min=" + Arrays.toString( Intervals.minAsIntArray( movingTileSearchRadiusBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( movingTileSearchRadiusBoundingBox ) ) + ",  size=" + Arrays.toString( Intervals.dimensionsAsIntArray( movingTileSearchRadiusBoundingBox ) ) );
		System.out.println( "Combined error ellipse: min=" + Arrays.toString( Intervals.minAsIntArray( combinedSearchRadiusBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( combinedSearchRadiusBoundingBox ) ) + ",  size=" + Arrays.toString( Intervals.dimensionsAsIntArray( combinedSearchRadiusBoundingBox ) ) );

		final int iterations = 10000;
		for ( int iter = 0; iter < iterations; ++iter )
		{
			// 1. Sample a point within unit sphere for the fixed tile
			final double[] fixedTileUnitSpherePoint = new double[ fixedTileSearchRadius.numDimensions() ];
			boolean fixedTilePointWithinUnitSphere = false;
			while ( !fixedTilePointWithinUnitSphere )
			{
				double sumSq = 0;
				for ( int d = 0; d < fixedTileUnitSpherePoint.length; ++d )
				{
					fixedTileUnitSpherePoint[ d ] = ( rnd.nextDouble() - 0.5 ) * 2;
					sumSq += Math.pow( fixedTileUnitSpherePoint[ d ], 2 );
				}
				fixedTilePointWithinUnitSphere = ( sumSq <= 1 );
			}

			// 2. Transform it to the global coordinate system with respect to the fixed tile search radius
			final Builder< PrimitiveMatrix > fixedTileUnitSpherePointVectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( fixedTileUnitSpherePoint.length + 1, 1 );
			for ( int d = 0; d < fixedTileUnitSpherePoint.length; ++d )
				fixedTileUnitSpherePointVectorBuilder.set( d, 0, fixedTileUnitSpherePoint[ d ] );
			fixedTileUnitSpherePointVectorBuilder.set( fixedTileUnitSpherePoint.length, 0, 1 );
			final PrimitiveMatrix fixedTilePointVector = fixedTileSearchRadius.getTransformMatrix().multiply( fixedTileUnitSpherePointVectorBuilder.get() );
			final double[] fixedTilePoint = new double[ fixedTileUnitSpherePoint.length ];
			for ( int d = 0; d < fixedTilePoint.length; ++d )
				fixedTilePoint[ d ] = fixedTilePointVector.get( d, 0 );
//			Assert.assertTrue( fixedTileSearchRadius.testPoint( fixedTilePoint ) );

			// 3. Sample a point within unit cube for the moving tile
			final double[] movingTileUnitCubePoint = new double[ movingTileSearchRadius.numDimensions() ];
			for ( int d = 0; d < movingTileUnitCubePoint.length; ++d )
				movingTileUnitCubePoint[ d ] = ( rnd.nextDouble() - 0.5 ) * 2;

			// 4. Transform it to the global coordinate system with respect to the moving tile search radius
			final Builder< PrimitiveMatrix > movingTileUnitCubePointVectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( movingTileUnitCubePoint.length + 1, 1 );
			for ( int d = 0; d < movingTileUnitCubePoint.length; ++d )
				movingTileUnitCubePointVectorBuilder.set( d, 0, movingTileUnitCubePoint[ d ] );
			movingTileUnitCubePointVectorBuilder.set( movingTileUnitCubePoint.length, 0, 1 );
			final PrimitiveMatrix movingTilePointVector = movingTileSearchRadius.getTransformMatrix().multiply( movingTileUnitCubePointVectorBuilder.get() );
			final double[] movingTilePoint = new double[ movingTileUnitCubePoint.length ];
			for ( int d = 0; d < movingTilePoint.length; ++d )
				movingTilePoint[ d ] = movingTilePointVector.get( d, 0 );

			// 5. Compute a vector between fixedTilePoint and movingTilePoint
			final double[] movingTileRelativeOffset = new double[ movingTilePoint.length ];
			for ( int d = 0; d < movingTileRelativeOffset.length; ++d )
				movingTileRelativeOffset[ d ] = movingTilePoint[ d ] - fixedTilePoint[ d ];

			// 6. Test all possible pixels within fixed tile search radius, and if it is a valid offset, create another pair of translated points
			double[] fixedTilePointPossibleTranslation = null, movingTilePointPossibleTranslation = null;
			for ( int xFixedTileSearchRadius = ( int ) fixedTileSearchRadiusBoundingBox.min( 0 ); xFixedTileSearchRadius <= ( int ) fixedTileSearchRadiusBoundingBox.max( 0 ); ++xFixedTileSearchRadius )
			{
				for ( int yFixedTileSearchRadius = ( int ) fixedTileSearchRadiusBoundingBox.min( 1 ); yFixedTileSearchRadius <= ( int ) fixedTileSearchRadiusBoundingBox.max( 1 ); ++yFixedTileSearchRadius )
				{
					final double[] testFixedTileSearchRadiusPoint = new double[] { xFixedTileSearchRadius, yFixedTileSearchRadius };
					final double[] testMovingTileSearchRadiusPoint = new double[] { xFixedTileSearchRadius + movingTileRelativeOffset[ 0 ], yFixedTileSearchRadius + movingTileRelativeOffset[ 1 ] };
					if ( fixedTileSearchRadius.testPoint( testFixedTileSearchRadiusPoint ) && movingTileSearchRadius.testPoint( testMovingTileSearchRadiusPoint ) )
					{
						fixedTilePointPossibleTranslation = testFixedTileSearchRadiusPoint;
						movingTilePointPossibleTranslation = testMovingTileSearchRadiusPoint;
						break;
					}
				}
				if ( fixedTilePointPossibleTranslation != null && movingTilePointPossibleTranslation != null )
					break;
			}
			if ( fixedTilePointPossibleTranslation == null )
				fixedTilePointPossibleTranslation = fixedTilePoint;
			if ( movingTilePointPossibleTranslation == null )
				movingTilePointPossibleTranslation = movingTilePoint;

			// 7. Find new moving tile point in the coordinate space of the fixed tile (with respect to the combined search radius)
			final double[] movingTileRelativePoint = new double[ movingTilePointPossibleTranslation.length ];
			for ( int d = 0; d < movingTileRelativePoint.length; ++d )
				movingTileRelativePoint[ d ] = fixedTile.getPosition( d ) + movingTileRelativeOffset[ d ];

			// 8. Test the new relative moving tile point against the combined error ellipse and compare the result with the expected outcome
			final boolean isPossibleOffset = ( fixedTileSearchRadius.testPoint( fixedTilePointPossibleTranslation ) && movingTileSearchRadius.testPoint( movingTilePointPossibleTranslation ) );
			try
			{
				Assert.fail( "FIXME" );
//				Assert.assertEquals( isPossibleOffset, combinedSearchRadius.testPoint( movingTileRelativePoint ) );
			}
			catch ( final AssertionError e )
			{
				System.out.println();
				System.out.println( "Assertion testing relative moving tile point " + Arrays.toString( movingTileRelativePoint ) + ":  expected(movingTileSearchRadius)=" + isPossibleOffset + ", actual(combinedSearchRadius)=" + combinedSearchRadius.testPoint( movingTileRelativePoint ) );


				// Create a .dat file with sorted tile offsets
				/*try ( final PrintWriter writer = new PrintWriter( "offsets.dat" ) )
				{
					final double[][] sortedOffsets = new double[ 2 ][ stitchedTiles.length ];
					for ( int d = 0; d < 2; ++d )
					{
						final List< Double > vals = new ArrayList<>();
						for ( final double[] offset : estimator.getTileOffsets().values() )
							vals.add( offset[ d ] );
						Collections.sort( vals );
						for ( int i = 0; i < vals.size(); ++i )
							sortedOffsets[ d ][ i ] = vals.get( i );
					}
					for ( int i = 0; i < stitchedTiles.length; ++i )
						writer.println( sortedOffsets[ 0 ][ i ] + " " + sortedOffsets[ 1 ][ i ] );
				}*/


				final int[] displaySize = new int[] { 1500, 1500 };
				final int[] displayOffset = Intervals.minAsIntArray( fixedTile.getBoundaries() );
				displayOffset[ 0 ] -= displaySize[ 0 ] / 2;
				displayOffset[ 1 ] -= displaySize[ 1 ] / 2;
				final IntImagePlus< ARGBType > img = ImagePlusImgs.argbs( Conversions.toLongArray( displaySize ) );
				final RandomAccess< ARGBType > imgRandomAccess = img.randomAccess();
				final int[] displayPosition = new int[ 2 ];

				// Draw fixed tile contour
				{
					final int fixedTileContourColor = ARGBType.rgba( 0x22, 0x22, 0, 0xff );
					final int[][] fixedTileContourCorners = new int[][] { Intervals.minAsIntArray( fixedTile.getBoundaries() ), Intervals.maxAsIntArray( fixedTile.getBoundaries() ) };
					for ( int dFixed = 0; dFixed < 2; ++dFixed )
					{
						for ( int dMove = 0; dMove < 2; ++dMove )
						{
							if ( dFixed != dMove)
							{
								for ( int i = 0; i < 2; ++i )
								{
									displayPosition[ dFixed ] = fixedTileContourCorners[ i ][ dFixed ] - displayOffset[ dFixed ];
									for ( int c = fixedTileContourCorners[ 0 ][ dMove ]; c <= fixedTileContourCorners[ 1 ][ dMove ]; ++c )
									{
										displayPosition[ dMove ] = c - displayOffset[ dMove ];
										if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
										{
											imgRandomAccess.setPosition( displayPosition );
											imgRandomAccess.get().set( fixedTileContourColor );
										}
									}
								}
							}
						}
					}
				}

				// Draw moving tile contour
				{
					final int movingTileContourColor = ARGBType.rgba( 0, 0x22, 0x22, 0xff );
					final int[][] movingTileContourCorners = new int[][] { Intervals.minAsIntArray( movingTile.getBoundaries() ), Intervals.maxAsIntArray( movingTile.getBoundaries() ) };
					for ( int dFixed = 0; dFixed < 2; ++dFixed )
					{
						for ( int dMove = 0; dMove < 2; ++dMove )
						{
							if ( dFixed != dMove)
							{
								for ( int i = 0; i < 2; ++i )
								{
									displayPosition[ dFixed ] = movingTileContourCorners[ i ][ dFixed ] - displayOffset[ dFixed ];
									for ( int c = movingTileContourCorners[ 0 ][ dMove ]; c <= movingTileContourCorners[ 1 ][ dMove ]; ++c )
									{
										displayPosition[ dMove ] = c - displayOffset[ dMove ];
										if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
										{
											imgRandomAccess.setPosition( displayPosition );
											imgRandomAccess.get().set( movingTileContourColor );
										}
									}
								}
							}
						}
					}
				}

				// Draw fixed tile left-upper corner point
				{
					final int fixedTileColor = ARGBType.rgba( 0xff, 0xff, 0, 0xff );
					for ( int d = 0; d < 2; ++d )
						displayPosition[ d ] = ( int ) fixedTile.getBoundaries().min( d ) - displayOffset[ d ];
					if ( displayPosition[ 0 ] > 0 && displayPosition[ 0 ] < displaySize[ 0 ] - 1 && displayPosition[ 1 ] > 0 && displayPosition[ 1 ] < displaySize[ 1 ] - 1 )
					{
						imgRandomAccess.setPosition( displayPosition );
						imgRandomAccess.get().set( fixedTileColor );
						imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] } );
						imgRandomAccess.get().set( fixedTileColor );
						imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] } );
						imgRandomAccess.get().set( fixedTileColor );
						imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] - 1 } );
						imgRandomAccess.get().set( fixedTileColor );
						imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] + 1 } );
						imgRandomAccess.get().set( fixedTileColor );
					}
				}

				// Draw fixed tile sample point
				/*{
					final int fixedTileSamplePointColor = ARGBType.rgba( 0xff, 0xff, 0xff, 0xff );
					for ( int d = 0; d < 2; ++d )
						displayPosition[ d ] = ( int ) fixedTilePoint[ d ] - displayOffset[ d ];
					imgRandomAccess.setPosition( displayPosition );
					imgRandomAccess.get().set( fixedTileSamplePointColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] - 1 } );
					imgRandomAccess.get().set( fixedTileSamplePointColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] - 1 } );
					imgRandomAccess.get().set( fixedTileSamplePointColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] + 1 } );
					imgRandomAccess.get().set( fixedTileSamplePointColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] + 1 } );
					imgRandomAccess.get().set( fixedTileSamplePointColor );
				}*/

				// Draw fixed tile possible translation point
				/*if ( fixedTilePointPossibleTranslation != fixedTilePoint )
				{
					final int fixedTileSamplePointPossibleTranslationColor = ARGBType.rgba( 0xaa, 0xaa, 0xaa, 0xff );
					for ( int d = 0; d < 2; ++d )
						displayPosition[ d ] = ( int ) fixedTilePointPossibleTranslation[ d ] - displayOffset[ d ];
					imgRandomAccess.setPosition( displayPosition );
					imgRandomAccess.get().set( fixedTileSamplePointPossibleTranslationColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] - 1 } );
					imgRandomAccess.get().set( fixedTileSamplePointPossibleTranslationColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] - 1 } );
					imgRandomAccess.get().set( fixedTileSamplePointPossibleTranslationColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] + 1 } );
					imgRandomAccess.get().set( fixedTileSamplePointPossibleTranslationColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] + 1 } );
					imgRandomAccess.get().set( fixedTileSamplePointPossibleTranslationColor );
				}*/

				// Draw moving tile left-upper corner point
				{
					final int movingTileColor = ARGBType.rgba( 0, 0xff, 0xff, 0xff );
					for ( int d = 0; d < 2; ++d )
						displayPosition[ d ] = ( int ) movingTile.getBoundaries().min( d ) - displayOffset[ d ];
					if ( displayPosition[ 0 ] > 0 && displayPosition[ 0 ] < displaySize[ 0 ] - 1 && displayPosition[ 1 ] > 0 && displayPosition[ 1 ] < displaySize[ 1 ] - 1 )
					{
						imgRandomAccess.setPosition( displayPosition );
						imgRandomAccess.get().set( movingTileColor );
						imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] } );
						imgRandomAccess.get().set( movingTileColor );
						imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] } );
						imgRandomAccess.get().set( movingTileColor );
						imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] - 1 } );
						imgRandomAccess.get().set( movingTileColor );
						imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] + 1 } );
						imgRandomAccess.get().set( movingTileColor );
					}
				}

				// Draw moving tile sample point
				/*{
					final int movingTileSamplePointColor = ARGBType.rgba( 0xff, 0xff, 0xff, 0xff );
					for ( int d = 0; d < 2; ++d )
						displayPosition[ d ] = ( int ) movingTilePoint[ d ] - displayOffset[ d ];
					imgRandomAccess.setPosition( displayPosition );
					imgRandomAccess.get().set( movingTileSamplePointColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] - 1 } );
					imgRandomAccess.get().set( movingTileSamplePointColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] - 1 } );
					imgRandomAccess.get().set( movingTileSamplePointColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] + 1 } );
					imgRandomAccess.get().set( movingTileSamplePointColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] + 1 } );
					imgRandomAccess.get().set( movingTileSamplePointColor );
				}*/

				// Draw moving tile possible translation point
				/*if ( movingTilePointPossibleTranslation != movingTilePoint )
				{
					final int movingTileSamplePointPossibleTranslationColor = ARGBType.rgba( 0xaa, 0xaa, 0xaa, 0xff );
					for ( int d = 0; d < 2; ++d )
						displayPosition[ d ] = ( int ) movingTilePointPossibleTranslation[ d ] - displayOffset[ d ];
					imgRandomAccess.setPosition( displayPosition );
					imgRandomAccess.get().set( movingTileSamplePointPossibleTranslationColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] - 1 } );
					imgRandomAccess.get().set( movingTileSamplePointPossibleTranslationColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] - 1 } );
					imgRandomAccess.get().set( movingTileSamplePointPossibleTranslationColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] + 1 } );
					imgRandomAccess.get().set( movingTileSamplePointPossibleTranslationColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] + 1 } );
					imgRandomAccess.get().set( movingTileSamplePointPossibleTranslationColor );
				}*/

				// Draw moving tile with offset (test point)
				/*{
					final int movingTileWithOffsetColor = ARGBType.rgba( 0xff, 0, 0, 0xff );
					for ( int d = 0; d < 2; ++d )
						displayPosition[ d ] = ( int ) movingTileRelativePoint[ d ] - displayOffset[ d ];
					imgRandomAccess.setPosition( displayPosition );
					imgRandomAccess.get().set( movingTileWithOffsetColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ] } );
					imgRandomAccess.get().set( movingTileWithOffsetColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ] } );
					imgRandomAccess.get().set( movingTileWithOffsetColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] - 1 } );
					imgRandomAccess.get().set( movingTileWithOffsetColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] + 1 } );
					imgRandomAccess.get().set( movingTileWithOffsetColor );
				}*/

				// Draw fixed tile ellipse contour
				{
					final int fixedTileEllipseCountourColor = ARGBType.rgba( 0xaa, 0xaa, 0, 0xff );
					final int[] fixedTileSearchRadiusBoundingBoxOffset = Intervals.minAsIntArray( fixedTileSearchRadiusBoundingBox );
					final boolean[][] fixedTileSearchRadiusTest = new boolean[ ( int ) fixedTileSearchRadiusBoundingBox.dimension( 0 ) ][ ( int ) fixedTileSearchRadiusBoundingBox.dimension( 1 ) ];
					for ( int xTest = ( int ) fixedTileSearchRadiusBoundingBox.min( 0 ); xTest <= ( int ) fixedTileSearchRadiusBoundingBox.max( 0 ); ++xTest )
						for ( int yTest = ( int ) fixedTileSearchRadiusBoundingBox.min( 1 ); yTest <= ( int ) fixedTileSearchRadiusBoundingBox.max( 1 ); ++yTest )
							if ( fixedTileSearchRadius.testPoint( xTest, yTest ) )
								fixedTileSearchRadiusTest[ xTest - fixedTileSearchRadiusBoundingBoxOffset[ 0 ] ][ yTest - fixedTileSearchRadiusBoundingBoxOffset[ 1 ] ] = true;
					for ( int i = 0; i < fixedTileSearchRadiusTest.length; ++i )
					{
						for ( int j = 0; j < fixedTileSearchRadiusTest[ i ].length; ++j )
						{
							if ( fixedTileSearchRadiusTest[ i ][ j ] &&
									!(
											( i - 1 >= 0 && fixedTileSearchRadiusTest[ i - 1 ][ j ] ) &&
											( i + 1 < fixedTileSearchRadiusTest.length && fixedTileSearchRadiusTest[ i + 1 ][ j ] ) &&
											( j - 1 >= 0 && fixedTileSearchRadiusTest[ i ][ j - 1 ] ) &&
											( j + 1 < fixedTileSearchRadiusTest[ i ].length && fixedTileSearchRadiusTest[ i ][ j + 1 ] )
									)
								)
							{
								final int[] indexes = new int[] { i, j };
								for ( int d = 0; d < 2; ++d )
									displayPosition[ d ] = fixedTileSearchRadiusBoundingBoxOffset[ d ] + indexes[ d ] - displayOffset[ d ];
								if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
								{
									imgRandomAccess.setPosition( displayPosition );
									imgRandomAccess.get().set( fixedTileEllipseCountourColor );
								}
							}
						}
					}
				}

				// Draw moving tile ellipse contour
				{
					final int movingTileEllipseCountourColor = ARGBType.rgba( 0, 0xaa, 0xaa, 0xff );
					final int[] movingTileSearchRadiusBoundingBoxOffset = Intervals.minAsIntArray( movingTileSearchRadiusBoundingBox );
					final boolean[][] movingTileSearchRadiusTest = new boolean[ ( int ) movingTileSearchRadiusBoundingBox.dimension( 0 ) ][ ( int ) movingTileSearchRadiusBoundingBox.dimension( 1 ) ];
					for ( int xTest = ( int ) movingTileSearchRadiusBoundingBox.min( 0 ); xTest <= ( int ) movingTileSearchRadiusBoundingBox.max( 0 ); ++xTest )
						for ( int yTest = ( int ) movingTileSearchRadiusBoundingBox.min( 1 ); yTest <= ( int ) movingTileSearchRadiusBoundingBox.max( 1 ); ++yTest )
							if ( movingTileSearchRadius.testPoint( xTest, yTest ) )
								movingTileSearchRadiusTest[ xTest - movingTileSearchRadiusBoundingBoxOffset[ 0 ] ][ yTest - movingTileSearchRadiusBoundingBoxOffset[ 1 ] ] = true;
					for ( int i = 0; i < movingTileSearchRadiusTest.length; ++i )
					{
						for ( int j = 0; j < movingTileSearchRadiusTest[ i ].length; ++j )
						{
							if ( movingTileSearchRadiusTest[ i ][ j ] &&
									!(
											( i - 1 >= 0 && movingTileSearchRadiusTest[ i - 1 ][ j ] ) &&
											( i + 1 < movingTileSearchRadiusTest.length && movingTileSearchRadiusTest[ i + 1 ][ j ] ) &&
											( j - 1 >= 0 && movingTileSearchRadiusTest[ i ][ j - 1 ] ) &&
											( j + 1 < movingTileSearchRadiusTest[ i ].length && movingTileSearchRadiusTest[ i ][ j + 1 ] )
									)
								)
							{
								final int[] indexes = new int[] { i, j };
								for ( int d = 0; d < 2; ++d )
									displayPosition[ d ] = movingTileSearchRadiusBoundingBoxOffset[ d ] + indexes[ d ] - displayOffset[ d ];
								if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
								{
									imgRandomAccess.setPosition( displayPosition );
									imgRandomAccess.get().set( movingTileEllipseCountourColor );
								}
							}
						}
					}
				}

				// Draw combined ellipse contour
				{
					final int combinedEllipseCountourColor = ARGBType.rgba( 0xaa, 0, 0, 0xff );
					final int[] combinedSearchRadiusBoundingBoxOffset = Intervals.minAsIntArray( combinedSearchRadiusBoundingBox );
					final boolean[][] combinedSearchRadiusTest = new boolean[ ( int ) combinedSearchRadiusBoundingBox.dimension( 0 ) ][ ( int ) combinedSearchRadiusBoundingBox.dimension( 1 ) ];
					for ( int xTest = ( int ) combinedSearchRadiusBoundingBox.min( 0 ); xTest <= ( int ) combinedSearchRadiusBoundingBox.max( 0 ); ++xTest )
						for ( int yTest = ( int ) combinedSearchRadiusBoundingBox.min( 1 ); yTest <= ( int ) combinedSearchRadiusBoundingBox.max( 1 ); ++yTest )
							if ( combinedSearchRadius.testPoint( xTest, yTest ) )
								combinedSearchRadiusTest[ xTest - combinedSearchRadiusBoundingBoxOffset[ 0 ] ][ yTest - combinedSearchRadiusBoundingBoxOffset[ 1 ] ] = true;
					for ( int i = 0; i < combinedSearchRadiusTest.length; ++i )
					{
						for ( int j = 0; j < combinedSearchRadiusTest[ i ].length; ++j )
						{
							if ( combinedSearchRadiusTest[ i ][ j ] &&
									!(
											( i - 1 >= 0 && combinedSearchRadiusTest[ i - 1 ][ j ] ) &&
											( i + 1 < combinedSearchRadiusTest.length && combinedSearchRadiusTest[ i + 1 ][ j ] ) &&
											( j - 1 >= 0 && combinedSearchRadiusTest[ i ][ j - 1 ] ) &&
											( j + 1 < combinedSearchRadiusTest[ i ].length && combinedSearchRadiusTest[ i ][ j + 1 ] )
									)
								)
							{
								final int[] indexes = new int[] { i, j };
								for ( int d = 0; d < 2; ++d )
									displayPosition[ d ] = combinedSearchRadiusBoundingBoxOffset[ d ] + indexes[ d ] - displayOffset[ d ];
								if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
								{
									imgRandomAccess.setPosition( displayPosition );
									imgRandomAccess.get().set( combinedEllipseCountourColor );
								}
							}
						}
					}
				}

				// Find adjusted overlapping region for both tiles
				final Pair< Interval, Interval > adjustedOverlaps = adjustOverlapRegion( tilePair, new ValuePair<>( fixedTileSearchRadius, movingTileSearchRadius ), combinedSearchRadius );

				// Draw adjusted overlap contour for the fixed tile
				{
					final int fixedTileContourColor = ARGBType.rgba( 0x88, 0x88, 0, 0xff );
					final Interval adjustedOverlapInterval = Intervals.translate( Intervals.translate( adjustedOverlaps.getA(), fixedTile.getBoundaries().min( 0 ), 0 ), fixedTile.getBoundaries().min( 1 ), 1 );
					final int[][] fixedTileAdjustedOverlapCorners = new int[][] { Intervals.minAsIntArray( adjustedOverlapInterval ), Intervals.maxAsIntArray( adjustedOverlapInterval ) };
					for ( int dFixed = 0; dFixed < 2; ++dFixed )
					{
						for ( int dMove = 0; dMove < 2; ++dMove )
						{
							if ( dFixed != dMove)
							{
								for ( int i = 0; i < 2; ++i )
								{
									displayPosition[ dFixed ] = fixedTileAdjustedOverlapCorners[ i ][ dFixed ] - displayOffset[ dFixed ];
									for ( int c = fixedTileAdjustedOverlapCorners[ 0 ][ dMove ]; c <= fixedTileAdjustedOverlapCorners[ 1 ][ dMove ]; ++c )
									{
										displayPosition[ dMove ] = c - displayOffset[ dMove ];
										if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
										{
											imgRandomAccess.setPosition( displayPosition );
											imgRandomAccess.get().set( fixedTileContourColor );
										}
									}
								}
							}
						}
					}
				}
				// Draw adjusted overlap contour for the moving tile
				{
					final int movingTileContourColor = ARGBType.rgba( 0, 0x88, 0x88, 0xff );
					final Interval adjustedOverlapInterval = Intervals.translate( Intervals.translate( adjustedOverlaps.getB(), movingTile.getBoundaries().min( 0 ), 0 ), movingTile.getBoundaries().min( 1 ), 1 );
					final int[][] movingTileAdjustedOverlapCorners = new int[][] { Intervals.minAsIntArray( adjustedOverlapInterval ), Intervals.maxAsIntArray( adjustedOverlapInterval ) };
					for ( int dFixed = 0; dFixed < 2; ++dFixed )
					{
						for ( int dMove = 0; dMove < 2; ++dMove )
						{
							if ( dFixed != dMove)
							{
								for ( int i = 0; i < 2; ++i )
								{
									displayPosition[ dFixed ] = movingTileAdjustedOverlapCorners[ i ][ dFixed ] - displayOffset[ dFixed ];
									for ( int c = movingTileAdjustedOverlapCorners[ 0 ][ dMove ]; c <= movingTileAdjustedOverlapCorners[ 1 ][ dMove ]; ++c )
									{
										displayPosition[ dMove ] = c - displayOffset[ dMove ];
										if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
										{
											imgRandomAccess.setPosition( displayPosition );
											imgRandomAccess.get().set( movingTileContourColor );
										}
									}
								}
							}
						}
					}
				}

				// Draw combined ellipse bounding box
				/*{
					final int combinedEllipseBoundingBoxColor = ARGBType.rgba( 0x55, 0, 0, 0xff );
					final int[][] combinedEllipseBoundingBoxCorners = new int[][] { Intervals.minAsIntArray( combinedSearchRadiusBoundingBox ), Intervals.maxAsIntArray( combinedSearchRadiusBoundingBox ) };
					for ( int dFixed = 0; dFixed < 2; ++dFixed )
					{
						for ( int dMove = 0; dMove < 2; ++dMove )
						{
							if ( dFixed != dMove)
							{
								for ( int i = 0; i < 2; ++i )
								{
									displayPosition[ dFixed ] = combinedEllipseBoundingBoxCorners[ i ][ dFixed ] - displayOffset[ dFixed ];
									for ( int c = combinedEllipseBoundingBoxCorners[ 0 ][ dMove ]; c <= combinedEllipseBoundingBoxCorners[ 1 ][ dMove ]; ++c )
									{
										displayPosition[ dMove ] = c - displayOffset[ dMove ];
										if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
										{
											imgRandomAccess.setPosition( displayPosition );
											imgRandomAccess.get().set( combinedEllipseBoundingBoxColor );
										}
									}
								}
							}
						}
					}
				}*/


				// ---------------------------------------------------
				// Draw combined search radius contour obtained using covariance matrices to compare it against combined error ellipse
				/*{
					final SearchRadius combinedCovariances = estimator.getCombinedCovariancesSearchRadius( fixedTileSearchRadius, movingTileSearchRadius );
					final Interval combinedCovariancesBoundingBox = Intervals.smallestContainingInterval( combinedCovariances.getBoundingBox() );
					final int combinedCovariancesCountourColor = ARGBType.rgba( 0xff, 0xcc, 0, 0xff );
					final int[] combinedCovariancesBoundingBoxOffset = Intervals.minAsIntArray( combinedCovariancesBoundingBox );
					final boolean[][] combinedCovariancesTest = new boolean[ ( int ) combinedCovariancesBoundingBox.dimension( 0 ) ][ ( int ) combinedCovariancesBoundingBox.dimension( 1 ) ];
					for ( int xTest = ( int ) combinedCovariancesBoundingBox.min( 0 ); xTest <= ( int ) combinedCovariancesBoundingBox.max( 0 ); ++xTest )
						for ( int yTest = ( int ) combinedCovariancesBoundingBox.min( 1 ); yTest <= ( int ) combinedCovariancesBoundingBox.max( 1 ); ++yTest )
							if ( combinedCovariances.testPoint( xTest, yTest ) )
								combinedCovariancesTest[ xTest - combinedCovariancesBoundingBoxOffset[ 0 ] ][ yTest - combinedCovariancesBoundingBoxOffset[ 1 ] ] = true;
					for ( int i = 0; i < combinedCovariancesTest.length; ++i )
					{
						for ( int j = 0; j < combinedCovariancesTest[ i ].length; ++j )
						{
							if ( combinedCovariancesTest[ i ][ j ] &&
									!(
											( i - 1 >= 0 && combinedCovariancesTest[ i - 1 ][ j ] ) &&
											( i + 1 < combinedCovariancesTest.length && combinedCovariancesTest[ i + 1 ][ j ] ) &&
											( j - 1 >= 0 && combinedCovariancesTest[ i ][ j - 1 ] ) &&
											( j + 1 < combinedCovariancesTest[ i ].length && combinedCovariancesTest[ i ][ j + 1 ] )
									)
								)
							{
								final int[] indexes = new int[] { i, j };
								for ( int d = 0; d < 2; ++d )
									displayPosition[ d ] = combinedCovariancesBoundingBoxOffset[ d ] + indexes[ d ] - displayOffset[ d ];
								if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
								{
									imgRandomAccess.setPosition( displayPosition );
									imgRandomAccess.get().set( combinedCovariancesCountourColor );
								}
							}
						}
					}
				}*/
				// ---------------------------------------------------


				/*
				// ********************************************
				// Draw confidence interval for the expected offset using previous method (statistics using the entire set).
				{
					System.out.println();
					System.out.println( "Fixed tile: " + Utils.getTileCoordinatesString( fixedTile ) );
					System.out.println( "Moving tile: " + Utils.getTileCoordinatesString( movingTile ) );

					final ConfidenceIntervalStats confidenceIntervalStats = ConfidenceIntervalStats.fromJson( "/nrs/saalfeld/igor/170210_SomatoYFP_MBP_Caspr/stitching/flip-x/less-blur,smaller-radius/5z/restitching/confidence-interval.json" );
					final TreeMap< Integer, Integer > diffDimensions = new TreeMap<>();
					for ( int d = 0; d < 2; ++d )
						if ( Utils.getTileCoordinates( fixedTile )[ d ] != Utils.getTileCoordinates( movingTile )[ d ] )
							diffDimensions.put( d, Utils.getTileCoordinates( movingTile )[ d ] - Utils.getTileCoordinates( fixedTile )[ d ] );
					if ( diffDimensions.size() != 1 || Math.abs( diffDimensions.firstEntry().getValue() ) != 1 )
						Assert.fail( "diffDimensions: " + diffDimensions );

					final Interval confidenceInterval3D = confidenceIntervalStats.getConfidenceInterval( diffDimensions.firstKey(), diffDimensions.firstEntry().getValue().intValue() == 1 );
					final Interval confidenceInterval = new FinalInterval( new long[] { confidenceInterval3D.min( 0 ), confidenceInterval3D.min( 1 ) }, new long[] { confidenceInterval3D.max( 0 ), confidenceInterval3D.max( 1 ) } );
					final long[] confidenceIntervalShiftedMin = new long[ confidenceInterval.numDimensions() ], confidenceIntervalShiftedMax = new long[ confidenceInterval.numDimensions() ];
					for ( int d = 0; d < confidenceInterval.numDimensions(); ++d )
					{
						confidenceIntervalShiftedMin[ d ] = confidenceInterval.min( d ) + movingTile.getBoundaries().min( d );
						confidenceIntervalShiftedMax[ d ] = confidenceInterval.max( d ) + movingTile.getBoundaries().min( d );
					}
					final Interval confidenceIntervalShifted = new FinalInterval( confidenceIntervalShiftedMin, confidenceIntervalShiftedMax );

					System.out.println( "Confidence interval by previous method: size=" + Arrays.toString( Intervals.dimensionsAsIntArray( confidenceIntervalShifted ) ) );

					final int confidenceIntervalShiftedColor = ARGBType.rgba( 0, 0xcc, 0, 0xff );
					final int[][] confidenceIntervalShiftedCorners = new int[][] { Intervals.minAsIntArray( confidenceIntervalShifted ), Intervals.maxAsIntArray( confidenceIntervalShifted ) };
					for ( int dFixed = 0; dFixed < 2; ++dFixed )
					{
						for ( int dMove = 0; dMove < 2; ++dMove )
						{
							if ( dFixed != dMove)
							{
								for ( int i = 0; i < 2; ++i )
								{
									displayPosition[ dFixed ] = confidenceIntervalShiftedCorners[ i ][ dFixed ] - displayOffset[ dFixed ];
									for ( int c = confidenceIntervalShiftedCorners[ 0 ][ dMove ]; c <= confidenceIntervalShiftedCorners[ 1 ][ dMove ]; ++c )
									{
										displayPosition[ dMove ] = c - displayOffset[ dMove ];
										if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
										{
											imgRandomAccess.setPosition( displayPosition );
											imgRandomAccess.get().set( confidenceIntervalShiftedColor );
										}
									}
								}
							}
						}
					}
				}
				System.out.println();
				// ********************************************
				*/


				// Display
				System.out.println( "Opening an image..." );
				final ImagePlus imp = img.getImagePlus();
				Utils.workaroundImagePlusNSlices( imp );

				new ImageJ();
				imp.show();
				Thread.sleep( 10 * 60 * 1000 );


				throw e;
			}
		}
	}



//	@Test
	public void testCombinedEllipse3D() throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[] stageTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/groups/betzig/betziglab/4Stephan/160727_Sample2_C3/Stitch_Igor/restitching/restitching-covariance/ch0.json" ) ) );
		final TileInfo[] stitchedTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( "/groups/betzig/betziglab/4Stephan/160727_Sample2_C3/Stitch_Igor/restitching/ch0-final.json" ) ) );

		final TileSearchRadiusEstimator estimator = new TileSearchRadiusEstimator( stageTiles, stitchedTiles, SEARCH_RADIUS_MULTIPLIER );
		System.out.println( "-- Created search radius estimator. Estimation window size (neighborhood): " + Arrays.toString( Intervals.dimensionsAsIntArray( estimator.getEstimationWindowSize() ) ) + " --" );

		final List< TilePair > overlappingTilePairs = FilterAdjacentShifts.filterAdjacentPairs( TileOperations.findOverlappingTiles( stageTiles ) );
		TilePair tilePair = null;
		SearchRadius fixedTileSearchRadius = null, movingTileSearchRadius = null;
		TileInfo fixedTile = null, movingTile = null;
		boolean foundNeighborhood = false;
		final int minNeighborhood = 5;
		while ( !foundNeighborhood )
		{
			tilePair = overlappingTilePairs.get( rnd.nextInt( overlappingTilePairs.size() ) );
			for ( int i = 0; i < overlappingTilePairs.size(); ++i )
				if ( overlappingTilePairs.get( i ).getA().getIndex().intValue() == 135 && overlappingTilePairs.get( i ).getB().getIndex().intValue() == 176 )
					tilePair = overlappingTilePairs.get( i );

			fixedTile = tilePair.getA();
			movingTile = tilePair.getB();
			fixedTileSearchRadius = estimator.getSearchRadiusTreeWithinEstimationWindow( fixedTile );
			movingTileSearchRadius = estimator.getSearchRadiusTreeWithinEstimationWindow( movingTile );
//			fixedTileSearchRadius = estimator.getSearchRadiusTreeUsingKNearestNeighbors( fixedTile, stitchedTiles.length );
//			movingTileSearchRadius = estimator.getSearchRadiusTreeUsingKNearestNeighbors( movingTile, stitchedTiles.length );
//			fixedTileSearchRadius = new SearchRadius( new double[] { 30, -15 }, new double[][] { new double[] { 15, 20 }, new double[] { 14, 24 } }, movingTileSearchRadius.getUsedPointsIndexes(), estimator.getStagePosition( fixedTile ) ); //estimator.getSearchRadiusTreeUsingKNearestNeighbors( fixedTile, 400 );
			if ( fixedTileSearchRadius.getUsedPointsIndexes().size() >= minNeighborhood && movingTileSearchRadius.getUsedPointsIndexes().size() >= minNeighborhood )
				foundNeighborhood = true;
		}
		System.out.println();
		System.out.println( "Found overlapping tile pair " + tilePair + " with neighborhood: fixedTile=" + fixedTileSearchRadius.getUsedPointsIndexes().size() + " points, movingTile=" + movingTileSearchRadius.getUsedPointsIndexes().size() + " points" );
		System.out.println( "Tile pair: " + Utils.getTileCoordinatesString( fixedTile ) + " and " + Utils.getTileCoordinatesString( movingTile ) );
		System.out.println();

		final SearchRadius combinedSearchRadius = estimator.getCombinedCovariancesSearchRadius( fixedTileSearchRadius, movingTileSearchRadius );

		final Interval fixedTileSearchRadiusBoundingBox = Intervals.smallestContainingInterval( fixedTileSearchRadius.getBoundingBox() );
		final Interval movingTileSearchRadiusBoundingBox = Intervals.smallestContainingInterval( movingTileSearchRadius.getBoundingBox() );
		final Interval combinedSearchRadiusBoundingBox = Intervals.smallestContainingInterval( combinedSearchRadius.getBoundingBox() );
		System.out.println( "Fixed tile search radius: min=" + Arrays.toString( Intervals.minAsIntArray( fixedTileSearchRadiusBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( fixedTileSearchRadiusBoundingBox ) ) + ",  size=" + Arrays.toString( Intervals.dimensionsAsIntArray( fixedTileSearchRadiusBoundingBox ) ) );
		System.out.println( "Moving tile search radius: min=" + Arrays.toString( Intervals.minAsIntArray( movingTileSearchRadiusBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( movingTileSearchRadiusBoundingBox ) ) + ",  size=" + Arrays.toString( Intervals.dimensionsAsIntArray( movingTileSearchRadiusBoundingBox ) ) );
		System.out.println( "Combined error ellipse: min=" + Arrays.toString( Intervals.minAsIntArray( combinedSearchRadiusBoundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsIntArray( combinedSearchRadiusBoundingBox ) ) + ",  size=" + Arrays.toString( Intervals.dimensionsAsIntArray( combinedSearchRadiusBoundingBox ) ) );

		final int iterations = 10000;
		for ( int iter = 0; iter < iterations; ++iter )
		{
			// 1. Sample a point within unit sphere for the fixed tile
			final double[] fixedTileUnitSpherePoint = new double[ fixedTileSearchRadius.numDimensions() ];
			boolean fixedTilePointWithinUnitSphere = false;
			while ( !fixedTilePointWithinUnitSphere )
			{
				double sumSq = 0;
				for ( int d = 0; d < fixedTileUnitSpherePoint.length; ++d )
				{
					fixedTileUnitSpherePoint[ d ] = ( rnd.nextDouble() - 0.5 ) * 2;
					sumSq += Math.pow( fixedTileUnitSpherePoint[ d ], 2 );
				}
				fixedTilePointWithinUnitSphere = ( sumSq <= 1 );
			}

			// 2. Transform it to the global coordinate system with respect to the fixed tile search radius
			final Builder< PrimitiveMatrix > fixedTileUnitSpherePointVectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( fixedTileUnitSpherePoint.length + 1, 1 );
			for ( int d = 0; d < fixedTileUnitSpherePoint.length; ++d )
				fixedTileUnitSpherePointVectorBuilder.set( d, 0, fixedTileUnitSpherePoint[ d ] );
			fixedTileUnitSpherePointVectorBuilder.set( fixedTileUnitSpherePoint.length, 0, 1 );
			final PrimitiveMatrix fixedTilePointVector = fixedTileSearchRadius.getTransformMatrix().multiply( fixedTileUnitSpherePointVectorBuilder.get() );
			final double[] fixedTilePoint = new double[ fixedTileUnitSpherePoint.length ];
			for ( int d = 0; d < fixedTilePoint.length; ++d )
				fixedTilePoint[ d ] = fixedTilePointVector.get( d, 0 );
//			Assert.assertTrue( fixedTileSearchRadius.testPoint( fixedTilePoint ) );

			// 3. Sample a point within unit cube for the moving tile
			final double[] movingTileUnitCubePoint = new double[ movingTileSearchRadius.numDimensions() ];
			for ( int d = 0; d < movingTileUnitCubePoint.length; ++d )
				movingTileUnitCubePoint[ d ] = ( rnd.nextDouble() - 0.5 ) * 2;

			// 4. Transform it to the global coordinate system with respect to the moving tile search radius
			final Builder< PrimitiveMatrix > movingTileUnitCubePointVectorBuilder = PrimitiveMatrix.FACTORY.getBuilder( movingTileUnitCubePoint.length + 1, 1 );
			for ( int d = 0; d < movingTileUnitCubePoint.length; ++d )
				movingTileUnitCubePointVectorBuilder.set( d, 0, movingTileUnitCubePoint[ d ] );
			movingTileUnitCubePointVectorBuilder.set( movingTileUnitCubePoint.length, 0, 1 );
			final PrimitiveMatrix movingTilePointVector = movingTileSearchRadius.getTransformMatrix().multiply( movingTileUnitCubePointVectorBuilder.get() );
			final double[] movingTilePoint = new double[ movingTileUnitCubePoint.length ];
			for ( int d = 0; d < movingTilePoint.length; ++d )
				movingTilePoint[ d ] = movingTilePointVector.get( d, 0 );

			// 5. Compute a vector between fixedTilePoint and movingTilePoint
			final double[] movingTileRelativeOffset = new double[ movingTilePoint.length ];
			for ( int d = 0; d < movingTileRelativeOffset.length; ++d )
				movingTileRelativeOffset[ d ] = movingTilePoint[ d ] - fixedTilePoint[ d ];

			try
			{
				Assert.fail( "FIXME" );
//				Assert.assertEquals( isPossibleOffset, combinedSearchRadius.testPoint( movingTileRelativePoint ) );
			}
			catch ( final AssertionError e )
			{
				System.out.println();
				System.out.println( "*** Drawing test case ***" );

				// Create a .dat file with sorted tile offsets
				/*try ( final PrintWriter writer = new PrintWriter( "offsets.dat" ) )
				{
					final double[][] sortedOffsets = new double[ 2 ][ stitchedTiles.length ];
					for ( int d = 0; d < 2; ++d )
					{
						final List< Double > vals = new ArrayList<>();
						for ( final double[] offset : estimator.getTileOffsets().values() )
							vals.add( offset[ d ] );
						Collections.sort( vals );
						for ( int i = 0; i < vals.size(); ++i )
							sortedOffsets[ d ][ i ] = vals.get( i );
					}
					for ( int i = 0; i < stitchedTiles.length; ++i )
						writer.println( sortedOffsets[ 0 ][ i ] + " " + sortedOffsets[ 1 ][ i ] );
				}*/


				final int[] displaySize = new int[] { 800, 1300, 800 };
				final int[] displayOffset = Intervals.minAsIntArray( fixedTile.getBoundaries() );
				displayOffset[ 0 ] -= displaySize[ 0 ] / 2;
				displayOffset[ 1 ] -= displaySize[ 1 ] / 2;
				displayOffset[ 2 ] -= displaySize[ 2 ] / 6;
				final IntImagePlus< ARGBType > img = ImagePlusImgs.argbs( Conversions.toLongArray( displaySize ) );
				final RandomAccess< ARGBType > imgRandomAccess = img.randomAccess();
				final int[] displayPosition = new int[ displaySize.length ];

				System.out.println( "Drawing fixed tile..." );
				// Draw fixed tile contour
				{
					final int fixedTileContourColor = ARGBType.rgba( 0x22, 0x22, 0, 0xff );
					final int[][] fixedTileContourCorners = new int[][] { Intervals.minAsIntArray( fixedTile.getBoundaries() ), Intervals.maxAsIntArray( fixedTile.getBoundaries() ) };
					for ( int dFixed1 = 0; dFixed1 < fixedTileContourCorners[ 0 ].length; ++dFixed1 )
					{
						for ( int dFixed2 = dFixed1 + 1; dFixed2 < fixedTileContourCorners[ 0 ].length; ++dFixed2 )
						{
							for ( int dMove = 0; dMove < fixedTileContourCorners[ 0 ].length; ++dMove )
							{
								if ( dMove != dFixed1 && dMove != dFixed2 )
								{
									for ( int i = 0; i < 2; ++i )
									{
										displayPosition[ dFixed1 ] = fixedTileContourCorners[ i ][ dFixed1 ] - displayOffset[ dFixed1 ];
										displayPosition[ dFixed2 ] = fixedTileContourCorners[ i ][ dFixed2 ] - displayOffset[ dFixed2 ];
										for ( int c = fixedTileContourCorners[ 0 ][ dMove ]; c <= fixedTileContourCorners[ 1 ][ dMove ]; ++c )
										{
											displayPosition[ dMove ] = c - displayOffset[ dMove ];
											if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] && displayPosition[ 2 ] >= 0 && displayPosition[ 2 ] < displaySize[ 2 ] )
											{
												imgRandomAccess.setPosition( displayPosition );
												imgRandomAccess.get().set( fixedTileContourColor );
											}
										}
									}
								}
							}
						}
					}
				}

				System.out.println( "Drawing moving tile..." );
				// Draw moving tile contour
				{
					final int movingTileContourColor = ARGBType.rgba( 0, 0x22, 0x22, 0xff );
					final int[][] movingTileContourCorners = new int[][] { Intervals.minAsIntArray( movingTile.getBoundaries() ), Intervals.maxAsIntArray( movingTile.getBoundaries() ) };
					for ( int dFixed1 = 0; dFixed1 < movingTileContourCorners[ 0 ].length; ++dFixed1 )
					{
						for ( int dFixed2 = dFixed1 + 1; dFixed2 < movingTileContourCorners[ 0 ].length; ++dFixed2 )
						{
							for ( int dMove = 0; dMove < movingTileContourCorners[ 0 ].length; ++dMove )
							{
								if ( dMove != dFixed1 && dMove != dFixed2 )
								{
									for ( int i = 0; i < 2; ++i )
									{
										displayPosition[ dFixed1 ] = movingTileContourCorners[ i ][ dFixed1 ] - displayOffset[ dFixed1 ];
										displayPosition[ dFixed2 ] = movingTileContourCorners[ i ][ dFixed2 ] - displayOffset[ dFixed2 ];
										for ( int c = movingTileContourCorners[ 0 ][ dMove ]; c <= movingTileContourCorners[ 1 ][ dMove ]; ++c )
										{
											displayPosition[ dMove ] = c - displayOffset[ dMove ];
											if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] && displayPosition[ 2 ] >= 0 && displayPosition[ 2 ] < displaySize[ 2 ] )
											{
												imgRandomAccess.setPosition( displayPosition );
												imgRandomAccess.get().set( movingTileContourColor );
											}
										}
									}
								}
							}
						}
					}
				}

				// Draw fixed tile left-upper corner point
				{
					final int fixedTileColor = ARGBType.rgba( 0xff, 0xff, 0, 0xff );
					for ( int d = 0; d < fixedTile.numDimensions(); ++d )
						displayPosition[ d ] = ( int ) fixedTile.getBoundaries().min( d ) - displayOffset[ d ];
					imgRandomAccess.setPosition( displayPosition );
					imgRandomAccess.get().set( fixedTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ], displayPosition[ 2 ] } );
					imgRandomAccess.get().set( fixedTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ], displayPosition[ 2 ] } );
					imgRandomAccess.get().set( fixedTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] - 1, displayPosition[ 2 ] } );
					imgRandomAccess.get().set( fixedTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] + 1, displayPosition[ 2 ] } );
					imgRandomAccess.get().set( fixedTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ], displayPosition[ 2 ] - 1 } );
					imgRandomAccess.get().set( fixedTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ], displayPosition[ 2 ] + 1 } );
					imgRandomAccess.get().set( fixedTileColor );
				}

				// Draw moving tile left-upper corner point
				{
					final int movingTileColor = ARGBType.rgba( 0, 0xff, 0xff, 0xff );
					for ( int d = 0; d < movingTile.numDimensions(); ++d )
						displayPosition[ d ] = ( int ) movingTile.getBoundaries().min( d ) - displayOffset[ d ];
					imgRandomAccess.setPosition( displayPosition );
					imgRandomAccess.get().set( movingTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] - 1, displayPosition[ 1 ], displayPosition[ 2 ] } );
					imgRandomAccess.get().set( movingTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ] + 1, displayPosition[ 1 ], displayPosition[ 2 ] } );
					imgRandomAccess.get().set( movingTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] - 1, displayPosition[ 2 ] } );
					imgRandomAccess.get().set( movingTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ] + 1, displayPosition[ 2 ] } );
					imgRandomAccess.get().set( movingTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ], displayPosition[ 2 ] - 1 } );
					imgRandomAccess.get().set( movingTileColor );
					imgRandomAccess.setPosition( new int[] { displayPosition[ 0 ], displayPosition[ 1 ], displayPosition[ 2 ] + 1 } );
					imgRandomAccess.get().set( movingTileColor );
				}

				System.out.println( "Drawing fixed tile ellipse..." );
				// Draw fixed tile ellipse
				{
					final int fixedTileEllipseCountourColor = ARGBType.rgba( 0xaa, 0xaa, 0, 0xff );
					final int[] fixedTileSearchRadiusBoundingBoxOffset = Intervals.minAsIntArray( fixedTileSearchRadiusBoundingBox );
					final boolean[][][] fixedTileSearchRadiusTest = new boolean[ ( int ) fixedTileSearchRadiusBoundingBox.dimension( 0 ) ][ ( int ) fixedTileSearchRadiusBoundingBox.dimension( 1 ) ][ ( int ) fixedTileSearchRadiusBoundingBox.dimension( 2 ) ];

					final MultithreadedExecutor threadPool = new MultithreadedExecutor();

//					for ( int xTest = ( int ) fixedTileSearchRadiusBoundingBox.min( 0 ); xTest <= ( int ) fixedTileSearchRadiusBoundingBox.max( 0 ); ++xTest )
//						for ( int yTest = ( int ) fixedTileSearchRadiusBoundingBox.min( 1 ); yTest <= ( int ) fixedTileSearchRadiusBoundingBox.max( 1 ); ++yTest )
//							for ( int zTest = ( int ) fixedTileSearchRadiusBoundingBox.min( 2 ); zTest <= ( int ) fixedTileSearchRadiusBoundingBox.max( 2 ); ++zTest )
//								if ( fixedTileSearchRadius.testPoint( xTest, yTest, zTest ) )
//									fixedTileSearchRadiusTest[ xTest - fixedTileSearchRadiusBoundingBoxOffset[ 0 ] ][ yTest - fixedTileSearchRadiusBoundingBoxOffset[ 1 ] ][ zTest - fixedTileSearchRadiusBoundingBoxOffset[ 2 ] ] = true;
					final int[] dimensions = Intervals.dimensionsAsIntArray( fixedTileSearchRadiusBoundingBox );
					final SearchRadius searchRadius = fixedTileSearchRadius;
					threadPool.run( index ->
						{
							final int[] indexes = new int[ fixedTileSearchRadiusBoundingBox.numDimensions() ];
							IntervalIndexer.indexToPosition( index, dimensions, indexes );
							final double[] position = new double[ indexes.length ];
							for ( int d = 0; d < position.length; ++d )
								position[ d ] = indexes[ d ] + fixedTileSearchRadiusBoundingBox.min( d );
							if ( searchRadius.testPoint( position ) )
								fixedTileSearchRadiusTest[ indexes[ 0 ] ][ indexes[ 1 ] ][ indexes[ 2 ] ] = true;
						},
						( int ) Intervals.numElements( fixedTileSearchRadiusBoundingBox ) );
					for ( int i = 0; i < fixedTileSearchRadiusTest.length; ++i )
					{
						for ( int j = 0; j < fixedTileSearchRadiusTest[ i ].length; ++j )
						{
							for ( int k = 0; k < fixedTileSearchRadiusTest[ i ][ j ].length; ++k )
							{
								if ( fixedTileSearchRadiusTest[ i ][ j ][ k ] &&
										!(
												( i - 1 >= 0 && fixedTileSearchRadiusTest[ i - 1 ][ j ][ k ] ) &&
												( i + 1 < fixedTileSearchRadiusTest.length && fixedTileSearchRadiusTest[ i + 1 ][ j ][ k ] ) &&
												( j - 1 >= 0 && fixedTileSearchRadiusTest[ i ][ j - 1 ][ k ] ) &&
												( j + 1 < fixedTileSearchRadiusTest[ i ].length && fixedTileSearchRadiusTest[ i ][ j + 1 ][ k ] ) &&
												( k - 1 >= 0 && fixedTileSearchRadiusTest[ i ][ j ][ k - 1 ] ) &&
												( k + 1 < fixedTileSearchRadiusTest[ i ][ j ].length && fixedTileSearchRadiusTest[ i ][ j ][ k + 1 ] )
										)
									)
								{
									final int[] indexes = new int[] { i, j, k };
									for ( int d = 0; d < displayPosition.length; ++d )
										displayPosition[ d ] = fixedTileSearchRadiusBoundingBoxOffset[ d ] + indexes[ d ] - displayOffset[ d ];
									if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] && displayPosition[ 2 ] >= 0 && displayPosition[ 2 ] < displaySize[ 2 ] )
									{
										imgRandomAccess.setPosition( displayPosition );
										imgRandomAccess.get().set( fixedTileEllipseCountourColor );
									}
								}
							}
						}
					}

					threadPool.close();
				}

				System.out.println( "Drawing moving tile ellipse..." );
				// Draw moving tile ellipse contour
				{
					final int movingTileEllipseCountourColor = ARGBType.rgba( 0, 0xaa, 0xaa, 0xff );
					final int[] movingTileSearchRadiusBoundingBoxOffset = Intervals.minAsIntArray( movingTileSearchRadiusBoundingBox );
					final boolean[][][] movingTileSearchRadiusTest = new boolean[ ( int ) movingTileSearchRadiusBoundingBox.dimension( 0 ) ][ ( int ) movingTileSearchRadiusBoundingBox.dimension( 1 ) ][ ( int ) movingTileSearchRadiusBoundingBox.dimension( 2 ) ];

					final MultithreadedExecutor threadPool = new MultithreadedExecutor();

//					for ( int xTest = ( int ) movingTileSearchRadiusBoundingBox.min( 0 ); xTest <= ( int ) movingTileSearchRadiusBoundingBox.max( 0 ); ++xTest )
//						for ( int yTest = ( int ) movingTileSearchRadiusBoundingBox.min( 1 ); yTest <= ( int ) movingTileSearchRadiusBoundingBox.max( 1 ); ++yTest )
//							for ( int zTest = ( int ) movingTileSearchRadiusBoundingBox.min( 2 ); zTest <= ( int ) movingTileSearchRadiusBoundingBox.max( 2 ); ++zTest )
//								if ( movingTileSearchRadius.testPoint( xTest, yTest, zTest ) )
//									movingTileSearchRadiusTest[ xTest - movingTileSearchRadiusBoundingBoxOffset[ 0 ] ][ yTest - movingTileSearchRadiusBoundingBoxOffset[ 1 ] ][ zTest - movingTileSearchRadiusBoundingBoxOffset[ 2 ] ] = true;
					final int[] dimensions = Intervals.dimensionsAsIntArray( movingTileSearchRadiusBoundingBox );
					final SearchRadius searchRadius = movingTileSearchRadius;
					threadPool.run( index ->
						{
							final int[] indexes = new int[ movingTileSearchRadiusBoundingBox.numDimensions() ];
							IntervalIndexer.indexToPosition( index, dimensions, indexes );
							final double[] position = new double[ indexes.length ];
							for ( int d = 0; d < position.length; ++d )
								position[ d ] = indexes[ d ] + movingTileSearchRadiusBoundingBox.min( d );
							if ( searchRadius.testPoint( position ) )
								movingTileSearchRadiusTest[ indexes[ 0 ] ][ indexes[ 1 ] ][ indexes[ 2 ] ] = true;
						},
						( int ) Intervals.numElements( movingTileSearchRadiusBoundingBox ) );
					for ( int i = 0; i < movingTileSearchRadiusTest.length; ++i )
					{
						for ( int j = 0; j < movingTileSearchRadiusTest[ i ].length; ++j )
						{
							for ( int k = 0; k < movingTileSearchRadiusTest[ i ][ j ].length; ++k )
							{
								if ( movingTileSearchRadiusTest[ i ][ j ][ k ] &&
										!(
												( i - 1 >= 0 && movingTileSearchRadiusTest[ i - 1 ][ j ][ k ] ) &&
												( i + 1 < movingTileSearchRadiusTest.length && movingTileSearchRadiusTest[ i + 1 ][ j ][ k ] ) &&
												( j - 1 >= 0 && movingTileSearchRadiusTest[ i ][ j - 1 ][ k ] ) &&
												( j + 1 < movingTileSearchRadiusTest[ i ].length && movingTileSearchRadiusTest[ i ][ j + 1 ][ k ] ) &&
												( k - 1 >= 0 && movingTileSearchRadiusTest[ i ][ j ][ k - 1] ) &&
												( k + 1 < movingTileSearchRadiusTest[ i ][ j ].length && movingTileSearchRadiusTest[ i ][ j ][ k + 1 ] )
										)
									)
								{
									final int[] indexes = new int[] { i, j, k };
									for ( int d = 0; d < displayPosition.length; ++d )
										displayPosition[ d ] = movingTileSearchRadiusBoundingBoxOffset[ d ] + indexes[ d ] - displayOffset[ d ];
									if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] && displayPosition[ 2 ] >= 0 && displayPosition[ 2 ] < displaySize[ 2 ] )
									{
										imgRandomAccess.setPosition( displayPosition );
										imgRandomAccess.get().set( movingTileEllipseCountourColor );
									}
								}
							}
						}
					}

					threadPool.close();
				}

				// Draw combined ellipse contour
				/*{
					final int combinedEllipseCountourColor = ARGBType.rgba( 0xaa, 0, 0, 0xff );
					final int[] combinedSearchRadiusBoundingBoxOffset = Intervals.minAsIntArray( combinedSearchRadiusBoundingBox );
					final boolean[][][] combinedSearchRadiusTest = new boolean[ ( int ) combinedSearchRadiusBoundingBox.dimension( 0 ) ][ ( int ) combinedSearchRadiusBoundingBox.dimension( 1 ) ][ ( int ) combinedSearchRadiusBoundingBox.dimension( 2 ) ];
					for ( int xTest = ( int ) combinedSearchRadiusBoundingBox.min( 0 ); xTest <= ( int ) combinedSearchRadiusBoundingBox.max( 0 ); ++xTest )
						for ( int yTest = ( int ) combinedSearchRadiusBoundingBox.min( 1 ); yTest <= ( int ) combinedSearchRadiusBoundingBox.max( 1 ); ++yTest )
							for ( int zTest = ( int ) combinedSearchRadiusBoundingBox.min( 2 ); zTest <= ( int ) combinedSearchRadiusBoundingBox.max( 2 ); ++zTest )
								if ( combinedSearchRadius.testPoint( xTest, yTest, zTest ) )
									combinedSearchRadiusTest[ xTest - combinedSearchRadiusBoundingBoxOffset[ 0 ] ][ yTest - combinedSearchRadiusBoundingBoxOffset[ 1 ] ][ zTest - combinedSearchRadiusBoundingBoxOffset[ 2 ] ] = true;
					for ( int i = 0; i < combinedSearchRadiusTest.length; ++i )
					{
						for ( int j = 0; j < combinedSearchRadiusTest[ i ].length; ++j )
						{
							for ( int k = 0; k < combinedSearchRadiusTest[ i ][ j ].length; ++k )
							{
								if ( combinedSearchRadiusTest[ i ][ j ][ k ] &&
										!(
												( i - 1 >= 0 && combinedSearchRadiusTest[ i - 1 ][ j ][ k ] ) &&
												( i + 1 < combinedSearchRadiusTest.length && combinedSearchRadiusTest[ i + 1 ][ j ][ k ] ) &&
												( j - 1 >= 0 && combinedSearchRadiusTest[ i ][ j - 1 ][ k ] ) &&
												( j + 1 < combinedSearchRadiusTest[ i ].length && combinedSearchRadiusTest[ i ][ j + 1 ][ k ] ) &&
												( k - 1 >= 0 && combinedSearchRadiusTest[ i ][ j ][ k - 1 ] ) &&
												( k + 1 < combinedSearchRadiusTest[ i ][ j ].length && combinedSearchRadiusTest[ i ][ j ][ k + 1 ] )
										)
									)
								{
									final int[] indexes = new int[] { i, j, k };
									for ( int d = 0; d < displayPosition.length; ++d )
										displayPosition[ d ] = combinedSearchRadiusBoundingBoxOffset[ d ] + indexes[ d ] - displayOffset[ d ];
									if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] && displayPosition[ 2 ] >= 0 && displayPosition[ 2 ] < displaySize[ 2 ] )
									{
										imgRandomAccess.setPosition( displayPosition );
										imgRandomAccess.get().set( combinedEllipseCountourColor );
									}
								}
							}
						}
					}
				}*/

				// Draw combined ellipse bounding box
				/*{
					final int combinedEllipseBoundingBoxColor = ARGBType.rgba( 0x55, 0, 0, 0xff );
					final int[][] combinedEllipseBoundingBoxCorners = new int[][] { Intervals.minAsIntArray( combinedSearchRadiusBoundingBox ), Intervals.maxAsIntArray( combinedSearchRadiusBoundingBox ) };
					for ( int dFixed = 0; dFixed < 2; ++dFixed )
					{
						for ( int dMove = 0; dMove < 2; ++dMove )
						{
							if ( dFixed != dMove)
							{
								for ( int i = 0; i < 2; ++i )
								{
									displayPosition[ dFixed ] = combinedEllipseBoundingBoxCorners[ i ][ dFixed ] - displayOffset[ dFixed ];
									for ( int c = combinedEllipseBoundingBoxCorners[ 0 ][ dMove ]; c <= combinedEllipseBoundingBoxCorners[ 1 ][ dMove ]; ++c )
									{
										displayPosition[ dMove ] = c - displayOffset[ dMove ];
										if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
										{
											imgRandomAccess.setPosition( displayPosition );
											imgRandomAccess.get().set( combinedEllipseBoundingBoxColor );
										}
									}
								}
							}
						}
					}
				}*/


				System.out.println( "Drawing combined ellipse..." );
				// ---------------------------------------------------
				// Draw combined search radius contour obtained using covariance matrices to compare it against combined error ellipse
				{
					final SearchRadius combinedCovariances = estimator.getCombinedCovariancesSearchRadius( fixedTileSearchRadius, movingTileSearchRadius );
					final Interval combinedCovariancesBoundingBox = Intervals.smallestContainingInterval( combinedCovariances.getBoundingBox() );
					final int combinedCovariancesCountourColor = ARGBType.rgba( 0xff, 0, 0, 0xff );
					final int[] combinedCovariancesBoundingBoxOffset = Intervals.minAsIntArray( combinedCovariancesBoundingBox );
					final boolean[][][] combinedCovariancesTest = new boolean[ ( int ) combinedCovariancesBoundingBox.dimension( 0 ) ][ ( int ) combinedCovariancesBoundingBox.dimension( 1 ) ][ ( int ) combinedCovariancesBoundingBox.dimension( 2 ) ];

					final MultithreadedExecutor threadPool = new MultithreadedExecutor();

//					for ( int xTest = ( int ) combinedCovariancesBoundingBox.min( 0 ); xTest <= ( int ) combinedCovariancesBoundingBox.max( 0 ); ++xTest )
//						for ( int yTest = ( int ) combinedCovariancesBoundingBox.min( 1 ); yTest <= ( int ) combinedCovariancesBoundingBox.max( 1 ); ++yTest )
//							for ( int zTest = ( int ) combinedCovariancesBoundingBox.min( 2 ); zTest <= ( int ) combinedCovariancesBoundingBox.max( 2 ); ++zTest )
//								if ( combinedCovariances.testPoint( xTest, yTest, zTest ) )
//									combinedCovariancesTest[ xTest - combinedCovariancesBoundingBoxOffset[ 0 ] ][ yTest - combinedCovariancesBoundingBoxOffset[ 1 ] ][ zTest - combinedCovariancesBoundingBoxOffset[ 2 ] ] = true;
					final int[] dimensions = Intervals.dimensionsAsIntArray( combinedCovariancesBoundingBox );
					threadPool.run( index ->
						{
							final int[] indexes = new int[ combinedCovariancesBoundingBox.numDimensions() ];
							IntervalIndexer.indexToPosition( index, dimensions, indexes );
							final double[] position = new double[ indexes.length ];
							for ( int d = 0; d < position.length; ++d )
								position[ d ] = indexes[ d ] + combinedCovariancesBoundingBox.min( d );
							if ( combinedCovariances.testPoint( position ) )
								combinedCovariancesTest[ indexes[ 0 ] ][ indexes[ 1 ] ][ indexes[ 2 ] ] = true;
						},
						( int ) Intervals.numElements( combinedCovariancesBoundingBox ) );
					for ( int i = 0; i < combinedCovariancesTest.length; ++i )
					{
						for ( int j = 0; j < combinedCovariancesTest[ i ].length; ++j )
						{
							for ( int k = 0; k < combinedCovariancesTest[ i ][ j ].length; ++k )
							{
								if ( combinedCovariancesTest[ i ][ j ][ k ] &&
										!(
												( i - 1 >= 0 && combinedCovariancesTest[ i - 1 ][ j ][ k ] ) &&
												( i + 1 < combinedCovariancesTest.length && combinedCovariancesTest[ i + 1 ][ j ][ k ] ) &&
												( j - 1 >= 0 && combinedCovariancesTest[ i ][ j - 1 ][ k ] ) &&
												( j + 1 < combinedCovariancesTest[ i ].length && combinedCovariancesTest[ i ][ j + 1 ][ k ] ) &&
												( k - 1 >= 0 && combinedCovariancesTest[ i ][ j ][ k - 1 ] ) &&
												( k + 1 < combinedCovariancesTest[ i ][ j ].length && combinedCovariancesTest[ i ][ j ][ k + 1 ] )
										)
									)
								{
									final int[] indexes = new int[] { i, j, k };
									for ( int d = 0; d < displayPosition.length; ++d )
										displayPosition[ d ] = combinedCovariancesBoundingBoxOffset[ d ] + indexes[ d ] - displayOffset[ d ];
									if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] && displayPosition[ 2 ] >= 0 && displayPosition[ 2 ] < displaySize[ 2 ] )
									{
										imgRandomAccess.setPosition( displayPosition );
										imgRandomAccess.get().set( combinedCovariancesCountourColor );
									}
								}
							}
						}
					}

					threadPool.close();
				}
				// ---------------------------------------------------


				/*
				// ********************************************
				// Draw confidence interval for the expected offset using previous method (statistics using the entire set).
				{
					System.out.println();
					System.out.println( "Fixed tile: " + Utils.getTileCoordinatesString( fixedTile ) );
					System.out.println( "Moving tile: " + Utils.getTileCoordinatesString( movingTile ) );

					final ConfidenceIntervalStats confidenceIntervalStats = ConfidenceIntervalStats.fromJson( "/nrs/saalfeld/igor/170210_SomatoYFP_MBP_Caspr/stitching/flip-x/less-blur,smaller-radius/5z/restitching/confidence-interval.json" );
					final TreeMap< Integer, Integer > diffDimensions = new TreeMap<>();
					for ( int d = 0; d < 2; ++d )
						if ( Utils.getTileCoordinates( fixedTile )[ d ] != Utils.getTileCoordinates( movingTile )[ d ] )
							diffDimensions.put( d, Utils.getTileCoordinates( movingTile )[ d ] - Utils.getTileCoordinates( fixedTile )[ d ] );
					if ( diffDimensions.size() != 1 || Math.abs( diffDimensions.firstEntry().getValue() ) != 1 )
						Assert.fail( "diffDimensions: " + diffDimensions );

					final Interval confidenceInterval3D = confidenceIntervalStats.getConfidenceInterval( diffDimensions.firstKey(), diffDimensions.firstEntry().getValue().intValue() == 1 );
					final Interval confidenceInterval = new FinalInterval( new long[] { confidenceInterval3D.min( 0 ), confidenceInterval3D.min( 1 ) }, new long[] { confidenceInterval3D.max( 0 ), confidenceInterval3D.max( 1 ) } );
					final long[] confidenceIntervalShiftedMin = new long[ confidenceInterval.numDimensions() ], confidenceIntervalShiftedMax = new long[ confidenceInterval.numDimensions() ];
					for ( int d = 0; d < confidenceInterval.numDimensions(); ++d )
					{
						confidenceIntervalShiftedMin[ d ] = confidenceInterval.min( d ) + movingTile.getBoundaries().min( d );
						confidenceIntervalShiftedMax[ d ] = confidenceInterval.max( d ) + movingTile.getBoundaries().min( d );
					}
					final Interval confidenceIntervalShifted = new FinalInterval( confidenceIntervalShiftedMin, confidenceIntervalShiftedMax );

					System.out.println( "Confidence interval by previous method: size=" + Arrays.toString( Intervals.dimensionsAsIntArray( confidenceIntervalShifted ) ) );

					final int confidenceIntervalShiftedColor = ARGBType.rgba( 0, 0xcc, 0, 0xff );
					final int[][] confidenceIntervalShiftedCorners = new int[][] { Intervals.minAsIntArray( confidenceIntervalShifted ), Intervals.maxAsIntArray( confidenceIntervalShifted ) };
					for ( int dFixed = 0; dFixed < 2; ++dFixed )
					{
						for ( int dMove = 0; dMove < 2; ++dMove )
						{
							if ( dFixed != dMove)
							{
								for ( int i = 0; i < 2; ++i )
								{
									displayPosition[ dFixed ] = confidenceIntervalShiftedCorners[ i ][ dFixed ] - displayOffset[ dFixed ];
									for ( int c = confidenceIntervalShiftedCorners[ 0 ][ dMove ]; c <= confidenceIntervalShiftedCorners[ 1 ][ dMove ]; ++c )
									{
										displayPosition[ dMove ] = c - displayOffset[ dMove ];
										if ( displayPosition[ 0 ] >= 0 && displayPosition[ 0 ] < displaySize[ 0 ] && displayPosition[ 1 ] >= 0 && displayPosition[ 1 ] < displaySize[ 1 ] )
										{
											imgRandomAccess.setPosition( displayPosition );
											imgRandomAccess.get().set( confidenceIntervalShiftedColor );
										}
									}
								}
							}
						}
					}
				}
				System.out.println();
				// ********************************************
				*/


				// Display
				System.out.println( "Opening an image..." );
				final ImagePlus imp = img.getImagePlus();
				Utils.workaroundImagePlusNSlices( imp );

				new ImageJ();
				imp.show();
				Thread.sleep( 10 * 60 * 1000 );


				throw e;
			}
		}
	}


	private Pair< Interval, Interval > adjustOverlapRegion( final TilePair tilePair, final Pair< SearchRadius, SearchRadius > searchRadiusesPair, final SearchRadius combinedSearchRadius )
	{
		// adjust the ROI to capture the search radius entirely
		// try all corners of the bounding box of the search radius and use the largest overlaps

		final Interval searchRadiusBoundingBox = Intervals.smallestContainingInterval( combinedSearchRadius.getBoundingBox() );

		final TileInfo fixedTile = tilePair.getA();
		final TileInfo movingTile = tilePair.getB();

		final SearchRadius fixedTileSearchRadius = searchRadiusesPair.getA();
		final SearchRadius movingTileSearchRadius = searchRadiusesPair.getB();

		final Interval predictedFixedTile = Intervals.createMinSize(
				Math.round( fixedTileSearchRadius.getEllipseCenter()[ 0 ] ),
				Math.round( fixedTileSearchRadius.getEllipseCenter()[ 1 ] ),
				fixedTile.getSize( 0 ),
				fixedTile.getSize( 1 ) );

		final Interval predictedMovingTile = Intervals.createMinSize(
				Math.round( movingTileSearchRadius.getEllipseCenter()[ 0 ] ),
				Math.round( movingTileSearchRadius.getEllipseCenter()[ 1 ] ),
				movingTile.getSize( 0 ),
				movingTile.getSize( 1 ) );

		final Interval[] predictedIntervals = new Interval[] { predictedFixedTile, predictedMovingTile };
		final Interval[] overlaps = new Interval[ 2 ];
		final Interval newOverlapGlobal = Intervals.intersect( predictedIntervals[ 0 ], predictedIntervals[ 1 ] );
		final long[] paddingValues = new long[] { Math.round( searchRadiusBoundingBox.dimension( 0 ) / 2 ), Math.round( searchRadiusBoundingBox.dimension( 1 ) / 2 ) };
		for ( int j = 0; j < 2; ++j )
		{
			final Interval newOverlap = Intervals.translate( Intervals.translate( newOverlapGlobal, -predictedIntervals[ j ].min( 0 ), 0 ), -predictedIntervals[ j ].min( 1 ), 1 );
			final long[] newMins = new long[ 2 ], newMaxs = new long[ 2 ];
			for ( int d = 0; d < 2; ++d )
			{
				final long tileSize = tilePair.toArray()[ j ].getSize( d );
				if ( newOverlap.min( d ) == 0 )
				{
					newMins[ d ] = newOverlap.min( d );
					newMaxs[ d ] = Math.min( newOverlap.max( d ) + paddingValues[ d ], tileSize - 1 );
				}
				else if ( newOverlap.max( d ) == tileSize - 1 )
				{
					newMins[ d ] = Math.max( newOverlap.min( d ) - paddingValues[ d ], 0 );
					newMaxs[ d ] = newOverlap.max( d );
				}
				else
					throw new RuntimeException( "Unexpected overlap size/position" );
			}
			overlaps[ j ] = new FinalInterval( newMins, newMaxs );
		}

		return new ValuePair<>( overlaps[ 0 ], overlaps[ 1 ] );
	}
}
