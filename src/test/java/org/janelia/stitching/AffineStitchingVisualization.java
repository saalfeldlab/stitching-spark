package org.janelia.stitching;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.janelia.stitching.OffsetUncertaintyEstimator.EstimatedRelativeSearchRadius;
import org.janelia.stitching.OffsetUncertaintyEstimator.EstimatedWorldSearchRadius;
import org.janelia.util.Conversions;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.RealPoint;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.neighborhood.DiamondShape.NeighborhoodsIterableInterval;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.img.imageplus.IntImagePlus;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.Translation;
import net.imglib2.realtransform.Translation2D;
import net.imglib2.type.logic.BitType;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;

public class AffineStitchingVisualization
{
	public static void main( final String[] args ) throws Exception
	{
		final String logDirectory = args.length > 0 ? args[ 0 ] : null;
		new AffineStitchingVisualization( logDirectory );
	}

	final long[] tileSize = new long[] { 100, 150 };

	final int[] tilePairIndexes = new int[] { 10, 11 };

	final int[] tileBoxPairIndexes = new int[] { 3, 1 };

	final double searchRadiusMultiplier = 3; // 3 * sigma

	final double errorEllipseRadiusAsTileSizeRatio = 0.1;

	final int[] tileEstimationWindow = new int[] { 3, 3 }; // 3x size of a tile

	final int minNumNeighboringTiles = 3;

	final boolean weightedPredictions = false;

	final boolean modifyLinearComponent = true;

	private AffineStitchingVisualization( final String logDirectory ) throws Exception
	{
		new ImageJ();

		final TileInfo[] tiles = getTiles();

		final List< SubTile > tileBoxes = SubTileOperations.subdivideTiles( tiles, new int[] { 2, 2 } );
		@SuppressWarnings( "unchecked" )
		final List< SubTile >[] tilePairBoxes = new ArrayList[] { new ArrayList<>(), new ArrayList<>() };
		for ( final SubTile tileBox : tileBoxes )
			for ( int i = 0; i < 2; ++i )
				if ( tileBox.getFullTile().getIndex().intValue() == tilePairIndexes[ i ] )
					tilePairBoxes[ i ].add( tileBox );

		final double[] searchWindowSize = NeighboringTilesLocator.getSearchWindowSize( tiles[ 0 ].getSize(), tileEstimationWindow );
		final NeighboringTilesLocator neighboringTilesLocator = new NeighboringTilesLocator( tiles, searchWindowSize, 2 );

		final SampleWeightCalculator sampleWeightCalculator = new SampleWeightCalculator( !weightedPredictions );

		final OffsetUncertaintyEstimator searchRadiusEstimator = new OffsetUncertaintyEstimator(
				neighboringTilesLocator,
				sampleWeightCalculator,
				searchRadiusMultiplier,
				minNumNeighboringTiles
			);

		final AffineGet[] estimatedTileTransforms = new AffineGet[] {
				tiles[ tilePairIndexes[ 0 ] ].getTransform(), // fixed tile already has an existing transformation
				TileTransformEstimator.estimateAffineTransformation(  // estimate the transform for the moving tile
						tiles[ tilePairIndexes[ 1 ] ],
						neighboringTilesLocator,
						sampleWeightCalculator
					)
			};

		System.out.println( "Estimated transform of the moving tile: " + estimatedTileTransforms[ 1 ] );
		final double[] simulatedMovingBoundingBoxOffset = new double[] { 12, -11 };

		final TilePair tilePair = new TilePair( tiles[ tilePairIndexes[ 0 ] ], tiles[ tilePairIndexes[ 1 ] ] );

		final SubTilePair tileBoxPair = new SubTilePair(
				tilePairBoxes[ 0 ].get( tileBoxPairIndexes[ 0 ] ),
				tilePairBoxes[ 1 ].get( tileBoxPairIndexes[ 1 ] )
			);

		final EstimatedRelativeSearchRadius movingBoxRelativeSearchRadius = PairwiseTileOperations.getCombinedSearchRadiusForMovingSubTile(
				tileBoxPair.toArray(),
				searchRadiusEstimator
			);

		// write out offset stats
		if  ( logDirectory != null )
		{
			writeOutOffsetStats( movingBoxRelativeSearchRadius, logDirectory );
			System.out.println( "offset stats have been written to file" );
		}

		// draw configuration
		final int[] worldDisplaySize = new int[] { 1600, 800 };
		final IntImagePlus< ARGBType > imgWorld = ImagePlusImgs.argbs( worldDisplaySize[ 0 ], worldDisplaySize[ 1 ] );
		final RandomAccess< ARGBType > imgWorldRandomAccess = imgWorld.randomAccess();

		drawStageTiles( tiles, worldDisplaySize, imgWorldRandomAccess, new ARGBType( ARGBType.rgba( 255, 0, 0, 32 ) ) );
		drawStitchedTiles( tiles, worldDisplaySize, imgWorldRandomAccess, new ARGBType( ARGBType.rgba( 0, 255, 0, 32 ) ) );

		drawTileBoxes( tilePairBoxes[ 0 ], worldDisplaySize, imgWorldRandomAccess, new ARGBType( ARGBType.rgba( 0, 96, 96, 32 ) ) );
		drawTileBoxes( tilePairBoxes[ 1 ], worldDisplaySize, imgWorldRandomAccess, new ARGBType( ARGBType.rgba( 96, 0, 96, 32 ) ) );

		// draw transformed tiles
		drawTransformedRectangle(
				getLocalRealIntervalCorners( tilePair.getA().getStageInterval() ),
				estimatedTileTransforms[ 0 ],
				worldDisplaySize,
				imgWorldRandomAccess,
				new ARGBType( ARGBType.rgba( 64, 64, 255, 255 ) )
			);
		drawTransformedRectangle(
				getLocalRealIntervalCorners( tilePair.getB().getStageInterval() ),
				estimatedTileTransforms[ 1 ],
				worldDisplaySize,
				imgWorldRandomAccess,
				new ARGBType( ARGBType.rgba( 255, 255, 255, 64 ) )
			);

		// draw transformed tile boxes
		drawTransformedRectangle(
				getRealIntervalCorners( tileBoxPair.getA() ),
				estimatedTileTransforms[ 0 ],
				worldDisplaySize,
				imgWorldRandomAccess,
				new ARGBType( ARGBType.rgba( 64, 64, 255, 255 ) )
			);
		drawTransformedRectangle(
				getRealIntervalCorners( tileBoxPair.getB() ),
				estimatedTileTransforms[ 1 ],
				worldDisplaySize,
				imgWorldRandomAccess,
				new ARGBType( ARGBType.rgba( 192, 192, 192, 255 ) )
			);

		// draw combined error ellipse (moving+fixed)
		movingBoxRelativeSearchRadius.combinedErrorEllipse.setErrorEllipseTransform(
				new Translation2D( Intervals.minAsDoubleArray( tileBoxPair.getB() ) )
			);
		drawErrorEllipse(
				movingBoxRelativeSearchRadius.combinedErrorEllipse,
				estimatedTileTransforms[ 1 ],
				worldDisplaySize,
				imgWorldRandomAccess,
				new ARGBType( ARGBType.rgba( 255, 255, 0, 255 ) )
			);

		final ImagePlus impWorld = imgWorld.getImagePlus();
		impWorld.show();



		final int[] localDisplaySize = new int[] { 800, 400 };
		final Translation2D displayOffsetTransform = new Translation2D( localDisplaySize[ 0 ] / 2, localDisplaySize[ 1 ] / 2 );

		final InvertibleRealTransform movingTileToFixedBoxTransform = PairwiseTileOperations.getMovingTileToFixedSubTileTransform(
				tileBoxPair.toArray(),
				estimatedTileTransforms
			);

		final InvertibleRealTransformSequence movingTileToDisplayTransform = new InvertibleRealTransformSequence();
		movingTileToDisplayTransform.add( movingTileToFixedBoxTransform );
		movingTileToDisplayTransform.add( displayOffsetTransform );

		final IntImagePlus< ARGBType > imgLocal = ImagePlusImgs.argbs( localDisplaySize[ 0 ], localDisplaySize[ 1 ] );
		final RandomAccess< ARGBType > imgLocalRandomAccess = imgLocal.randomAccess();

		drawAxes( localDisplaySize, imgLocalRandomAccess );

		// draw bounding box of the moving tile box
		final RealInterval transformedMovingBoxInterval = TransformedTileOperations.getTransformedBoundingBoxReal(
				tileBoxPair.getB(),
				movingTileToFixedBoxTransform
			);
		drawTransformedRectangle(
				getRealIntervalCorners( transformedMovingBoxInterval ),
				displayOffsetTransform,
				localDisplaySize,
				imgLocalRandomAccess,
				new ARGBType( ARGBType.rgba( 64, 64, 64, 64 ) )
			);

		// draw zero-min fixed tile box
		drawTransformedRectangle(
				getLocalRealIntervalCorners( tileBoxPair.getA() ),
				displayOffsetTransform,
				localDisplaySize,
				imgLocalRandomAccess,
				new ARGBType( ARGBType.rgba( 64, 64, 255, 255 ) )
			);

		// draw transformed moving tile box
		drawTransformedRectangle(
				getRealIntervalCorners( tileBoxPair.getB() ),
				movingTileToDisplayTransform,
				localDisplaySize,
				imgLocalRandomAccess,
				new ARGBType( ARGBType.rgba( 192, 192, 192, 255 ) )
			);


		// account for the offset between zero-min of the transformed tile and the ROI
		final double[] transformedMovingTileBoxToBoundingBoxOffset = PairwiseTileOperations.getTransformedMovingSubTileToBoundingBoxOffset(
				tileBoxPair.toArray(),
				estimatedTileTransforms
			);
		printDoubleArray(
				System.lineSeparator() + "Offset between transformed top-left point and bounding box: %s",
				transformedMovingTileBoxToBoundingBoxOffset,
				2
			);

		movingBoxRelativeSearchRadius.combinedErrorEllipse.setErrorEllipseTransform(
				PairwiseTileOperations.getErrorEllipseTransform( tileBoxPair.toArray(), estimatedTileTransforms )
			);

		// draw transformed search radius in the fixed box space
		System.out.println( System.lineSeparator() + "Drawing world->local transformed error ellipse..." );
		drawErrorEllipse(
				movingBoxRelativeSearchRadius.combinedErrorEllipse,
				displayOffsetTransform,
				localDisplaySize,
				imgLocalRandomAccess,
				new ARGBType( ARGBType.rgba( 0, 255, 255, 255 ) )
			);

		// draw transformed uncorrelated error ellipse in the fixed box space
		final double[] uncorrelatedErrorEllipseRadius = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipseRadius(
				tileBoxPair.getA().getFullTile().getSize(),
				errorEllipseRadiusAsTileSizeRatio
			);
		final ErrorEllipse movingBoxUncorrelatedErrorEllipse = OffsetUncertaintyEstimator.getUncorrelatedErrorEllipse( uncorrelatedErrorEllipseRadius );
		movingBoxUncorrelatedErrorEllipse.setErrorEllipseTransform( PairwiseTileOperations.getErrorEllipseTransform( tileBoxPair.toArray(), estimatedTileTransforms ) );
		drawErrorEllipse(
				movingBoxUncorrelatedErrorEllipse,
				displayOffsetTransform,
				localDisplaySize,
				imgLocalRandomAccess,
				new ARGBType( ARGBType.rgba( 64, 255, 64, 255 ) )
			);

		final ImagePlus impLocal = imgLocal.getImagePlus();
		impLocal.show();

		Thread.sleep( 3000 );


		// validate ellipse test by drawing boundaries after testing all display points
		System.out.println( System.lineSeparator() + "Drawing result of testing display points against the transformed error ellipse..." );
		drawErrorEllipseByInverseTest(
				movingBoxRelativeSearchRadius.combinedErrorEllipse,
				displayOffsetTransform,
				localDisplaySize,
				imgLocalRandomAccess,
				new ARGBType( ARGBType.rgba( 255, 255, 0, 255 ) )
			);
		impLocal.updateAndDraw();
		System.out.println( "Done" );

		Thread.sleep( 3000 );


		// draw new offset position of moving tile box
		drawTransformedRectangle(
				getLocalRealIntervalCorners( transformedMovingBoxInterval ),
				new Translation2D( simulatedMovingBoundingBoxOffset ).concatenate( displayOffsetTransform ),
				localDisplaySize,
				imgLocalRandomAccess,
				new ARGBType( ARGBType.rgba( 64, 64, 0, 64 ) )
			);
		impLocal.updateAndDraw();



		// test that simulated offset is within the error ellipse
		System.out.println();
		if ( movingBoxRelativeSearchRadius.combinedErrorEllipse.testOffset( simulatedMovingBoundingBoxOffset ) )
			System.out.println( "New offset is inside the accepted error range" );
		else
			System.out.println( "New offset is outside the accepted error range" );


		// map the new offset back to the global space
//		final AffineGet newMovingTileTransform = PairwiseTileOperations.getNewMovingTileTransform(
//				tileBoxPair.toArray(),
//				estimatedTileTransforms,
//				simulatedMovingBoundingBoxOffset
//			);

		// draw moving tile at its new world position
//		drawTransformedRectangle(
//				getLocalRealIntervalCorners( tilePair.getB().getStageInterval() ),
//				newMovingTileTransform,
//				worldDisplaySize,
//				imgWorldRandomAccess,
//				new ARGBType( ARGBType.rgba( 64, 64, 0, 64 ) )
//			);

		// draw moving tile box at its new world position
//		drawTransformedRectangle(
//				getRealIntervalCorners( tileBoxPair.getB() ),
//				newMovingTileTransform,
//				worldDisplaySize,
//				imgWorldRandomAccess,
//				new ARGBType( ARGBType.rgba( 64, 64, 0, 64 ) )
//			);

		impWorld.updateAndDraw();


		// find offset between the fixed and moving tiles in the offset space
//		final double[] newTranslatedOffset = PairwiseTileOperations.getNewStitchedOffset(
//				tileBoxPair.toArray(),
//				estimatedTileTransforms,
//				simulatedMovingBoundingBoxOffset
//			);
//		System.out.println( System.lineSeparator() + "New offset in translated space: " + Arrays.toString( newTranslatedOffset ) );

		System.out.println( System.lineSeparator() + "OK" );
	}

	private TileInfo[] getTiles()
	{
		final int[] tileGridSize = new int[] { 4, 3 };

		final List< double[] > stagePositions = new ArrayList<>();
		final double[] topLeftTileStagePosition = new double[] { 200, 100 };
		final double[] stageOverlapPercent = new double[] { 0.2, 0.2 };
		for ( int row = 0; row < tileGridSize[ 1 ]; ++row )
		{
			for ( int col = 0; col < tileGridSize[ 0 ]; ++col )
			{
				final int[] gridIndex = new int[] { col, row };
				final double[] stagePosition = new double[ 2 ];
				for ( int d = 0; d < stagePosition.length; ++d )
					stagePosition[ d ] = Math.round( topLeftTileStagePosition[ d ] + ( tileSize[ d ] * ( 1.0 - stageOverlapPercent[ d ] ) ) * gridIndex[ d ] );
				stagePositions.add( stagePosition );
			}
		}

		final List< double[] > stitchedPositions = new ArrayList<>();
		final double[] topLeftTileStitchedPosition = new double[] { 800, 100 };
		final double[] stitchedOverlapPercent = new double[] { 0.1, 0.3 };
		for ( int row = 0; row < tileGridSize[ 1 ]; ++row )
		{
			for ( int col = 0; col < tileGridSize[ 0 ]; ++col )
			{
				final int[] gridIndex = new int[] { col, row };
				final double[] stitchedPosition = new double[ 2 ];
				for ( int d = 0; d < stitchedPosition.length; ++d )
					stitchedPosition[ d ] = Math.round( topLeftTileStitchedPosition[ d ] + ( tileSize[ d ] * ( 1.0 - stitchedOverlapPercent[ d ] ) ) * gridIndex[ d ] );
				stitchedPositions.add( stitchedPosition );
			}
		}
		stitchedPositions.set( stitchedPositions.size() - 1, null ); // to be stitched

		final long[] halfTileSize = new long[ tileSize.length ];
		for ( int d = 0; d < halfTileSize.length; ++d )
			halfTileSize[ d ] = tileSize[ d ] / 2;

		final double rotationAngleStep = 5;
		final List< TileInfo > tiles = new ArrayList<>();
		for ( int i = 0; i < stagePositions.size(); ++i )
		{
			final TileInfo tile = new TileInfo( 2 );
			tile.setIndex( i );
			tile.setSize( tileSize );
			tile.setStagePosition( stagePositions.get( i ) );

			if ( stitchedPositions.get( i ) != null )
			{
				final AffineTransform2D rotationTransform = new AffineTransform2D();
				rotationTransform.rotate( Math.toRadians( rotationAngleStep * ( i % tileGridSize[ 0 ] ) ) );

				final AffineTransform2D stitchedTransform = new AffineTransform2D();
				stitchedTransform
					.preConcatenate( new Translation( Conversions.toDoubleArray( halfTileSize ) ).inverse() )
					.preConcatenate( rotationTransform ) // rotation around the middle point of the tile
					.preConcatenate( new Translation( Conversions.toDoubleArray( halfTileSize ) ) )
					.preConcatenate( new Translation( stitchedPositions.get( i ) ) );

				tile.setTransform( stitchedTransform );
			}

			tiles.add( tile );
		}

		return tiles.toArray( new TileInfo[ 0 ] );
	}

	private void writeOutOffsetStats( final EstimatedRelativeSearchRadius combinedSearchRadius, final String logDirectory ) throws IOException
	{
		final String baseFileName = "offset-stats.csv";
		final String[] fileNameSuffixes = new String[] { "fixed", "moving" };
		final String[] axesStr = new String[] { "x", "y", "z" };
		final String delimiter = " ";

		for ( int i = 0; i < 2; ++i )
		{
			final String fileName = Paths.get( logDirectory, Utils.addFilenameSuffix( baseFileName, "_" + fileNameSuffixes[ i ] ) ).toString();
			final EstimatedWorldSearchRadius searchRadius = combinedSearchRadius.worldSearchRadiusStats[ i ];
			try ( final PrintWriter writer = new PrintWriter(fileName ) )
			{
				for ( int d = 0; d < searchRadius.errorEllipse.numDimensions(); ++d )
					writer.print( "stage_" + axesStr[ d ] + delimiter );

				for ( int d = 0; d < searchRadius.errorEllipse.numDimensions(); ++d )
					writer.print( ( d == 0 ? "" : delimiter ) + "world_" + axesStr[ d ] );

				writer.println();

				for ( final Pair< RealPoint, RealPoint > stageAndWorld : searchRadius.stageAndWorldCoordinates )
				{
					for ( int d = 0; d < stageAndWorld.getA().numDimensions(); ++d )
						writer.print( String.format( "%.2f", stageAndWorld.getA().getDoublePosition( d ) ) + delimiter );

					for ( int d = 0; d < stageAndWorld.getB().numDimensions(); ++d )
						writer.print( ( d == 0 ? "" : delimiter ) + String.format( "%.2f", stageAndWorld.getB().getDoublePosition( d ) ) );

					writer.println();
				}
			}
		}

		System.out.println();
		for ( int i = 0; i < 2; ++i )
		{
			final EstimatedWorldSearchRadius searchRadius = combinedSearchRadius.worldSearchRadiusStats[ i ];
			System.out.println( fileNameSuffixes[ i ] + " search radius offsets:" );
			for ( final Pair< RealPoint, RealPoint > stageAndWorld : searchRadius.stageAndWorldCoordinates )
			{
				final double[] offset = new double[ Math.max( stageAndWorld.getA().numDimensions(), stageAndWorld.getB().numDimensions() ) ];
				for ( int d = 0; d < offset.length; ++d )
					offset[ d ] = stageAndWorld.getB().getDoublePosition( d ) - stageAndWorld.getA().getDoublePosition( d );
				printDoubleArray( offset, 2 );
			}
			System.out.println();
		}
	}

	private void drawStageTiles(
			final TileInfo[] tiles,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color )
	{
		for ( final TileInfo tile : tiles )
		{
			drawTransformedRectangle(
					getRealIntervalCorners( tile.getStageInterval() ),
					null,
					displaySize,
					imgRandomAccess,
					color
				);
		}
	}

	private void drawStitchedTiles(
			final TileInfo[] tiles,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color )
	{
		for ( final TileInfo tile : tiles )
		{
			if ( tile.getTransform() != null )
			{
				drawTransformedRectangle(
						getLocalRealIntervalCorners( tile.getStageInterval() ),
						tile.getTransform(),
						displaySize,
						imgRandomAccess,
						color
					);
			}
		}
	}

	private void drawTileBoxes(
			final List< SubTile > tileBoxes,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color )
	{
		for ( final SubTile tileBox : tileBoxes )
		{
			final AffineTransform2D tileTransform = new AffineTransform2D();
			for ( int d = 0; d < tileBox.getFullTile().numDimensions(); ++d )
				tileTransform.set( tileBox.getFullTile().getStagePosition( d ), d, 2 );

			drawTransformedRectangle(
					getRealIntervalCorners( tileBox ),
					tileTransform,
					displaySize,
					imgRandomAccess,
					color
				);
		}
	}

	private Set< Integer > drawTransformedRectangle(
			final double[][] rectCorners,
			final RealTransform pointTransform,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color )
	{
		final int dim = displaySize.length;
		final double[] pos = new double[ dim ];
		final int[] posDisplay = new int[ dim ];

		final Set< Integer > visitedPixels = new HashSet<>();

		for ( int dMove = 0; dMove < dim; ++dMove )
		{
			final int[] fixedDimsPossibilities = new int[ dim - 1 ];
			Arrays.fill( fixedDimsPossibilities, 2 );
			final IntervalIterator fixedDimsPossibilitiesIterator = new IntervalIterator( fixedDimsPossibilities );

			final List< Integer > fixedDims = new ArrayList<>();
			for ( int d = 0; d < dim; ++d )
				if ( d != dMove )
					fixedDims.add( d );

			while ( fixedDimsPossibilitiesIterator.hasNext() )
			{
				fixedDimsPossibilitiesIterator.fwd();

				for ( double c = rectCorners[ 0 ][ dMove ]; c <= rectCorners[ 1 ][ dMove ]; c += 0.1 )
				{
					pos[ dMove ] = c;
					for ( int fixedDimIndex = 0; fixedDimIndex < fixedDims.size(); ++fixedDimIndex )
					{
						final int dFixed = fixedDims.get( fixedDimIndex );
						final int minOrMax = fixedDimsPossibilitiesIterator.getIntPosition( fixedDimIndex );
						pos[ dFixed ] = rectCorners[ minOrMax ][ dFixed ];
					}

					if ( pointTransform != null )
						pointTransform.apply( pos, pos );

					for ( int d = 0; d < dim; ++d )
						posDisplay[ d ] = ( int ) Math.round( pos[ d ] );

					boolean insideView = true;
					for ( int d = 0; d < dim; ++d )
						insideView &= ( posDisplay[ d ] >= 0 && posDisplay[ d ] < displaySize[ d ] );

					if ( insideView )
					{
						final int pixelIndex = IntervalIndexer.positionToIndex( posDisplay, displaySize );
						if ( !visitedPixels.contains( pixelIndex ) )
						{
							imgRandomAccess.setPosition( posDisplay );
							imgRandomAccess.get().set( color );
							visitedPixels.add( pixelIndex );
						}
					}
				}
			}
		}

		return visitedPixels;
	}

	private Set< Integer > drawErrorEllipse(
			final ErrorEllipse errorEllipse,
			final RealTransform displayTransform,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color )
	{
		final int dim = displaySize.length;
		final double[] pos = new double[ dim ];
		final int[] posDisplay = new int[ dim ];

		final Interval errorEllipseBoundingBox = Intervals.smallestContainingInterval( errorEllipse.estimateBoundingBox() );

		System.out.println( "Error ellipse bounding box size: " + Arrays.toString( Intervals.dimensionsAsLongArray( errorEllipseBoundingBox ) ) );

		final RandomAccessibleInterval< BitType > errorEllipseTestImg = Views.translate(
				ArrayImgs.bits( Intervals.dimensionsAsLongArray( errorEllipseBoundingBox ) ),
				Intervals.minAsLongArray( errorEllipseBoundingBox )
			);

		final Cursor< BitType > errorEllipseTestImgCursor = Views.iterable( errorEllipseTestImg ).localizingCursor();
		while ( errorEllipseTestImgCursor.hasNext() )
		{
			errorEllipseTestImgCursor.fwd();
			errorEllipseTestImgCursor.localize( pos );
			if ( errorEllipse.testOffset( pos ) )
				errorEllipseTestImgCursor.get().setOne();
		}

		final List< RealPoint > boundaryPoints = findBoundaryPoints( errorEllipseTestImg );
		final Set< Integer > visitedPixels = new HashSet<>();

		for ( final RealPoint pt : boundaryPoints )
		{
			pt.localize( pos );

			if ( displayTransform != null )
				displayTransform.apply( pos, pos );

			for ( int d = 0; d < dim; ++d )
				posDisplay[ d ] = ( int ) Math.round( pos[ d ] );

			boolean insideView = true;
			for ( int d = 0; d < dim; ++d )
				insideView &= ( posDisplay[ d ] >= 0 && posDisplay[ d ] < displaySize[ d ] );

			if ( insideView )
			{
				final int pixelIndex = IntervalIndexer.positionToIndex( posDisplay, displaySize );
				if ( !visitedPixels.contains( pixelIndex ) )
				{
					imgRandomAccess.setPosition( posDisplay );
					imgRandomAccess.get().set( color );
					visitedPixels.add( pixelIndex );
				}
			}
		}

		return visitedPixels;
	}

	private Set< Integer > drawErrorEllipseByInverseTest(
			final ErrorEllipse errorEllipse,
			final AffineGet displayTransform,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color )
	{
		final int dim = displaySize.length;
		final double[] pos = new double[ dim ];
		final int[] posDisplay = new int[ dim ];

		final RandomAccessibleInterval< BitType > displayTestImg = ArrayImgs.bits( Conversions.toLongArray( displaySize ) );
		final Cursor< BitType > displayTestImgCursor = Views.iterable( displayTestImg ).localizingCursor();
		while ( displayTestImgCursor.hasNext() )
		{
			displayTestImgCursor.fwd();
			displayTestImgCursor.localize( pos );
			displayTransform.applyInverse( pos, pos );
			if ( errorEllipse.testOffset( pos ) )
				displayTestImgCursor.get().setOne();
		}

		final List< RealPoint > boundaryPoints = findBoundaryPoints( displayTestImg );
		final Set< Integer > visitedPixels = new HashSet<>();

		for ( final RealPoint pt : boundaryPoints )
		{
			pt.localize( pos );
			for ( int d = 0; d < dim; ++d )
				posDisplay[ d ] = ( int ) Math.round( pos[ d ] );

			boolean insideView = true;
			for ( int d = 0; d < dim; ++d )
				insideView &= ( posDisplay[ d ] >= 0 && posDisplay[ d ] < displaySize[ d ] );

			if ( insideView )
			{
				final int pixelIndex = IntervalIndexer.positionToIndex( posDisplay, displaySize );
				if ( !visitedPixels.contains( pixelIndex ) )
				{
					imgRandomAccess.setPosition( posDisplay );
					imgRandomAccess.get().set( color );
					visitedPixels.add( pixelIndex );
				}
			}
		}

		return visitedPixels;
	}

	private List< RealPoint > findBoundaryPoints( final RandomAccessibleInterval< BitType > binaryImg )
	{
		final List< RealPoint > transformedBoundaryPoints = new ArrayList<>();
		final NeighborhoodsIterableInterval< BitType > neighborhoods = new DiamondShape( 1 ).neighborhoodsSafe( binaryImg );
		final Cursor< Neighborhood< BitType > > neighborhoodsCursor = neighborhoods.localizingCursor();
		final RandomAccess< BitType > binaryImgRandomAccess = binaryImg.randomAccess();
		while ( neighborhoodsCursor.hasNext() )
		{
			final Neighborhood< BitType > neighborhood = neighborhoodsCursor.next();
			binaryImgRandomAccess.setPosition( neighborhoodsCursor );
			if ( binaryImgRandomAccess.get().get() )
			{
				// it is a contour point if it is inside the ellipse but at least one of the neighbors is outside the ellipse
				boolean isInnerPoint = true;

				final Cursor< BitType > neighborhoodCursor = neighborhood.cursor();
				while ( neighborhoodCursor.hasNext() )
				{
					neighborhoodCursor.fwd();

					boolean insideInterval = true;
					for ( int d = 0; d < neighborhoodCursor.numDimensions(); ++d )
						insideInterval &= neighborhoodCursor.getLongPosition( d ) >= binaryImg.min( d ) && neighborhoodCursor.getLongPosition( d ) <= binaryImg.max( d );

					if ( insideInterval )
						isInnerPoint &= neighborhoodCursor.get().get();
				}

				if ( !isInnerPoint )
					transformedBoundaryPoints.add( new RealPoint( neighborhoodsCursor ) );
			}
		}
		return transformedBoundaryPoints;
	}

	private void drawAxes( final int[] displaySize, final RandomAccess< ARGBType > imgRandomAccess )
	{
		final ARGBType axesColor = new ARGBType( ARGBType.rgba( 32, 32, 32, 32 ) );
		for ( int d = 0; d < displaySize.length; ++d )
		{
			final int[] displayPos = new int[ displaySize.length ];
			for ( int i = 0; i < displaySize.length; ++i )
				displayPos[ i ] = displaySize[ i ] / 2;

			for ( int c = 0; c < displaySize[ d ]; ++c )
			{
				displayPos[ d ] = c;
				imgRandomAccess.setPosition( displayPos );
				imgRandomAccess.get().set( axesColor );
			}
		}
	}

	private double[][] getRealIntervalCorners( final RealInterval realInterval )
	{
		return new double[][] {
				Intervals.minAsDoubleArray( realInterval ),
				Intervals.maxAsDoubleArray( realInterval )
		};
	}

	private double[][] getLocalRealIntervalCorners( final RealInterval realInterval )
	{
		final double[] localMax = new double[ realInterval.numDimensions() ];
		for ( int d = 0; d < realInterval.numDimensions(); ++d )
			localMax[ d ] = realInterval.realMax( d ) - realInterval.realMin( d );

		return new double[][] {
				new double[ realInterval.numDimensions() ],
				localMax
		};
	}

	private static void printDoubleArray( final double[] arr, final int numDecimalPlaces )
	{
		printDoubleArray( "%s", arr, numDecimalPlaces );
	}
	private static void printDoubleArray( final String format, final double[] arr, final int numDecimalPlaces )
	{
		final String[] arrStr = new String[ arr.length ];
		for ( int d = 0; d < arrStr.length; ++d )
			arrStr[ d ] = String.format( "%." + numDecimalPlaces + "f", arr[ d ] );
		System.out.println( String.format( format, Arrays.toString( arrStr ) ) );
	}
}
