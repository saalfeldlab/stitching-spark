package org.janelia.stitching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.janelia.stitching.TileSearchRadiusEstimator.EstimatedTileBoxSearchRadius;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.neighborhood.DiamondShape.NeighborhoodsIterableInterval;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.img.imageplus.IntImagePlus;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.Translation2D;
import net.imglib2.type.logic.BitType;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class AffineStitchingVisualization
{
	public static void main( final String[] args ) throws Exception
	{
		new AffineStitchingVisualization();
	}

	final long[] tileSize = new long[] { 100, 150 };

	final int[] displaySize = new int[] { 1600, 800 };

	final int[] tilePairIndexes = new int[] { 5, 6 };

	final int[] tileBoxPairIndexes = new int[] { 3, 1 };

	final double searchRadiusMultiplier = 3; // 3 * sigma

	private AffineStitchingVisualization() throws ImgLibException, PipelineExecutionException
	{
		final TileInfo[] tiles = getTiles();

		final IntImagePlus< ARGBType > img = ImagePlusImgs.argbs( displaySize[ 0 ], displaySize[ 1 ] );
		final RandomAccess< ARGBType > imgRandomAccess = img.randomAccess();

		drawStageTiles( tiles, imgRandomAccess, new ARGBType( ARGBType.rgba( 255, 0, 0, 32 ) ) );
		drawStitchedTiles( tiles, imgRandomAccess, new ARGBType( ARGBType.rgba( 0, 255, 0, 32 ) ) );

		final List< SubdividedTileBox > tileBoxes = SplitTileOperations.splitTilesIntoBoxes( tiles, new int[] { 2, 2 } );
		@SuppressWarnings( "unchecked" )
		final List< SubdividedTileBox >[] tilePairBoxes = new ArrayList[] { new ArrayList<>(), new ArrayList<>() };
		for ( final SubdividedTileBox tileBox : tileBoxes )
			for ( int i = 0; i < 2; ++i )
				if ( tileBox.getFullTile().getIndex().intValue() == tilePairIndexes[ i ] )
					tilePairBoxes[ i ].add( tileBox );

		drawTileBoxes( tilePairBoxes[ 0 ], imgRandomAccess, new ARGBType( ARGBType.rgba( 0, 96, 96, 32 ) ) );
		drawTileBoxes( tilePairBoxes[ 1 ], imgRandomAccess, new ARGBType( ARGBType.rgba( 96, 0, 96, 32 ) ) );

		final TileSearchRadiusEstimator searchRadiusEstimator = new TileSearchRadiusEstimator( tiles, 3, new int[] { 2, 2 } );
		final InvertibleRealTransform[] estimatedTileTransforms = new InvertibleRealTransform[] {
				StitchSubdividedTileBoxPair.estimateAffineTransformation( tiles[ tilePairIndexes[ 0 ] ], searchRadiusEstimator ),
				StitchSubdividedTileBoxPair.estimateAffineTransformation( tiles[ tilePairIndexes[ 1 ] ], searchRadiusEstimator )
			};

		final TilePair tilePair = new TilePair( tiles[ tilePairIndexes[ 0 ] ], tiles[ tilePairIndexes[ 1 ] ] );

		// draw transformed tiles
		drawTransformedRectangle(
				getLocalRealIntervalCorners( tilePair.getA() ),
				estimatedTileTransforms[ 0 ],
				displaySize,
				imgRandomAccess,
				new ARGBType( ARGBType.rgba( 64, 64, 255, 255 ) )
			);
		drawTransformedRectangle(
				getLocalRealIntervalCorners( tilePair.getB() ),
				estimatedTileTransforms[ 1 ],
				displaySize,
				imgRandomAccess,
				new ARGBType( ARGBType.rgba( 255, 255, 255, 64 ) )
			);

		final SubdividedTileBoxPair tileBoxPair = new SubdividedTileBoxPair(
				tilePairBoxes[ 0 ].get( tileBoxPairIndexes[ 0 ] ),
				tilePairBoxes[ 1 ].get( tileBoxPairIndexes[ 1 ] )
			);

		// draw transformed tile boxes
		drawTransformedRectangle(
				getRealIntervalCorners( tileBoxPair.getA() ),
				estimatedTileTransforms[ 0 ],
				displaySize,
				imgRandomAccess,
				new ARGBType( ARGBType.rgba( 64, 64, 255, 255 ) )
			);
		drawTransformedRectangle(
				getRealIntervalCorners( tileBoxPair.getB() ),
				estimatedTileTransforms[ 1 ],
				displaySize,
				imgRandomAccess,
				new ARGBType( ARGBType.rgba( 255, 255, 255, 255 ) )
			);

		final EstimatedTileBoxSearchRadius movingBoxWorldSearchRadius = StitchSubdividedTileBoxPair.getCombinedSearchRadiusForMovingBox(
				searchRadiusEstimator,
				tileBoxPair.toArray(),
				estimatedTileTransforms
			);

		drawErrorEllipse(
				movingBoxWorldSearchRadius.errorEllipse,
				new Translation2D( Intervals.minAsDoubleArray( SplitTileOperations.transformTileBox( tileBoxPair.getB() ) ) ),
				displaySize,
				imgRandomAccess,
				new ARGBType( ARGBType.rgba( 255, 255, 0, 255 ) )
			);

		new ImageJ();
		final ImagePlus imp = img.getImagePlus();
		imp.show();
	}

	private TileInfo[] getTiles()
	{
		final double[][] stagePositions = new double[][] {
				new double[] { 200, 100 },
				new double[] { 281, 105 },
				new double[] { 358, 102 },
				new double[] { 430, 108 },

				new double[] { 193, 240 },
				new double[] { 274, 231 },
				new double[] { 362, 237 },
				new double[] { 447, 241 },

				new double[] { 204, 378 },
				new double[] { 288, 382 },
				new double[] { 377, 389 },
				new double[] { 460, 385 },
		};

		final double[][] stitchedPositions = new double[][] {
			new double[] { 800, 100 },
			new double[] { 888, 108 },
			new double[] { 974, 116 },
			new double[] { 1060, 125 },

			new double[] { 802, 236 },
			new double[] { 890, 247 },
			new double[] { 980, 254 },
			new double[] { 1060, 260 },

			new double[] { 798, 380 },
			new double[] { 885, 388 },
			new double[] { 970, 400 },
			new double[] { 1065, 408 },
		};

		final List< TileInfo > tiles = new ArrayList<>();
		for ( int i = 0; i < stagePositions.length; ++i )
		{
			final TileInfo tile = new TileInfo( 2 );
			tile.setIndex( i );
			tile.setSize( tileSize );
			tile.setPosition( stagePositions[ i ] );
			tile.setTransform( new Translation2D( stitchedPositions[ i ] ) );
			tiles.add( tile );
		}

		return tiles.toArray( new TileInfo[ 0 ] );
	}

	private void drawStageTiles( final TileInfo[] tiles, final RandomAccess< ARGBType > imgRandomAccess, final ARGBType color )
	{
		for ( final TileInfo tile : tiles )
		{
			drawRectangle(
					getRealIntervalCorners( tile ),
					null,
					displaySize,
					imgRandomAccess,
					color
				);
		}
	}

	private void drawStitchedTiles( final TileInfo[] tiles, final RandomAccess< ARGBType > imgRandomAccess, final ARGBType color )
	{
		for ( final TileInfo tile : tiles )
		{
			drawTransformedRectangle(
					getLocalRealIntervalCorners( tile ),
					tile.getTransform(),
					displaySize,
					imgRandomAccess,
					color
				);
		}
	}

	private void drawTileBoxes( final List< SubdividedTileBox > tileBoxes, final RandomAccess< ARGBType > imgRandomAccess, final ARGBType color )
	{
		for ( final SubdividedTileBox tileBox : tileBoxes )
		{
			final AffineTransform2D tileTransform = new AffineTransform2D();
			for ( int d = 0; d < tileBox.getFullTile().numDimensions(); ++d )
				tileTransform.set( tileBox.getFullTile().getPosition( d ), d, 2 );

			drawRectangle(
					getRealIntervalCorners( tileBox ),
					tileTransform,
					displaySize,
					imgRandomAccess,
					color
				);
		}
	}

	private void drawRectangle(
			final double[][] rectCorners,
			final AffineGet displayTransform,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color )
	{
		final int dim = displaySize.length;
		final double[][] rectDisplayCorners = new double[ 2 ][ dim ];

		for ( int i = 0; i < 2; ++i )
		{
			rectDisplayCorners[ i ] = rectCorners[ i ].clone();
			if ( displayTransform != null )
				displayTransform.apply( rectDisplayCorners[ i ], rectDisplayCorners[ i ] );
		}

		drawTransformedRectangle( rectDisplayCorners, null, displaySize, imgRandomAccess, color );
	}

	private void drawTransformedRectangle(
			final double[][] rectCorners,
			final InvertibleRealTransform pointTransform,
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
							imgRandomAccess.get().add( color );
							visitedPixels.add( pixelIndex );
						}
					}
				}
			}
		}
	}

	private void drawErrorEllipse(
			final ErrorEllipse errorEllipse,
			final AffineGet displayTransform,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color )
	{
		final int dim = displaySize.length;
		final double[] pos = new double[ dim ];
		final int[] posDisplay = new int[ dim ];

		final Set< Integer > visitedPixels = new HashSet<>();

		final Interval errorEllipseBoundingBox = Intervals.smallestContainingInterval( errorEllipse.getBoundingBox() );
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

		final NeighborhoodsIterableInterval< BitType > neighborhoods = new DiamondShape( 1 ).neighborhoodsSafe( errorEllipseTestImg );
		final Cursor< Neighborhood< BitType > > neighborhoodsCursor = neighborhoods.localizingCursor();
		final RandomAccess< BitType > errorEllipseTestImgRandomAccess = errorEllipseTestImg.randomAccess();
		while ( neighborhoodsCursor.hasNext() )
		{
			final Neighborhood< BitType > neighborhood = neighborhoodsCursor.next();
			errorEllipseTestImgRandomAccess.setPosition( neighborhoodsCursor );
			if ( errorEllipseTestImgRandomAccess.get().get() )
			{
				boolean isInnerPoint = true;
				for ( final BitType pt : neighborhood )
					isInnerPoint &= pt.get();

				// it is a contour point if it is inside the ellipse but at least one of the neighbors is outside the ellipse
				if ( !isInnerPoint )
				{
					neighborhoodsCursor.localize( pos );

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
							imgRandomAccess.get().add( color );
							visitedPixels.add( pixelIndex );
						}
					}
				}
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
}
