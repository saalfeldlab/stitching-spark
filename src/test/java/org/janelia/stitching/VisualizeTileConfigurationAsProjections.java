package org.janelia.stitching;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RealInterval;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.img.imageplus.IntImagePlus;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.RealTransformSequence;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;

public class VisualizeTileConfigurationAsProjections
{
	private static class TileForDrawing
	{
		public final Integer index;
		public final Dimensions size;
		public final AffineGet transform;

		public TileForDrawing( final Integer index, final Dimensions size, final AffineGet transform )
		{
			this.index = index;
			this.size = size;
			this.transform = transform;
		}
	}

	private static final long[] displaySize = new long[] { 4000, 4000 };
	private static final boolean projectOnlyMiddleSlab = true;

	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createByURI( URI.create( args[ 0 ] ) );
		final TileInfo[] groundtruthTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final TileInfo[] stitchedTiles = args.length > 1 ? TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) ) : null;

		new ImageJ();

		final Optional< List< Set< Integer > > > tileIndexesProjectionsWhitelist = Optional.ofNullable( getTileIndexesProjectionsWhitelist( groundtruthTiles ) );

		final List< TileForDrawing > stageTilesForDrawing = new ArrayList<>(), groundtruthTilesForDrawing = new ArrayList<>();
		for ( final TileInfo tile : groundtruthTiles )
		{
			stageTilesForDrawing.add( new TileForDrawing( tile.getIndex(), new FinalDimensions( tile.getSize() ), new Translation( tile.getStagePosition() ) ) );
			groundtruthTilesForDrawing.add( new TileForDrawing( tile.getIndex(), new FinalDimensions( tile.getSize() ), tile.getTransform() ) );
		}

		drawTiles( stageTilesForDrawing, new ARGBType( ARGBType.rgba( 255, 0, 0, 255 ) ), "stage", tileIndexesProjectionsWhitelist );
		drawTiles( groundtruthTilesForDrawing, new ARGBType( ARGBType.rgba( 0, 255, 0, 255 ) ), "groundtruth", tileIndexesProjectionsWhitelist );

		if ( stitchedTiles != null )
		{
			final List< TileForDrawing > stitchedTilesForDrawing = new ArrayList<>();
			for ( final TileInfo tile : stitchedTiles )
				stitchedTilesForDrawing.add( new TileForDrawing( tile.getIndex(), new FinalDimensions( tile.getSize() ), tile.getTransform() ) );
			drawTiles( stitchedTilesForDrawing, new ARGBType( ARGBType.rgba( 0, 0, 255, 255 ) ), "stitch", tileIndexesProjectionsWhitelist );
		}

		System.out.println( groundtruthTiles.length + " input/groundtruth tiles" );
		if ( stitchedTiles != null )
			System.out.println( stitchedTiles.length + " stitched tiles" );

		if ( tileIndexesProjectionsWhitelist.isPresent() )
		{
			System.out.println();
			System.out.println( "Number of input/groundtruth tiles in the projections:" );

			for ( int d = 0; d < 3; ++d )
			{
				final List< Integer > projectionDims = new ArrayList<>();
				for ( int k = 0; k < 3; ++k )
					if ( k != d )
						projectionDims.add( k );
				final String projectionAxesStr = projectionDims.stream().map( k -> AxisMapping.getAxisStr( k ) ).collect( Collectors.joining() );
				System.out.println( "  " + projectionAxesStr + ": " + tileIndexesProjectionsWhitelist.get().get( d ).size() );
			}

			System.out.println();
			System.out.println( "Number of stitched tiles in the projections:" );

			for ( int d = 0; d < 3; ++d )
			{
				int includedStitchedTiles = 0;
				for ( final TileInfo stitchedTile : stitchedTiles )
					if ( tileIndexesProjectionsWhitelist.get().get( d ).contains( stitchedTile.getIndex() ) )
						++includedStitchedTiles;

				final List< Integer > projectionDims = new ArrayList<>();
				for ( int k = 0; k < 3; ++k )
					if ( k != d )
						projectionDims.add( k );
				final String projectionAxesStr = projectionDims.stream().map( k -> AxisMapping.getAxisStr( k ) ).collect( Collectors.joining() );
				System.out.println( "  " + projectionAxesStr + ": " + includedStitchedTiles );
			}
		}
	}

	private static List< Set< Integer > > getTileIndexesProjectionsWhitelist( final TileInfo[] tiles )
	{
		final List< Set< Integer > > tileIndexesProjectionsWhitelist;
		if ( projectOnlyMiddleSlab )
		{
			if ( tiles[ 0 ].numDimensions() == 3 )
			{
				tileIndexesProjectionsWhitelist = new ArrayList<>();
				for ( int d = 0; d < 3; ++d )
				{
					final TreeMap< Long, Set< Integer > > positionsInProjectionDimensionToTiles = new TreeMap<>();
					for ( final TileInfo tile : tiles )
					{
						final long positionInProjectDimension = Math.round( tile.getStagePosition( d ) );
						if ( !positionsInProjectionDimensionToTiles.containsKey( positionInProjectDimension ) )
							positionsInProjectionDimensionToTiles.put( positionInProjectDimension, new HashSet<>() );
						positionsInProjectionDimensionToTiles.get( positionInProjectDimension ).add( tile.getIndex() );
					}
					final long middlePositionInProjectionDimension = positionsInProjectionDimensionToTiles.keySet().stream().mapToLong( Long::longValue ).toArray()[ positionsInProjectionDimensionToTiles.size() / 2 ];
					final Set< Integer > tileIndexesAtMiddlePositionInProjectionDimension = positionsInProjectionDimensionToTiles.get( middlePositionInProjectionDimension );
					tileIndexesProjectionsWhitelist.add( tileIndexesAtMiddlePositionInProjectionDimension );
				}
			}
			else if ( tiles[ 0 ].numDimensions() == 2 )
			{
				tileIndexesProjectionsWhitelist = null;
			}
			else
			{
				throw new RuntimeException( "only 2d/3d tiles are supported" );
			}
		}
		else
		{
			tileIndexesProjectionsWhitelist = null;
		}
		return tileIndexesProjectionsWhitelist;
	}

	private static void drawTiles(
			final List< TileForDrawing > tilesForDrawing,
			final ARGBType color,
			final String caption,
			final Optional< List< Set< Integer > > > tileIndexesProjectionsWhitelist ) throws ImgLibException
	{
		if ( tilesForDrawing.iterator().next().size.numDimensions() == 2 )
		{
			if ( tileIndexesProjectionsWhitelist.isPresent() )
				throw new RuntimeException( "whitelist is expected to be null for 2d, as all tiles should be included" );
			drawTilesProjection( tilesForDrawing, color, caption );
		}
		else if ( tilesForDrawing.iterator().next().size.numDimensions() == 3 )
		{
			for ( int d = 0; d < 3; ++d )
			{
				final List< Integer > projectionDims = new ArrayList<>();
				for ( int k = 0; k < 3; ++k )
					if ( k != d )
						projectionDims.add( k );

				final List< TileForDrawing > projectedTilesForDrawing = new ArrayList<>();
				for ( final TileForDrawing tileForDrawing: tilesForDrawing )
				{
					if ( tileIndexesProjectionsWhitelist.isPresent() && !tileIndexesProjectionsWhitelist.get().get( d ).contains( tileForDrawing.index ) )
						continue;

					final List< Long > projectedTileSize = new ArrayList<>();
					for ( final int dim : projectionDims )
						projectedTileSize.add( tileForDrawing.size.dimension( dim ) );
					final Dimensions projectedTileDimensions = new FinalDimensions( projectedTileSize.stream().mapToLong( Long::longValue ).toArray() );

					final List< List< Double > > projectedTileTransformMatrix = new ArrayList<>();
					for ( final int row : projectionDims )
					{
						final List< Double > matrixRow = new ArrayList<>();
						for ( final int col : projectionDims )
							matrixRow.add( tileForDrawing.transform.get( row, col ) );
						matrixRow.add( tileForDrawing.transform.get( row, tileForDrawing.transform.numDimensions() ) );
						projectedTileTransformMatrix.add( matrixRow );
					}
					final double[][] projectedTileTransformMatrixPrimitive = projectedTileTransformMatrix.stream().map( vals -> vals.stream().mapToDouble( Double::doubleValue ).toArray() ).toArray( double[][]::new );
					final AffineGet projectedTileTransform = TransformUtils.createTransform( projectedTileTransformMatrixPrimitive );

					projectedTilesForDrawing.add( new TileForDrawing( tileForDrawing.index, projectedTileDimensions, projectedTileTransform ) );
				}

				final String projectionAxesStr = projectionDims.stream().map( k -> AxisMapping.getAxisStr( k ) ).collect( Collectors.joining() );
				drawTilesProjection( projectedTilesForDrawing, color, caption + "-" + projectionAxesStr );
			}
		}
		else
		{
			throw new RuntimeException( "only 2d/3d tiles are supported" );
		}
	}

	private static void drawTilesProjection( final List< TileForDrawing > projectedTilesForDrawing, final ARGBType color, final String caption ) throws ImgLibException
	{
		final IntImagePlus< ARGBType > img = ImagePlusImgs.argbs( displaySize );
		final RandomAccess< ARGBType > imgRandomAccess = img.randomAccess();

		final double[] displayOffset = new double[ displaySize.length ];
		Arrays.fill( displayOffset, Double.MAX_VALUE );
		for ( final TileForDrawing projectedTileForDrawing : projectedTilesForDrawing )
		{
			final RealInterval transformedBoundingBox = TransformedTileOperations.getTransformedBoundingBox( new FinalInterval( projectedTileForDrawing.size ), projectedTileForDrawing.transform );
			for ( int d = 0; d < displayOffset.length; ++d )
				displayOffset[ d ] = Math.min( transformedBoundingBox.realMin( d ), displayOffset[ d ] );
		}
		for ( int d = 0; d < displayOffset.length; ++d )
			displayOffset[ d ] -= 10;

		for ( final TileForDrawing projectedTileForDrawing : projectedTilesForDrawing )
		{
			final RealTransformSequence projectedTileDisplayTransform = new RealTransformSequence();
			projectedTileDisplayTransform.add( projectedTileForDrawing.transform );
			projectedTileDisplayTransform.add( new Translation( displayOffset ).inverse() );

			drawTransformedRectangle(
					getLocalRealIntervalCorners( new FinalInterval( projectedTileForDrawing.size ) ),
					projectedTileDisplayTransform,
					Intervals.dimensionsAsIntArray( img ),
					imgRandomAccess,
					color
				);
		}

		final ImagePlus imp = img.getImagePlus();
		imp.setTitle( caption );
		imp.show();
	}

	private static Set< Integer > drawTransformedRectangle(
			final double[][] rectCorners,
			final RealTransform transform,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color )
	{
		final int dim = displaySize.length;
		final double[] position = new double[ dim ];

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
					position[ dMove ] = c;
					for ( int fixedDimIndex = 0; fixedDimIndex < fixedDims.size(); ++fixedDimIndex )
					{
						final int dFixed = fixedDims.get( fixedDimIndex );
						final int minOrMax = fixedDimsPossibilitiesIterator.getIntPosition( fixedDimIndex );
						position[ dFixed ] = rectCorners[ minOrMax ][ dFixed ];
					}
					transform.apply( position, position );
					drawPixel( position, displaySize, imgRandomAccess, color, Optional.of( visitedPixels ) );
				}
			}
		}

		// mark transformed top-left point
		transform.apply( rectCorners[ 0 ], position );
		for ( int dx = -3; dx <= 3; ++dx )
		{
			for ( int dy = -3; dy <= 3; ++dy )
			{
				final double[] positionWithStep = new double[] { position[ 0 ] + dx, position[ 1 ] + dy };
				drawPixel( positionWithStep, displaySize, imgRandomAccess, new ARGBType( ARGBType.rgba( 255, 255, 255, 255 ) ), Optional.empty() );
			}
		}

		return visitedPixels;
	}

	private static void drawPixel(
			final double[] position,
			final int[] displaySize,
			final RandomAccess< ARGBType > imgRandomAccess,
			final ARGBType color,
			final Optional< Set< Integer > > visitedPixels )
	{
		final int[] displayPosition = new int[ position.length ];
		for ( int d = 0; d < displayPosition.length; ++d )
			displayPosition[ d ] = ( int ) Math.round( position[ d ] );

		boolean insideView = true;
		for ( int d = 0; d < displayPosition.length; ++d )
			insideView &= ( displayPosition[ d ] >= 0 && displayPosition[ d ] < displaySize[ d ] );

		if ( insideView )
		{
			final int pixelIndex = IntervalIndexer.positionToIndex( displayPosition, displaySize );
			if ( !visitedPixels.isPresent() || !visitedPixels.get().contains( pixelIndex ) )
			{
				imgRandomAccess.setPosition( displayPosition );
				imgRandomAccess.get().set( color );

				if ( visitedPixels.isPresent() )
					visitedPixels.get().add( pixelIndex );
			}
		};
	}

	private static double[][] getLocalRealIntervalCorners( final RealInterval realInterval )
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
