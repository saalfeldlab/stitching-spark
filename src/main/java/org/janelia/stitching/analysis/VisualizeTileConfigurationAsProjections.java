package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.AxisMapping;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TransformUtils;
import org.janelia.stitching.TransformedTileOperations;

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
		public final ARGBType color;

		public TileForDrawing(
				final Integer index,
				final Dimensions size,
				final AffineGet transform,
				final ARGBType color )
		{
			this.index = index;
			this.size = size;
			this.transform = transform;
			this.color = color.copy();
		}
	}

	private static class TileForInspection
	{
		public final Integer inspectedTileIndex;
		public final ARGBType inspectedTileColor;
		public final Set< Integer > neighboringTileIndexes;
		public final ARGBType neighboringTilesColor;

		public TileForInspection(
				final Integer inspectedTileIndex,
				final ARGBType inspectedTileColor,
				final Set< Integer > neighboringTileIndexes,
				final ARGBType neighboringTilesColor )
		{
			this.inspectedTileIndex = inspectedTileIndex;
			this.inspectedTileColor = inspectedTileColor.copy();
			this.neighboringTileIndexes = neighboringTileIndexes;
			this.neighboringTilesColor = neighboringTilesColor.copy();
		}
	}

	private static final long[] displaySize = new long[] { 4000, 4000 };
	private static final boolean projectOnlyMiddleSlab = true;

	private static ARGBType stageTilesColor = new ARGBType( ARGBType.rgba( 255, 0, 0, 255 ) );
	private static ARGBType groundtruthTilesColor = new ARGBType( ARGBType.rgba( 0, 255, 0, 255 ) );
	private static ARGBType stitchedTilesColor = new ARGBType( ARGBType.rgba( 255, 255, 0, 255 ) );

	private static ARGBType inspectedTileColor = new ARGBType( ARGBType.rgba( 255, 255, 255, 255 ) );
	private static ARGBType inspectedTileNeighborsColor = new ARGBType( ARGBType.rgba( 0, 0, 255, 255 ) );

	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createByURI( URI.create( args[ 0 ] ) );
		final TileInfo[] groundtruthTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 0 ] ) ) );
		final TileInfo[] stitchedTiles = args.length > 1 ? TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ 1 ] ) ) ) : null;

		final Optional< TileForInspection > tileForInspection;
		final Optional< long[] > projectionGridCoords;

		if ( args.length > 2 )
		{
			final String usedPairwiseShiftsPath = args[ 2 ];
			final Integer tileToInspect = Integer.parseInt( args[ 3 ] );

			final List< SerializablePairWiseStitchingResult > usedPairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( usedPairwiseShiftsPath ) ) );
			final Set< Integer > inspectedTileNeighbors = new HashSet<>();
			for ( final SerializablePairWiseStitchingResult usedPairwiseShift : usedPairwiseShifts )
				for ( int i = 0; i < 2; ++i )
					if ( usedPairwiseShift.getSubTilePair().getFullTilePair().toArray()[ i ].getIndex().intValue() == tileToInspect.intValue() )
						inspectedTileNeighbors.add( usedPairwiseShift.getSubTilePair().getFullTilePair().toArray()[ ( i + 1 ) % 2 ].getIndex() );

			if ( groundtruthTiles[ 0 ].numDimensions() == 3 )
			{
				final long[] inspectedTileCoords = new long[ 3 ];
				for ( final TileInfo tile : groundtruthTiles )
				{
					if ( tile.getIndex().intValue() == tileToInspect.intValue() )
					{
						for ( int d = 0; d < tile.numDimensions(); ++d )
							inspectedTileCoords[ d ] = Math.round( tile.getStagePosition( d ) );
						break;
					}
				}
				projectionGridCoords = Optional.of( inspectedTileCoords );
				System.out.println( "Coordinates used for projection: " + Arrays.toString( inspectedTileCoords ) );
			}
			else
			{
				projectionGridCoords = Optional.empty();
			}

			tileForInspection = Optional.of( new TileForInspection(
					tileToInspect,
					inspectedTileColor,
					inspectedTileNeighbors,
					inspectedTileNeighborsColor
				) );
		}
		else
		{
			tileForInspection = Optional.empty();
			projectionGridCoords = Optional.empty();
		}

		new ImageJ();

		final Optional< List< Set< Integer > > > tileIndexesProjectionsWhitelist = Optional.ofNullable( getTileIndexesProjectionsWhitelist( groundtruthTiles, projectionGridCoords ) );

		final List< TileForDrawing > stageTilesForDrawing = new ArrayList<>(), groundtruthTilesForDrawing = new ArrayList<>();
		for ( final TileInfo tile : groundtruthTiles )
		{
			stageTilesForDrawing.add( new TileForDrawing( tile.getIndex(), new FinalDimensions( tile.getSize() ), new Translation( tile.getStagePosition() ), stageTilesColor ) );
			groundtruthTilesForDrawing.add( new TileForDrawing( tile.getIndex(), new FinalDimensions( tile.getSize() ), tile.getTransform(), groundtruthTilesColor ) );
		}

		drawTiles( stageTilesForDrawing, "stage", tileIndexesProjectionsWhitelist );
		drawTiles( groundtruthTilesForDrawing, "groundtruth", tileIndexesProjectionsWhitelist );

		if ( stitchedTiles != null )
		{
			final List< TileForDrawing > stitchedTilesForDrawing = new ArrayList<>();
			for ( final TileInfo tile : stitchedTiles )
				stitchedTilesForDrawing.add( new TileForDrawing( tile.getIndex(), new FinalDimensions( tile.getSize() ), tile.getTransform(), stitchedTilesColor ) );

			drawTiles(
					stitchedTilesForDrawing,
					"stitch",
					tileIndexesProjectionsWhitelist,
					tileForInspection
				);
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
				final Set< Integer > includedStitchedTiles = new LinkedHashSet<>();
				for ( final TileInfo stitchedTile : stitchedTiles )
					if ( tileIndexesProjectionsWhitelist.get().get( d ).contains( stitchedTile.getIndex() ) )
						includedStitchedTiles.add( stitchedTile.getIndex() );

				final List< Integer > projectionDims = new ArrayList<>();
				for ( int k = 0; k < 3; ++k )
					if ( k != d )
						projectionDims.add( k );
				final String projectionAxesStr = projectionDims.stream().map( k -> AxisMapping.getAxisStr( k ) ).collect( Collectors.joining() );
				System.out.println( "  " + projectionAxesStr + ": " + includedStitchedTiles.size() + "  " + includedStitchedTiles );
			}
		}

		if ( tileForInspection.isPresent() )
		{
			System.out.println();
			System.out.println( "Tile to inspect: " + tileForInspection.get().inspectedTileIndex );
			System.out.println( "Its neighbors used for optimization: " + tileForInspection.get().neighboringTileIndexes );
		}
	}

	private static List< Set< Integer > > getTileIndexesProjectionsWhitelist( final TileInfo[] tiles, final Optional< long[] > projectionGridCoords )
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

					if ( projectionGridCoords.isPresent() )
					{
						tileIndexesProjectionsWhitelist.add( positionsInProjectionDimensionToTiles.get( projectionGridCoords.get()[ d ] ) );
					}
					else
					{
						final long middlePositionInProjectionDimension = positionsInProjectionDimensionToTiles.keySet().stream().mapToLong( Long::longValue ).toArray()[ positionsInProjectionDimensionToTiles.size() / 2 ];
						final Set< Integer > tileIndexesAtMiddlePositionInProjectionDimension = positionsInProjectionDimensionToTiles.get( middlePositionInProjectionDimension );
						tileIndexesProjectionsWhitelist.add( tileIndexesAtMiddlePositionInProjectionDimension );
					}
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
			final String caption,
			final Optional< List< Set< Integer > > > tileIndexesProjectionsWhitelist ) throws ImgLibException
	{
		drawTiles( tilesForDrawing, caption, tileIndexesProjectionsWhitelist, Optional.empty() );
	}

	private static void drawTiles(
			final List< TileForDrawing > tilesForDrawing,
			final String caption,
			final Optional< List< Set< Integer > > > tileIndexesProjectionsWhitelist,
			final Optional< TileForInspection > tileForInspection ) throws ImgLibException
	{
		if ( tileForInspection.isPresent() )
		{
			for ( final TileForDrawing tileForDrawing : tilesForDrawing )
			{
				if ( tileForDrawing.index.intValue() == tileForInspection.get().inspectedTileIndex.intValue() )
					tileForDrawing.color.set( tileForInspection.get().inspectedTileColor );
				else if ( tileForInspection.get().neighboringTileIndexes.contains( tileForDrawing.index ) )
					tileForDrawing.color.set( tileForInspection.get().neighboringTilesColor );
			}
		}

		if ( tilesForDrawing.iterator().next().size.numDimensions() == 2 )
		{
			if ( tileIndexesProjectionsWhitelist.isPresent() )
				throw new RuntimeException( "whitelist is expected to be null for 2d, as all tiles should be included" );
			drawTilesProjection( tilesForDrawing, caption );
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

					projectedTilesForDrawing.add( new TileForDrawing( tileForDrawing.index, projectedTileDimensions, projectedTileTransform, tileForDrawing.color ) );
				}

				final String projectionAxesStr = projectionDims.stream().map( k -> AxisMapping.getAxisStr( k ) ).collect( Collectors.joining() );
				drawTilesProjection( projectedTilesForDrawing, caption + "-" + projectionAxesStr );
			}
		}
		else
		{
			throw new RuntimeException( "only 2d/3d tiles are supported" );
		}
	}

	private static void drawTilesProjection( final List< TileForDrawing > projectedTilesForDrawing, final String caption ) throws ImgLibException
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
					projectedTileForDrawing.color
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
