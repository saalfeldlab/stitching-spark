package org.janelia.stitching.analysis;

import java.awt.Color;
import java.awt.Graphics;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.AxisMapping;
import org.janelia.stitching.CheckSubTileMatchesCoplanarity;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.SubTile;
import org.janelia.stitching.SubTileOperations;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.TransformUtils;
import org.janelia.stitching.TransformedTileOperations;
import org.janelia.stitching.Utils;
import org.janelia.util.ComparableTuple;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RealInterval;
import net.imglib2.display.screenimage.awt.ARGBScreenImage;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.realtransform.InvertibleRealTransformSequence;
import net.imglib2.realtransform.RealTransform;
import net.imglib2.realtransform.Scale2D;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;

public class VisualizeTileConfigurationAsProjections
{
	private static class VisualizeTileConfigurationAsProjectionsCmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-g", aliases = { "--groundtruthTilesPath" }, required = false,
				usage = "Path to the grountruth tile configuration file.")
		public String groundtruthTilesPath;

		@Option(name = "-s", aliases = { "--stitchedTilesPath" }, required = false,
				usage = "Path to the stitched tile configuration file.")
		public String stitchedTilesPath;

		@Option(name = "-p", aliases = { "--usedPairwiseConfigurationPath" }, required = false,
				usage = "Path to the used pairwise configuration file (generated after optimization).")
		public String usedPairwiseConfigurationPath;

		@Option(name = "-t", aliases = { "--tileToInspect" }, required = false,
				usage = "Tile to inspect.")
		public Integer tileToInspect;

		public boolean parsedSuccessfully = false;

		public VisualizeTileConfigurationAsProjectionsCmdArgs( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}

			if ( groundtruthTilesPath == null && stitchedTilesPath == null )
				throw new IllegalArgumentException( "At least one tile configuration (groundtruth or stitched) has to be provided" );

			if ( usedPairwiseConfigurationPath != null && stitchedTilesPath == null )
				throw new IllegalArgumentException( "Pairwise file is provided, expected stitched tile configuration to be provided as well" );
		}
	}

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

		public TileForInspection(
				final Integer inspectedTileIndex,
				final ARGBType inspectedTileColor )
		{
			this.inspectedTileIndex = inspectedTileIndex;
			this.inspectedTileColor = inspectedTileColor.copy();
		}
	}

	private static final int[] displaySize = new int[] { 4000, 4000 };
	private static final double displayScale = 0.25;
	private static final double displayPadding = 10;
	private static final boolean projectOnlyMiddleSlab = true;

	private static ARGBType stageTilesColor = new ARGBType( ARGBType.rgba( 255, 0, 0, 255 ) );
	private static ARGBType groundtruthTilesColor = new ARGBType( ARGBType.rgba( 0, 255, 0, 255 ) );
	private static ARGBType stitchedTilesColor = new ARGBType( ARGBType.rgba( 255, 255, 0, 255 ) );

	private static ARGBType inspectedTileColor = new ARGBType( ARGBType.rgba( 255, 255, 255, 255 ) );
	private static ARGBType pairwiseConnectionsColor = new ARGBType( ARGBType.rgba( 128, 255, 128, 255 ) );

	public static void main( final String[] args ) throws Exception
	{
		final VisualizeTileConfigurationAsProjectionsCmdArgs parsedArgs = new VisualizeTileConfigurationAsProjectionsCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final TileInfo[] groundtruthTiles = parsedArgs.groundtruthTilesPath != null ? TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( parsedArgs.groundtruthTilesPath ) ) ) : null;
		final TileInfo[] stitchedTiles = parsedArgs.stitchedTilesPath != null ? TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( parsedArgs.stitchedTilesPath ) ) ) : null;

		final int numDimensions = groundtruthTiles != null ? groundtruthTiles[ 0 ].numDimensions() : stitchedTiles[ 0 ].numDimensions();

		final Optional< TileForInspection > tileForInspection;
		final Optional< long[] > projectionGridCoords;
		if ( parsedArgs.tileToInspect != null )
		{
			if ( numDimensions == 3 )
			{
				final long[] inspectedTileCoords = new long[ 3 ];
				for ( final TileInfo tile : ( groundtruthTiles != null ? groundtruthTiles : stitchedTiles ) )
				{
					if ( tile.getIndex().intValue() == parsedArgs.tileToInspect.intValue() )
					{
						for ( int d = 0; d < tile.numDimensions(); ++d )
							inspectedTileCoords[ d ] = getTileGridPositionInDimension( tile, d );
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
			tileForInspection = Optional.of( new TileForInspection( parsedArgs.tileToInspect, inspectedTileColor ) );
		}
		else
		{
			tileForInspection = Optional.empty();
			projectionGridCoords = Optional.empty();
		}

		final Optional< List< SerializablePairWiseStitchingResult > > pairwiseConnections;
		if ( parsedArgs.usedPairwiseConfigurationPath != null )
		{
			pairwiseConnections = Optional.of( TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( parsedArgs.usedPairwiseConfigurationPath ) ) ) );
		}
		else
		{
			pairwiseConnections = Optional.empty();
		}

		final Optional< List< Set< Integer > > > tileIndexesProjectionsWhitelist = Optional.ofNullable( getTileIndexesProjectionsWhitelist(
				groundtruthTiles != null ? groundtruthTiles : stitchedTiles,
				projectionGridCoords
			) );

		new ImageJ();

//		final List< TileForDrawing > stageTilesForDrawing = new ArrayList<>();
//		for ( final TileInfo tile : ( groundtruthTiles != null ? groundtruthTiles : stitchedTiles ) )
//			stageTilesForDrawing.add( new TileForDrawing( tile.getIndex(), new FinalDimensions( tile.getSize() ), new Translation( tile.getStagePosition() ), stageTilesColor ) );
//		drawTiles( stageTilesForDrawing, "stage", tileIndexesProjectionsWhitelist );

		if ( groundtruthTiles != null )
		{
			final List< TileForDrawing > groundtruthTilesForDrawing = new ArrayList<>();
			for ( final TileInfo tile : groundtruthTiles )
				groundtruthTilesForDrawing.add( new TileForDrawing( tile.getIndex(), new FinalDimensions( tile.getSize() ), TransformedTileOperations.getTileTransform( tile, false ), groundtruthTilesColor ) );
			drawTiles( groundtruthTilesForDrawing, "groundtruth", tileIndexesProjectionsWhitelist );
		}

		if ( stitchedTiles != null )
		{
			final List< TileForDrawing > stitchedTilesForDrawing = new ArrayList<>();
			for ( final TileInfo tile : stitchedTiles )
				stitchedTilesForDrawing.add( new TileForDrawing( tile.getIndex(), new FinalDimensions( tile.getSize() ), tile.getTransform(), stitchedTilesColor ) );

			drawTiles(
					stitchedTilesForDrawing,
					"stitch",
					tileIndexesProjectionsWhitelist,
					tileForInspection,
					pairwiseConnections
				);
		}

		if ( groundtruthTiles != null )
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
			if ( pairwiseConnections.isPresent() )
			{
				final Map< Integer, Integer > tileToInspectOptimizationNeighbors = new TreeMap<>();
				for ( final SerializablePairWiseStitchingResult pairwiseConnection : pairwiseConnections.get() )
				{
					final SubTile[] subTiles = pairwiseConnection.getSubTilePair().toArray();
					for ( int i = 0; i < 2; ++i )
					{
						if ( subTiles[ i ].getFullTile().getIndex().intValue() == parsedArgs.tileToInspect.intValue() )
						{
							final int neighboringTile = subTiles[ ( i + 1 ) % 2 ].getFullTile().getIndex();
							tileToInspectOptimizationNeighbors.put( neighboringTile, tileToInspectOptimizationNeighbors.getOrDefault( neighboringTile, 0 ) + 1 );
						}
					}
				}
				System.out.println( "Its neighbors used for optimization (tile index -> count): " + tileToInspectOptimizationNeighbors );
			}
		}

		System.out.println();
		if ( pairwiseConnections.isPresent() )
			printCollinearAndCoplanarConfigurations( pairwiseConnections.get() );
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
						final long positionInProjectDimension = getTileGridPositionInDimension( tile, d );
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

	private static long getTileGridPositionInDimension( final TileInfo tile, final int d )
	{
		try
		{
			final int[] gridCoordinates = Utils.getTileCoordinates( tile );
			System.out.println( "  use grid coordinates for tile " + tile.getIndex() );
			return gridCoordinates[ d ];
		}
		catch ( final Exception e )
		{
			System.out.println( "  use rounded stage position for tile " + tile.getIndex() );
			return Math.round( tile.getStagePosition( d ) );
		}
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
			final Optional< List< SerializablePairWiseStitchingResult > > pairwiseConnections ) throws ImgLibException
	{
		drawTiles( tilesForDrawing, caption, tileIndexesProjectionsWhitelist, Optional.empty(), pairwiseConnections );
	}

	private static void drawTiles(
			final List< TileForDrawing > tilesForDrawing,
			final String caption,
			final Optional< List< Set< Integer > > > tileIndexesProjectionsWhitelist,
			final Optional< TileForInspection > tileForInspection,
			final Optional< List< SerializablePairWiseStitchingResult > > pairwiseConnections ) throws ImgLibException
	{
		if ( tileForInspection.isPresent() )
		{
			for ( final TileForDrawing tileForDrawing : tilesForDrawing )
			{
				if ( tileForDrawing.index.intValue() == tileForInspection.get().inspectedTileIndex.intValue() )
					tileForDrawing.color.set( tileForInspection.get().inspectedTileColor );
			}
		}

		if ( tilesForDrawing.iterator().next().size.numDimensions() == 2 )
		{
			if ( tileIndexesProjectionsWhitelist.isPresent() )
				throw new RuntimeException( "whitelist is expected to be null for 2d, as all tiles should be included" );
			drawTilesProjection( new int[] { 0, 1 }, tilesForDrawing, caption, pairwiseConnections, tileForInspection );
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
				drawTilesProjection(
						projectionDims.stream().mapToInt( Integer::intValue ).toArray(),
						projectedTilesForDrawing,
						caption + "-" + projectionAxesStr,
						pairwiseConnections,
						tileForInspection
					);
			}
		}
		else
		{
			throw new RuntimeException( "only 2d/3d tiles are supported" );
		}
	}

	private static void drawTilesProjection(
			final int[] projectionDimensions,
			final List< TileForDrawing > projectedTilesForDrawing,
			final String caption,
			final Optional< List< SerializablePairWiseStitchingResult > > pairwiseConnections,
			final Optional< TileForInspection > tileForInspection ) throws ImgLibException
	{
		final ARGBScreenImage img = new ARGBScreenImage( displaySize[ 0 ], displaySize[ 1 ] );
		final RandomAccess< ARGBType > imgRandomAccess = img.randomAccess();

		final double[] displayOffset = new double[ displaySize.length ];
		Arrays.fill( displayOffset, Double.MAX_VALUE );
		for ( final TileForDrawing projectedTileForDrawing : projectedTilesForDrawing )
		{
			final RealInterval transformedBoundingBox = TransformedTileOperations.getTransformedBoundingBox(
					new FinalInterval( projectedTileForDrawing.size ),
					scaleTransform( projectedTileForDrawing.transform )
				);
			for ( int d = 0; d < displayOffset.length; ++d )
				displayOffset[ d ] = Math.min( transformedBoundingBox.realMin( d ), displayOffset[ d ] );
		}
		for ( int d = 0; d < displayOffset.length; ++d )
			displayOffset[ d ] -= displayPadding;

		for ( final TileForDrawing projectedTileForDrawing : projectedTilesForDrawing )
		{
			final InvertibleRealTransformSequence projectedTileDisplayTransform = scaleTransform( projectedTileForDrawing.transform );
			projectedTileDisplayTransform.add( new Translation( displayOffset ).inverse() );

			drawTransformedRectangle(
					getLocalRealIntervalCorners( new FinalInterval( projectedTileForDrawing.size ) ),
					projectedTileDisplayTransform,
					Intervals.dimensionsAsIntArray( img ),
					imgRandomAccess,
					projectedTileForDrawing.color
				);
		}

		if ( pairwiseConnections.isPresent() )
		{
			drawPairwiseConnections(
					projectionDimensions,
					img,
					projectedTilesForDrawing,
					pairwiseConnections.get(),
					displayOffset,
					tileForInspection
				);
		}

		final ImagePlus imp = ImageJFunctions.wrap( img, caption );
		imp.show();
	}

	private static void drawPairwiseConnections(
			final int[] projectionDimensions,
			final ARGBScreenImage screenImage,
			final List< TileForDrawing > projectedTilesForDrawing,
			final List< SerializablePairWiseStitchingResult > pairwiseConnections,
			final double[] displayOffset,
			final Optional< TileForInspection > tileForInspection )
	{
		final Graphics graphics = screenImage.image().createGraphics();

		final Map< Integer, TileForDrawing > tilesForDrawingMap = new TreeMap<>();
		for ( final TileForDrawing tileForDrawing : projectedTilesForDrawing )
			tilesForDrawingMap.put( tileForDrawing.index, tileForDrawing );

		for ( final SerializablePairWiseStitchingResult pairwiseConnection : pairwiseConnections )
		{
			final TilePair tilePair = pairwiseConnection.getSubTilePair().getFullTilePair();
			if ( tilesForDrawingMap.containsKey( tilePair.getA().getIndex() ) && tilesForDrawingMap.containsKey( tilePair.getB().getIndex() ) )
			{
				final int[][] subTileMiddlePointDisplayPos = new int[ 2 ][];
				final SubTile[] subTiles = pairwiseConnection.getSubTilePair().toArray();
				for ( int i = 0; i < 2; ++i )
				{
					final InvertibleRealTransformSequence projectedTileDisplayTransform = scaleTransform( tilesForDrawingMap.get( subTiles[ i ].getFullTile().getIndex() ).transform );
					projectedTileDisplayTransform.add( new Translation( displayOffset ).inverse() );

					final double[] subTileMiddlePointPos = SubTileOperations.getSubTileMiddlePoint( subTiles[ i ] );
					final double[] subTileProjectionMiddlePointPos = new double[ projectionDimensions.length ];
					for ( int d = 0; d < subTileProjectionMiddlePointPos.length; ++d )
						subTileProjectionMiddlePointPos[ d ] = subTileMiddlePointPos[ projectionDimensions[ d ] ];

					final double[] transformedDisplayPos = new double[ subTileProjectionMiddlePointPos.length ];
					projectedTileDisplayTransform.apply( subTileProjectionMiddlePointPos, transformedDisplayPos );

					subTileMiddlePointDisplayPos[ i ] = new int[ transformedDisplayPos.length ];
					for ( int d = 0; d < transformedDisplayPos.length; ++d )
						subTileMiddlePointDisplayPos[ i ][ d ] = ( int ) Math.round( transformedDisplayPos[ d ] );
				}

				if ( isInsideView( subTileMiddlePointDisplayPos[ 0 ], displaySize ) && isInsideView( subTileMiddlePointDisplayPos[ 1 ], displaySize ) )
				{
					if ( tileForInspection.isPresent() && ( tilePair.getA().getIndex().intValue() == tileForInspection.get().inspectedTileIndex.intValue() || tilePair.getB().getIndex().intValue() == tileForInspection.get().inspectedTileIndex.intValue() ) )
						graphics.setColor( argbToColor( inspectedTileColor ) );
					else
						graphics.setColor( argbToColor( pairwiseConnectionsColor ) );

					graphics.drawLine(
							subTileMiddlePointDisplayPos[ 0 ][ 0 ], subTileMiddlePointDisplayPos[ 0 ][ 1 ],
							subTileMiddlePointDisplayPos[ 1 ][ 0 ], subTileMiddlePointDisplayPos[ 1 ][ 1 ]
						);

					graphics.fillRect( subTileMiddlePointDisplayPos[ 0 ][ 0 ] - 2, subTileMiddlePointDisplayPos[ 0 ][ 1 ] - 2, 5, 5 );
					graphics.fillRect( subTileMiddlePointDisplayPos[ 1 ][ 0 ] - 2, subTileMiddlePointDisplayPos[ 1 ][ 1 ] - 2, 5, 5 );
				}
			}
		}
	}

	private static InvertibleRealTransformSequence scaleTransform( final InvertibleRealTransform transform )
	{
		final InvertibleRealTransformSequence scaledTransform = new InvertibleRealTransformSequence();
		scaledTransform.add( transform );
		scaledTransform.add( new Scale2D( displayScale, displayScale ) );
		return scaledTransform;
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

		if ( isInsideView( displayPosition, displaySize ) )
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

	private static boolean isInsideView( final int[] displayPosition, final int[] displaySize )
	{
		boolean insideView = true;
		for ( int d = 0; d < displayPosition.length; ++d )
			insideView &= ( displayPosition[ d ] >= 0 && displayPosition[ d ] < displaySize[ d ] );
		return insideView;
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

	private static Color argbToColor( final ARGBType argb )
	{
		return new Color(
				ARGBType.red( argb.get() ),
				ARGBType.green( argb.get() ),
				ARGBType.blue( argb.get() ),
				ARGBType.alpha( argb.get() )
			);
	}

	private static void printCollinearAndCoplanarConfigurations( final List< SerializablePairWiseStitchingResult > pairwiseConnections )
	{
		final Map< Integer, List< SubTile > > tileIndexToItsSubTiles = new HashMap<>();
		for ( final SerializablePairWiseStitchingResult match : pairwiseConnections )
		{
			for ( final SubTile subTile : match.getSubTilePair().toArray() )
			{
				final int tileIndex = subTile.getFullTile().getIndex();
				if ( !tileIndexToItsSubTiles.containsKey( tileIndex ) )
					tileIndexToItsSubTiles.put( tileIndex, new ArrayList<>() );
				tileIndexToItsSubTiles.get( tileIndex ).add( subTile );
			}
		}

		final Set< Integer > collinearConfigurationTileIndexes = new TreeSet<>(), coplanarConfigurationTileIndexes = new TreeSet<>();

		for ( final Entry< Integer, List< SubTile > > entry : tileIndexToItsSubTiles.entrySet() )
		{
			final TreeMap< ComparableTuple< Long >, Integer > subTilePositionGroups = CheckSubTileMatchesCoplanarity.groupSubTilesByTheirLocalPosition( entry.getValue() );
			if ( CheckSubTileMatchesCoplanarity.isCollinear( subTilePositionGroups ) )
				collinearConfigurationTileIndexes.add( entry.getKey() );
			else if ( CheckSubTileMatchesCoplanarity.isCoplanar( subTilePositionGroups ) )
				coplanarConfigurationTileIndexes.add( entry.getKey() );
		}

		if ( !collinearConfigurationTileIndexes.isEmpty() )
			System.out.println( "Tiles with collinear config: " + collinearConfigurationTileIndexes );

		if ( !coplanarConfigurationTileIndexes.isEmpty() )
			System.out.println( "Tiles with coplanar config: " + coplanarConfigurationTileIndexes );
	}
}
