package org.janelia.stitching;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;

import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.util.Intervals;

public class PaintTileConfigurationSVG
{
	private final static String TILE_SVG_TEMPLATE_START = "<g";
	private final static String TILE_SVG_TEMPLATE_END = "</text>";

	private final static String BOUNDING_BOX_WIDTH_PLACEHOLDER = "{bounding_box_width}";
	private final static String BOUNDING_BOX_HEIGHT_PLACEHOLDER = "{bounding_box_height}";

	private final static String TILE_ID_PLACEHOLDER = "{tile_id}";
	private final static String TILE_WIDTH_PLACEHOLDER = "{tile_width}";
	private final static String TILE_HEIGHT_PLACEHOLDER = "{tile_height}";
	private final static String TILE_X_PLACEHOLDER = "{tile_x}";
	private final static String TILE_Y_PLACEHOLDER = "{tile_y}";
	private final static String TILE_CENTER_X_PLACEHOLDER = "{tile_center_x}";
	private final static String TILE_CENTER_Y_PLACEHOLDER = "{tile_center_y}";
	private final static String TILE_TEXT_FONTSIZE_PLACEHOLDER = "{tile_text_fontsize}";
	private final static String TILE_FILL_PLACEHOLDER = "{tile_fill}";

	private final static String TILE_FILL_INCLUDED = "#008000";
	private final static String TILE_FILL_EXCLUDED = "#800000";

	private final static long TILE_TEXT_FONTSIZE = 80;

	private final static String[] DIMENSION_STR = new String[] { "x", "y", "z" };

	public static void main( final String[] args ) throws Exception
	{
		final String tilesConfigStagePath = args[ 0 ];
		final String tilesConfigStitchedPath = args[ 1 ];
		final String svgTemplatePath = args[ 2 ];

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final TileInfo[] stageTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( tilesConfigStagePath ) ) );
		final Map< Integer, TileInfo > stitchedTilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( tilesConfigStitchedPath ) ) ) );

		for ( int sliceDim = 0; sliceDim < DIMENSION_STR.length; ++sliceDim )
		{
			final List< Integer > sliceDimsIndexes = new ArrayList<>();
			for ( int d = 0; d < DIMENSION_STR.length; ++d )
				if ( d != sliceDim )
					sliceDimsIndexes.add( d );

			final String svgTemplate = new String( Files.readAllBytes( Paths.get( svgTemplatePath ) ) );
			final int tileSvgTemplateStartIndex = svgTemplate.indexOf( TILE_SVG_TEMPLATE_START );
			final int tileSvgTemplateEndIndex = svgTemplate.lastIndexOf( TILE_SVG_TEMPLATE_END ) + TILE_SVG_TEMPLATE_END.length();
			final String tileSvgTemplate = svgTemplate.substring( tileSvgTemplateStartIndex, tileSvgTemplateEndIndex );
			final String svgHeaderTemplate = svgTemplate.substring( 0, tileSvgTemplateStartIndex );
			final String svgFooter = svgTemplate.substring( tileSvgTemplateEndIndex );


			// ------ Stage configuration ------
			TileOperations.translateTilesToOriginReal( stageTiles );
			final Interval stageBoundingBox = TileOperations.getCollectionBoundaries( stageTiles );

			final long stageMiddlePoint = stageBoundingBox.min( sliceDim ) + stageBoundingBox.dimension( sliceDim ) / 2 + 50;
			final long[] stageMiddleSliceIntervalMin = Intervals.minAsLongArray( stageBoundingBox ).clone();
			final long[] stageMiddleSliceIntervalMax = Intervals.maxAsLongArray( stageBoundingBox ).clone();
			stageMiddleSliceIntervalMin[ sliceDim ] = stageMiddlePoint;
			stageMiddleSliceIntervalMax[ sliceDim ] = stageMiddlePoint;
			final Interval stageMiddleSliceInterval = new FinalInterval( stageMiddleSliceIntervalMin, stageMiddleSliceIntervalMax );

			final List< TileInfo > stageMiddleSliceTiles = new ArrayList<>();
			for ( final TileInfo tile : stageTiles )
			{
				final Interval tileInterval = tile.getBoundaries();
				if ( TileOperations.overlap( tileInterval, stageMiddleSliceInterval ) )
					stageMiddleSliceTiles.add( tile );
			}

			{
				final Dimensions stageOutputSliceDimensions = new FinalDimensions(
						stageBoundingBox.dimension( sliceDimsIndexes.get( 0 ) ),
						stageBoundingBox.dimension( sliceDimsIndexes.get( 1 ) )
					);

				final String stageSvgHeader = getSvgHeader( stageOutputSliceDimensions, svgHeaderTemplate );
				final StringBuilder stageSvg = new StringBuilder( stageSvgHeader );

				for ( final TileInfo stageTile : stageMiddleSliceTiles )
				{
					final String stageTileSvg = getTileSvg(
							stageTile,
							stitchedTilesMap.containsKey( stageTile.getIndex() ),
							tileSvgTemplate,
							sliceDimsIndexes
						);
					stageSvg.append( stageTileSvg );
				}
				stageSvg.append( svgFooter );

				Files.write(
						Paths.get( Paths.get( svgTemplatePath ).getParent().toString(), "stage-" + DIMENSION_STR[ sliceDim ] + ".svg" ),
						stageSvg.toString().getBytes()
					);
			}



			// ------ Stitched configuration ------
			TileOperations.translateTilesToOriginReal( stitchedTilesMap.values().toArray( new TileInfo[ 0 ] ) );
			final Interval stitchedBoundingBox = TileOperations.getCollectionBoundaries( stitchedTilesMap.values().toArray( new TileInfo[ 0 ] ) );

			{
				final Dimensions stitchedOutputSliceDimensions = new FinalDimensions(
						stitchedBoundingBox.dimension( sliceDimsIndexes.get( 0 ) ),
						stitchedBoundingBox.dimension( sliceDimsIndexes.get( 1 ) )
					);

				final String stitchedSvgHeader = getSvgHeader( stitchedOutputSliceDimensions, svgHeaderTemplate );
				final StringBuilder stitchedSvg = new StringBuilder( stitchedSvgHeader );

				for ( final TileInfo stageTile : stageMiddleSliceTiles )
				{
					final TileInfo stitchedTile = stitchedTilesMap.get( stageTile.getIndex() );
					if ( stitchedTile != null )
					{
						final String stitchedTileSvg = getTileSvg(
								stitchedTile,
								true,
								tileSvgTemplate,
								sliceDimsIndexes
							);
						stitchedSvg.append( stitchedTileSvg );
					}
				}
				stitchedSvg.append( svgFooter );

				Files.write(
						Paths.get( Paths.get( svgTemplatePath ).getParent().toString(), "stitched-" + DIMENSION_STR[ sliceDim ] + ".svg" ),
						stitchedSvg.toString().getBytes()
					);
			}
		}

		System.out.println( "Done" );
	}

	private static String getSvgHeader( final Dimensions outputDimensions, final String svgHeaderTemplate )
	{
		return svgHeaderTemplate
				.replace( BOUNDING_BOX_WIDTH_PLACEHOLDER, new Long( outputDimensions.dimension( 0 ) ).toString() )
				.replace( BOUNDING_BOX_HEIGHT_PLACEHOLDER, new Long( outputDimensions.dimension( 1 ) ).toString() );
	}

	private static String getTileSvg(
			final TileInfo tile,
			final boolean included,
			final String tileSvgTemplate,
			final List< Integer > sliceDimsIndexes )
	{
		final Interval tileBox = tile.getBoundaries();
		return tileSvgTemplate
				.replace( TILE_ID_PLACEHOLDER, tile.getIndex().toString() )
				.replace( TILE_WIDTH_PLACEHOLDER, new Long( tileBox.dimension( sliceDimsIndexes.get( 0 ) ) ).toString() )
				.replace( TILE_HEIGHT_PLACEHOLDER, new Long( tileBox.dimension( sliceDimsIndexes.get( 1 ) ) ).toString() )
				.replace( TILE_X_PLACEHOLDER, new Long( tileBox.min( sliceDimsIndexes.get( 0 ) ) ).toString() )
				.replace( TILE_Y_PLACEHOLDER, new Long( tileBox.min( sliceDimsIndexes.get( 1 ) ) ).toString() )
				.replace( TILE_CENTER_X_PLACEHOLDER, new Long( tileBox.min( sliceDimsIndexes.get( 0 ) ) + tileBox.dimension( sliceDimsIndexes.get( 0 ) ) / 2 ).toString() )
				.replace( TILE_CENTER_Y_PLACEHOLDER, new Long( tileBox.min( sliceDimsIndexes.get( 1 ) ) + tileBox.dimension( sliceDimsIndexes.get( 1 ) ) / 2 + TILE_TEXT_FONTSIZE / 2 ).toString() )
				.replace( TILE_TEXT_FONTSIZE_PLACEHOLDER, new Long( TILE_TEXT_FONTSIZE ).toString() )
				.replace( TILE_FILL_PLACEHOLDER, included ? TILE_FILL_INCLUDED : TILE_FILL_EXCLUDED );
	}
}
