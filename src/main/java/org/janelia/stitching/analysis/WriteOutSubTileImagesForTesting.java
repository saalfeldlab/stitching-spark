package org.janelia.stitching.analysis;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.flatfield.FlatfieldCorrection;
import org.janelia.stitching.StitchSubTilePair;
import org.janelia.stitching.SubTile;
import org.janelia.stitching.SubTileOperations;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import ij.ImagePlus;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.RandomAccessiblePairNullable;

public class WriteOutSubTileImagesForTesting
{
	private static final double[] defaultPixelResolution = new double[] { 0.097, 0.097, 0.18 };
	private static final double blurSigma = 2.0;

	public static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void main( final String[] args ) throws Exception
	{
		final String[] channelsPaths = new String[ args.length - 2 ];
		for ( int c = 0; c < channelsPaths.length; ++c )
			channelsPaths[ c ] = args[ c ];

		final String[] tilePairStr = args[ args.length - 2 ].split( "," ), subTilePairStr = args[ args.length - 1 ].split( "," );
		final int[] tilePairIndices = new int[ 2 ], subTilePairIndices = new int[ 2 ];
		for ( int i = 0; i < 2; ++i ) {
			tilePairIndices[ i ] = Integer.parseInt( tilePairStr[ i ] );
			subTilePairIndices[ i ] = Integer.parseInt( subTilePairStr[ i ] );
		}
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final TileInfo[][] tileChannels = new TileInfo[ channelsPaths.length ][];
		for ( int c = 0; c < channelsPaths.length; ++c )
			tileChannels[ c ] = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( channelsPaths[ c ] ) ) );

		for ( int c = 1; c < tileChannels.length; ++c )
			if ( tileChannels[ c ].length != tileChannels[ 0 ].length )
				throw new RuntimeException();

		final int[] subdivisionGrid = new int[ tileChannels[ 0 ][ 0 ].numDimensions() ];
		Arrays.fill( subdivisionGrid, 2 );
		final List< SubTile > subTiles = SubTileOperations.subdivideTiles( tileChannels[ 0 ], subdivisionGrid );

		final SubTile[] subTilePair = new SubTile[ 2 ];
		for ( final SubTile subTile : subTiles ) {
			for ( int i = 0; i < 2; ++i ) {
				if ( subTile.getIndex().intValue() == subTilePairIndices[ i ] )
					subTilePair[ i ] = subTile;
			}
		}

		for ( int i = 0; i < 2; ++i ) {
			if ( subTilePair[ i ].getFullTile().getIndex().intValue() != tilePairIndices[ i ] )
				throw new RuntimeException();
		}

		// set default pixel resolution if needed
		for ( int c = 0; c < channelsPaths.length; ++c )
			for ( final TileInfo tile : tileChannels[ c ] )
				if ( tile.getPixelResolution() == null )
					tile.setPixelResolution( defaultPixelResolution.clone() );

		final List< RandomAccessiblePairNullable< U, U > > flatfieldsForChannels = new ArrayList<>();
		for ( int c = 0; c < channelsPaths.length; ++c )
		{
			flatfieldsForChannels.add( FlatfieldCorrection.loadFlatfields(
					dataProvider,
					channelsPaths[ c ].substring( 0, channelsPaths[ c ].lastIndexOf( "." ) ),
					tileChannels[ 0 ][ 0 ].numDimensions()
				) );
		}

		final String basePath = PathResolver.getParent( channelsPaths[ 0 ] );

		for ( int i = 0; i < 2; ++i )
		{
			System.out.println("Rendering subtile " + subTilePair[ i ].getIndex() );

			final List< TileInfo > channelTilesForRendering = new ArrayList<>();
			for ( int c = 0; c < tileChannels.length; ++c )
				channelTilesForRendering.add( Utils.createTilesMap( tileChannels[ c ] ).get( tilePairIndices[ i ] ) );

			final RandomAccessibleInterval/*< T >*/ renderedSubTile = StitchSubTilePair.renderSubTile(
					dataProvider,
					subTilePair[ i ],
					new Translation( subTilePair[ i ].numDimensions() ),
					channelTilesForRendering,
					Optional.empty(),
					blurSigma
				);
			final ImagePlus subTileImp = Utils.copyToImagePlus( renderedSubTile );
			final String subTileImpPath = PathResolver.get( basePath, "tile-" + subTilePair[ i ].getFullTile().getIndex() + "_subtile-" + subTilePair[ i ].getIndex() );
			dataProvider.saveImage( subTileImp, URI.create( subTileImpPath ) );
		}

		System.out.println( System.lineSeparator() + "Done, saved to " + basePath );
	}
}
