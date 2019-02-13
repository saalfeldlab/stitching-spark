package org.janelia.flatfield;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.dataaccess.PathResolver;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileLoader;
import org.janelia.stitching.Utils;

import ij.ImagePlus;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealConverter;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.imglib2.view.RandomAccessiblePairNullable;
import net.imglib2.view.Views;

public class ApplyFlatfieldCorrection
{
	public static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void main( final String[] args ) throws Exception
	{
		final String inputTileConfiguration = args[ 0 ];
		final DataProvider dataProvider = DataProviderFactory.create( DataProviderFactory.detectType( inputTileConfiguration ) );
		final TileInfo[] tiles = dataProvider.loadTiles( inputTileConfiguration );
		final RandomAccessiblePairNullable< U, U > flatfield = FlatfieldCorrection.loadCorrectionImages( dataProvider, inputTileConfiguration, tiles[ 0 ].numDimensions() );
		if ( flatfield == null )
			throw new NullPointerException( "flatfield images were not found" );

		final String outputDirectory = PathResolver.get( PathResolver.getParent( inputTileConfiguration ), PathResolver.getFileName( inputTileConfiguration ) + "_test-apply-flatfield" );
		int processed = 0;
		for ( final TileInfo tile : tiles )
		{
			final RandomAccessibleInterval< T > tileImg = TileLoader.loadTile( tile, dataProvider );
			final FlatfieldCorrectedRandomAccessible< T, U > flatfieldCorrectedTileImg = new FlatfieldCorrectedRandomAccessible<>( tileImg, flatfield.toRandomAccessiblePair() );
			final RandomAccessibleInterval< U > correctedImg = Views.interval( flatfieldCorrectedTileImg, tileImg );
			final RandomAccessibleInterval< T > convertedImg = Converters.convert( correctedImg, new RealConverter<>(), Util.getTypeFromInterval( tileImg ) );
			final ImagePlus correctedImp = Utils.copyToImagePlus( convertedImg );
			dataProvider.saveImage( correctedImp, PathResolver.get( outputDirectory, PathResolver.getFileName( tile.getFilePath() ) ) );

			System.out.println( "  processed " + (++processed) + " tiles out of " + tiles.length );
		}

		System.out.println( System.lineSeparator() + "Done" );
	}
}
