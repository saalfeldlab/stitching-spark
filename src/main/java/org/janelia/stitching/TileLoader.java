package org.janelia.stitching;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.util.ImageImporter;

import ij.ImagePlus;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

public class TileLoader
{
	public static enum TileStorageType
	{
		IMAGE_FILE,
		N5_DATASET
	}

	public static TileStorageType getTileType( final TileInfo tile, final DataProvider dataProvider )
	{
		final String n5Path  = PathResolver.getParent( PathResolver.getParent( tile.getFilePath() ) );
		final String tileDatasetPath = Paths.get( n5Path ).relativize( Paths.get( tile.getFilePath() ) ).toString();

		try
		{
			final N5Reader n5 = dataProvider.createN5Reader( URI.create( n5Path ) );
			if ( n5.datasetExists( tileDatasetPath ) )
				return TileStorageType.N5_DATASET;
		}
		catch ( final IOException e )
		{
		}
		return TileStorageType.IMAGE_FILE;
	}

	public static String getChannelN5DatasetPath( final TileInfo tile )
	{
		final String n5Path = PathResolver.getParent( PathResolver.getParent( tile.getFilePath() ) );
		final String tileDatasetPath = Paths.get( n5Path ).relativize( Paths.get( tile.getFilePath() ) ).toString();
		return tileDatasetPath;
	}

	public static DatasetAttributes getTileN5DatasetAttributes( final TileInfo tile, final DataProvider dataProvider ) throws IOException
	{
		if ( getTileType( tile, dataProvider ) != TileStorageType.N5_DATASET )
			throw new IllegalArgumentException( "Expected the given tile to be an N5 dataset" );

		final String n5Path  = PathResolver.getParent( PathResolver.getParent( tile.getFilePath() ) );
		final String tileDatasetPath = Paths.get( n5Path ).relativize( Paths.get( tile.getFilePath() ) ).toString();

		final N5Reader n5 = dataProvider.createN5Reader( URI.create( n5Path ) );
		return n5.getDatasetAttributes( tileDatasetPath );
	}

	public static < T extends NativeType< T > & RealType< T > > RandomAccessibleInterval< T > loadTile( final TileInfo tile, final DataProvider dataProvider )
	{
		// check if a given tile path is an N5 dataset
		final String n5Path  = PathResolver.getParent( PathResolver.getParent( tile.getFilePath() ) );
		final String tileDatasetPath = Paths.get( n5Path ).relativize( Paths.get( tile.getFilePath() ) ).toString();

		// try to load from N5, if it fails then it is not an N5 dataset, so try to open as an image file
		try
		{
			final N5Reader n5 = dataProvider.createN5Reader( URI.create( n5Path ) );
			if ( n5.datasetExists( tileDatasetPath ) )
				return N5Utils.open( n5, tileDatasetPath );
		}
		catch ( final IOException e )
		{
		}

		final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
		return ImagePlusImgs.from( imp );
	}

	/*public static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > RandomAccessibleInterval< T > loadTile(
			final TileInfo tile,
			final DataProvider dataProvider,
			final RandomAccessiblePairNullable< U, U > flatfield )
	{
		// check if a given tile path is an N5 dataset
		final String n5Path  = PathResolver.getParent( PathResolver.getParent( tile.getFilePath() ) );
		final String tileDatasetPath = Paths.get( n5Path ).relativize( Paths.get( tile.getFilePath() ) ).toString();

		// try to load from N5, if it fails then it is not an N5 dataset, so try to open as an image file
		final RandomAccessibleInterval< T > img;
		{
			RandomAccessibleInterval< T > inputImg = null;
			try
			{
				final N5Reader n5 = dataProvider.createN5Reader( URI.create( n5Path ) );
				if ( n5.datasetExists( tileDatasetPath ) )
					inputImg = N5Utils.open( n5, tileDatasetPath );
			}
			catch ( final IOException e )
			{
			}

			if ( inputImg == null )
			{
				final ImagePlus imp = ImageImporter.openImage( tile.getFilePath() );
				inputImg = ImagePlusImgs.from( imp );
			}

			img = inputImg;
		}

		// apply flatfield correction if needed
		final RandomAccessibleInterval< T > source;
		if ( flatfield != null )
		{
			final FlatfieldCorrectedRandomAccessible< T, U > flatfieldCorrectedImg = new FlatfieldCorrectedRandomAccessible<>( img, flatfield.toRandomAccessiblePair() );
			final RandomAccessible< T > correctedConvertedRandomAccessible = Converters.convert( flatfieldCorrectedImg, new RealConverter<>(), Util.getTypeFromInterval( img ) );
			source = Views.interval( correctedConvertedRandomAccessible, img );
		}
		else
		{
			source = img;
		}

		return source;
	}*/
}
