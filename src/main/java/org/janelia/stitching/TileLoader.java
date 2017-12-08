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
	public static enum TileType
	{
		IMAGE_FILE,
		N5_DATASET
	}

	public static TileType getTileType( final TileInfo tile, final DataProvider dataProvider )
	{
		final String n5Path  = PathResolver.getParent( PathResolver.getParent( tile.getFilePath() ) );
		final String tileDatasetPath = Paths.get( n5Path ).relativize( Paths.get( tile.getFilePath() ) ).toString();

		try
		{
			final N5Reader n5 = dataProvider.createN5Reader( URI.create( n5Path ) );
			if ( n5.datasetExists( tileDatasetPath ) )
				return TileType.N5_DATASET;
		}
		catch ( final IOException e )
		{
		}
		return TileType.IMAGE_FILE;
	}

	public static String getChannelN5DatasetPath( final TileInfo tile )
	{
		final String n5Path = PathResolver.getParent( PathResolver.getParent( tile.getFilePath() ) );
		final String tileDatasetPath = Paths.get( n5Path ).relativize( Paths.get( tile.getFilePath() ) ).toString();
		return tileDatasetPath;
	}

	public static DatasetAttributes getTileN5DatasetAttributes( final TileInfo tile, final DataProvider dataProvider ) throws IOException
	{
		if ( getTileType( tile, dataProvider ) != TileType.N5_DATASET )
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
}
