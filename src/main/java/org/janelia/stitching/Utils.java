package org.janelia.stitching;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.util.Conversions;

import ij.ImagePlus;
import mpicbg.stitching.ImageCollectionElement;

/**
 * Provides some useful methods for working with file paths
 *
 * @author Igor Pisarev
 */

public class Utils {

	public static String addFilenameSuffix( final String filename, final String suffix )
	{
		int lastDotIndex = filename.lastIndexOf( "." );
		if ( lastDotIndex == -1 )
			lastDotIndex = filename.length();

		final StringBuilder ret = new StringBuilder( filename );
		ret.insert( lastDotIndex, suffix );
		return ret.toString();
	}

	public static String removeFilenameSuffix( final String filename, final String suffix )
	{
		final int suffixIndex = filename.lastIndexOf( suffix );
		if ( suffixIndex == -1 )
			return filename;

		int lastDotIndex = filename.lastIndexOf( "." );
		if ( lastDotIndex == -1 )
			lastDotIndex = filename.length();

		if ( suffixIndex + suffix.length() != lastDotIndex )
			return filename; // not a suffix

		final StringBuilder ret = new StringBuilder( filename );
		ret.delete( suffixIndex, lastDotIndex );
		return ret.toString();
	}

	public static String getAbsoluteImagePath( final StitchingJob job, final TileInfo tile )
	{
		String filePath = tile.getFilePath();
		if ( !Paths.get( filePath ).isAbsolute() )
			filePath = Paths.get( job.getBaseFolder(), filePath ).toString();
		return filePath;
	}

	public static ImageCollectionElement createElement( final StitchingJob job, final TileInfo tile ) throws Exception
	{
		final File file = new File( job == null ? tile.getFilePath() : getAbsoluteImagePath( job, tile) );
		final ImageCollectionElement e = new ImageCollectionElement( file, tile.getIndex() );
		e.setOffset( Conversions.toFloatArray( tile.getPosition() ) );
		e.setDimensionality( tile.numDimensions() );
		//e.setModel( TileModelFactory.createOffsetModel( tile ) );
		e.setModel( TileModelFactory.createDefaultModel( tile.numDimensions() ) );
		return e;
	}

	public static ImageCollectionElement createElement( final TileInfo tile ) throws Exception
	{
		return createElement( null, tile );
	}

	public static void workaroundImagePlusNSlices( final ImagePlus imp )
	{
		final int[] possible3rdDim = new int[] { imp.getNChannels(), imp.getNSlices(), imp.getNFrames() };
		Arrays.sort( possible3rdDim );
		if ( possible3rdDim[ 0 ] * possible3rdDim[ 1 ] == 1 )
			imp.setDimensions( 1, possible3rdDim[ 2 ], 1 );
	}

	public static Map< Integer, TileInfo > createTilesMap( final TileInfo[] tiles )
	{
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tilesMap.put( tile.getIndex(), tile );
		return tilesMap;
	}
	public static Map< Integer, TileInfo > createTilesMap( final List< SerializablePairWiseStitchingResult > shifts )
	{
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTilePair().toArray() )
				tilesMap.put( tile.getIndex(), tile );
		return tilesMap;
	}
}
