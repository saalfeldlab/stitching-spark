package org.janelia.stitching;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.janelia.util.Conversions;

import ij.IJ;
import ij.ImagePlus;
import mpicbg.stitching.ImageCollectionElement;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.NumericType;

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
		e.setModel( TileModelFactory.createOffsetModel( tile ) );
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

	public static < T extends NumericType< T > > void saveTileImageToFile( final TileInfo tile, final Img< T > img )
	{
		final ImagePlus imp = ImageJFunctions.wrap( img, "" );
		Utils.workaroundImagePlusNSlices( imp );
		tile.setType( ImageType.valueOf( imp.getType() ) );
		System.out.println( "Saving the resulting file for cell " + tile.getIndex() );
		IJ.saveAsTiff( imp, tile.getFilePath() );
		imp.close();
	}

	public static TreeMap< Integer, TileInfo > createTilesMap( final TileInfo[] tiles )
	{
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tilesMap.put( tile.getIndex(), tile );
		return tilesMap;
	}
	public static TreeMap< Integer, TileInfo > createTilesMap( final List< SerializablePairWiseStitchingResult > shifts, final boolean onlyValid )
	{
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
			for ( final TileInfo tile : shift.getTilePair().toArray() )
				if ( !onlyValid || shift.getIsValidOverlap() )
					tilesMap.put( tile.getIndex(), tile );
		return tilesMap;
	}
	public static TreeMap< Integer, TileInfo > createTilesMapMulti( final List< SerializablePairWiseStitchingResult[] > shiftsMulti, final boolean onlyValid )
	{
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMulti )
		{
			boolean isValidOverlap = true;
			if ( onlyValid )
				for ( final SerializablePairWiseStitchingResult shift: shiftMulti )
					if ( !shift.getIsValidOverlap() )
						isValidOverlap = false;

			if ( isValidOverlap )
				for ( final TileInfo tile : shiftMulti[ 0 ].getTilePair().toArray() )
					tilesMap.put( tile.getIndex(), tile );
		}
		return tilesMap;
	}
	public static TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult > > createPairwiseShiftsMap( final List< SerializablePairWiseStitchingResult > shifts, final boolean onlyValid )
	{
		final TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult > > shiftsMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			if ( !onlyValid || shift.getIsValidOverlap() )
			{
				final int ind1 = Math.min( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() );
				final int ind2 = Math.max( shift.getTilePair().first().getIndex(), shift.getTilePair().second().getIndex() );

				if ( !shiftsMap.containsKey( ind1 ) )
					shiftsMap.put( ind1, new TreeMap<>() );

				shiftsMap.get( ind1 ).put( ind2, shift );
			}
		}
		return shiftsMap;
	}
	public static TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult[] > > createPairwiseShiftsMultiMap( final List< SerializablePairWiseStitchingResult[] > shiftsMulti, final boolean onlyValid )
	{
		final TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult[] > > shiftsMultiMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult[] shiftMulti : shiftsMulti )
		{
			boolean isValidOverlap = true;
			if ( onlyValid )
				for ( final SerializablePairWiseStitchingResult shift: shiftMulti )
					if ( !shift.getIsValidOverlap() )
						isValidOverlap = false;

			if ( isValidOverlap )
			{
				final int ind1 = Math.min( shiftMulti[ 0 ].getTilePair().first().getIndex(), shiftMulti[ 0 ].getTilePair().second().getIndex() );
				final int ind2 = Math.max( shiftMulti[ 0 ].getTilePair().first().getIndex(), shiftMulti[ 0 ].getTilePair().second().getIndex() );

				if ( !shiftsMultiMap.containsKey( ind1 ) )
					shiftsMultiMap.put( ind1, new TreeMap<>() );

				shiftsMultiMap.get( ind1 ).put( ind2, shiftMulti );
			}
		}
		return shiftsMultiMap;
	}
}
