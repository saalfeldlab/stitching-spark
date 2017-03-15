package org.janelia.stitching;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.util.Conversions;

import ij.IJ;
import ij.ImagePlus;
import mpicbg.spim.data.sequence.VoxelDimensions;
import mpicbg.stitching.ImageCollectionElement;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

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
	public static int[] getImagePlusDimensions( final ImagePlus imp )
	{
		final int[] dim3 = new int[] { imp.getNChannels(), imp.getNSlices(), imp.getNFrames() };
		Arrays.sort( dim3 );
		if ( dim3[ dim3.length - 1 ] == 1 )
			return new int[] { imp.getWidth(), imp.getHeight() };
		return new int[] { imp.getWidth(), imp.getHeight(), dim3[ dim3.length - 1 ] };
	}

	public static ImageType getImageType( final List< TileInfo > tiles )
	{
		ImageType imageType = null;
		for ( final TileInfo tile : tiles )
		{
			if ( imageType == null )
				imageType = tile.getType();
			else if ( imageType != tile.getType() )
				return null;
		}
		return imageType;
	}

	public static < T extends NumericType< T > > void saveTileImageToFile( final TileInfo tile, final Img< T > img ) throws ImgLibException
	{
		final ImagePlus imp = ( img instanceof ImagePlusImg ? ((ImagePlusImg)img).getImagePlus() : ImageJFunctions.wrap( img, "" ) );
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
				final int ind1 = Math.min( shift.getTilePair().getA().getIndex(), shift.getTilePair().getB().getIndex() );
				final int ind2 = Math.max( shift.getTilePair().getA().getIndex(), shift.getTilePair().getB().getIndex() );

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
				final int ind1 = Math.min( shiftMulti[ 0 ].getTilePair().getA().getIndex(), shiftMulti[ 0 ].getTilePair().getB().getIndex() );
				final int ind2 = Math.max( shiftMulti[ 0 ].getTilePair().getA().getIndex(), shiftMulti[ 0 ].getTilePair().getB().getIndex() );

				if ( !shiftsMultiMap.containsKey( ind1 ) )
					shiftsMultiMap.put( ind1, new TreeMap<>() );

				shiftsMultiMap.get( ind1 ).put( ind2, shiftMulti );
			}
		}
		return shiftsMultiMap;
	}


	public static int[] getTileCoordinates( final TileInfo tile ) throws Exception
	{
		return getTileCoordinates( Paths.get( tile.getFilePath() ).getFileName().toString() );
	}
	public static int[] getTileCoordinates( final String filename ) throws Exception
	{
		final String coordsPatternStr = ".*(\\d{3})x_(\\d{3})y_(\\d{3})z.*";
		final Pattern coordsPattern = Pattern.compile( coordsPatternStr );
		final Matcher matcher = coordsPattern.matcher( filename );
		if ( !matcher.find() )
			throw new Exception( "Can't parse coordinates" );

		// don't forget to swap X and Y axes
		final int[] coordinates = new int[]
				{
					Integer.parseInt( matcher.group( 2 ) ),
					Integer.parseInt( matcher.group( 1 ) ),
					Integer.parseInt( matcher.group( 3 ) )
				};
		return coordinates;
	}
	public static String getTileCoordinatesString( final TileInfo tile ) throws Exception
	{
		return getTileCoordinatesString( Paths.get( tile.getFilePath() ).getFileName().toString() );
	}
	public static String getTileCoordinatesString( final String filename ) throws Exception
	{
		final String coordsPatternStr = ".*(\\d{3}x_\\d{3}y_\\d{3}z).*";
		final Pattern coordsPattern = Pattern.compile( coordsPatternStr );
		final Matcher matcher = coordsPattern.matcher( filename );
		if ( !matcher.find() )
			throw new Exception( "Can't parse coordinates" );
		return matcher.group( 1 );
	}
	public static List< Pair< TileInfo, int[] > > getTilesCoordinates( final TileInfo[] tiles ) throws Exception
	{
		final List< Pair< TileInfo, int[] > > tileCoordinates = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			tileCoordinates.add( new ValuePair<>( tile, getTileCoordinates( tile ) ) );
		return tileCoordinates;
	}
	public static TreeMap< Integer, int[] > getTilesCoordinatesMap( final TileInfo[] tiles ) throws Exception
	{
		final TreeMap< Integer, int[] > coordinatesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			coordinatesMap.put( tile.getIndex(), getTileCoordinates( tile ) );
		return coordinatesMap;
	}

	public static long getTileTimestamp( final TileInfo tile ) throws Exception
	{
		return getTileTimestamp( Paths.get( tile.getFilePath() ).getFileName().toString() );
	}
	public static long getTileTimestamp( final String filename ) throws Exception
	{
		final String timePatternStr = ".*_(\\d*)msecAbs.*";
		final Pattern timePattern = Pattern.compile( timePatternStr );
		final Matcher matcher = timePattern.matcher( filename );
		if ( !matcher.find() )
			throw new Exception( "Can't parse timestamp" );

		final long timestamp = Long.parseLong( matcher.group( 1 ) );
		return timestamp;
	}
	public static List< Pair< TileInfo, Long > > getTilesTimestamps( final TileInfo[] tiles ) throws Exception
	{
		final List< Pair< TileInfo, Long > > tileTimestamps = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			tileTimestamps.add( new ValuePair<>( tile, getTileTimestamp( tile ) ) );
		return tileTimestamps;
	}
	public static TreeMap< Integer, Long > getTilesTimestampsMap( final TileInfo[] tiles ) throws Exception
	{
		final TreeMap< Integer, Long > timestampsMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			timestampsMap.put( tile.getIndex(), getTileTimestamp( tile ) );
		return timestampsMap;
	}

	public static List< TileInfo > sortTilesByTimestamp( final TileInfo[] tiles ) throws Exception
	{
		final List< Pair< TileInfo, Long > > tileTimestamps = getTilesTimestamps( tiles );

		final TreeMap< Long, List< TileInfo > > timestampToTiles = new TreeMap<>();
		for ( final Pair< TileInfo, Long > tileTimestamp : tileTimestamps )
		{
			if ( !timestampToTiles.containsKey( tileTimestamp.getB() ) )
				timestampToTiles.put( tileTimestamp.getB(), new ArrayList<>() );
			timestampToTiles.get( tileTimestamp.getB() ).add( tileTimestamp.getA() );
		}

		final List< TileInfo > tilesSortedByTimestamp = new ArrayList<>();
		for ( final List< TileInfo > timestampTiles : timestampToTiles.values() )
			tilesSortedByTimestamp.addAll( timestampTiles );

		return tilesSortedByTimestamp;
	}


	public static double[] normalizeVoxelDimensions( final VoxelDimensions voxelDimensions )
	{
		final double[] normalizedVoxelDimensions = new double[ voxelDimensions.numDimensions() ];
		double voxelDimensionsMinValue = Double.MAX_VALUE;
		for ( int d = 0; d < normalizedVoxelDimensions.length; d++ )
			voxelDimensionsMinValue = Math.min( voxelDimensions.dimension( d ), voxelDimensionsMinValue );
		for ( int d = 0; d < normalizedVoxelDimensions.length; d++ )
			normalizedVoxelDimensions[ d ] = voxelDimensions.dimension( d ) / voxelDimensionsMinValue;
		return normalizedVoxelDimensions;
	}


	// Implementation of Scala's grouped(N)
	public static < T > Iterable< List< T > > grouped( final Iterator< T > iter, final int groupSize )
	{
		final List< List< T > > groups = new ArrayList<>();
		List< T > group = new ArrayList<>();
		while ( iter.hasNext() )
		{
			group.add( iter.next() );
			if ( group.size() == groupSize )
			{
				groups.add( group );
				group = new ArrayList<>();
			}
		}
		if ( !group.isEmpty() )
			groups.add( group );
		return groups;
	}


	public static long[] concatArrays( final long[]... arrays )
	{
		int totalLength = 0;
		for ( int i = 0; i < arrays.length; i++ )
			totalLength += arrays[ i ].length;

		final long[] ret = new long[ totalLength ];
		int usedLength = 0;
		for ( int i = 0; i < arrays.length; i++ )
		{
			System.arraycopy( arrays[ i ], 0, ret, usedLength, arrays[ i ].length );
			usedLength += arrays[ i ].length;
		}

		return ret;
	}
}
