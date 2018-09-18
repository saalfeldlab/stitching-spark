package org.janelia.stitching;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.dataaccess.PathResolver;
import org.janelia.util.Conversions;

import ij.IJ;
import ij.ImagePlus;
import mpicbg.models.Model;
import mpicbg.stitching.ImageCollectionElement;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;

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

	public static ImageCollectionElement createElementCollectionElementModel( final TileInfo tile, final Model< ? > model )
	{
		final File file = new File( tile.getFilePath() );
		final ImageCollectionElement e = new ImageCollectionElement( file, tile.getIndex() );
		e.setOffset( Conversions.toFloatArray( tile.getStagePosition() ) );
		e.setDimensionality( tile.numDimensions() );
		e.setModel( model );
		return e;
	}

	public static < T extends NativeType< T > & RealType< T > > ImagePlus copyToImagePlus( final RandomAccessibleInterval< T > img ) throws ImgLibException
	{
		final ImagePlusImg< T, ? > imagePlusImg = new ImagePlusImgFactory< T >().create( Intervals.dimensionsAsLongArray( img ), Util.getTypeFromInterval( img ) );
		final Cursor< T > imgCursor = Views.flatIterable( img ).cursor();
		final Cursor< T > imagePlusImgCursor = Views.flatIterable( imagePlusImg ).cursor();
		while ( imagePlusImgCursor.hasNext() || imgCursor.hasNext() )
			imagePlusImgCursor.next().set( imgCursor.next() );
		final ImagePlus imp = imagePlusImg.getImagePlus();
		workaroundImagePlusNSlices( imp );
		return imp;
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
			for ( final TileInfo tile : shift.getSubTilePair().getFullTilePair().toArray() )
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
				for ( final TileInfo tile : shiftMulti[ 0 ].getSubTilePair().getFullTilePair().toArray() )
					tilesMap.put( tile.getIndex(), tile );
		}
		return tilesMap;
	}
	public static TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult > > createSubTilePairwiseResultsMap( final List< SerializablePairWiseStitchingResult > subTilePairwiseResults, final boolean onlyValid )
	{
		final TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult > > subTilePairwiseResultsMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult subTilePairwiseResult : subTilePairwiseResults )
		{
			if ( !onlyValid || subTilePairwiseResult.getIsValidOverlap() )
			{
				final SubTilePair subTilePair = subTilePairwiseResult.getSubTilePair();
				final int ind1 = Math.min( subTilePair.getA().getIndex(), subTilePair.getB().getIndex() );
				final int ind2 = Math.max( subTilePair.getA().getIndex(), subTilePair.getB().getIndex() );

				if ( !subTilePairwiseResultsMap.containsKey( ind1 ) )
					subTilePairwiseResultsMap.put( ind1, new TreeMap<>() );

				subTilePairwiseResultsMap.get( ind1 ).put( ind2, subTilePairwiseResult );
			}
		}
		return subTilePairwiseResultsMap;
	}
	public static TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult[] > > createSubTilePairwiseResultsMultiMap( final List< SerializablePairWiseStitchingResult[] > subTilePairwiseResultsMulti, final boolean onlyValid )
	{
		final TreeMap< Integer, TreeMap< Integer, SerializablePairWiseStitchingResult[] > > subTilePairwiseResultsMultiMap = new TreeMap<>();
		for ( final SerializablePairWiseStitchingResult[] subTilePairwiseResultMulti : subTilePairwiseResultsMulti )
		{
			boolean isValidOverlap = true;
			if ( onlyValid )
				for ( final SerializablePairWiseStitchingResult subTilePairwiseResult : subTilePairwiseResultMulti )
					if ( !subTilePairwiseResult.getIsValidOverlap() )
						isValidOverlap = false;

			if ( isValidOverlap )
			{
				final SubTilePair subTilePair = subTilePairwiseResultMulti[ 0 ].getSubTilePair();
				final int ind1 = Math.min( subTilePair.getA().getIndex(), subTilePair.getB().getIndex() );
				final int ind2 = Math.max( subTilePair.getA().getIndex(), subTilePair.getB().getIndex() );

				if ( !subTilePairwiseResultsMultiMap.containsKey( ind1 ) )
					subTilePairwiseResultsMultiMap.put( ind1, new TreeMap<>() );

				subTilePairwiseResultsMultiMap.get( ind1 ).put( ind2, subTilePairwiseResultMulti );
			}
		}
		return subTilePairwiseResultsMultiMap;
	}


	public static int[] getTileCoordinates( final TileInfo tile ) throws RuntimeException
	{
		return getTileCoordinates( PathResolver.getFileName( tile.getFilePath() ) );
	}
	public static int[] getTileCoordinates( final String filename ) throws RuntimeException
	{
		final String coordsPatternStr = ".*(\\d{3})x_(\\d{3})y_(\\d{3})z.*";
		final Pattern coordsPattern = Pattern.compile( coordsPatternStr );
		final Matcher matcher = coordsPattern.matcher( filename );
		if ( !matcher.find() )
			throw new RuntimeException( "Can't parse coordinates" );

		// don't forget to swap X and Y axes
		final int[] coordinates = new int[]
				{
					Integer.parseInt( matcher.group( 2 ) ),
					Integer.parseInt( matcher.group( 1 ) ),
					Integer.parseInt( matcher.group( 3 ) )
				};
		return coordinates;
	}
	public static String getTileCoordinatesString( final TileInfo tile ) throws RuntimeException
	{
		return getTileCoordinatesString( PathResolver.getFileName( tile.getFilePath() ) );
	}
	public static String getTileCoordinatesString( final String filename ) throws RuntimeException
	{
		final String coordsPatternStr = ".*(\\d{3}x_\\d{3}y_\\d{3}z).*";
		final Pattern coordsPattern = Pattern.compile( coordsPatternStr );
		final Matcher matcher = coordsPattern.matcher( filename );
		if ( !matcher.find() )
			throw new RuntimeException( "Can't parse coordinates" );
		return matcher.group( 1 );
	}
	public static List< Pair< TileInfo, int[] > > getTilesCoordinates( final TileInfo[] tiles ) throws RuntimeException
	{
		final List< Pair< TileInfo, int[] > > tileCoordinates = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			tileCoordinates.add( new ValuePair<>( tile, getTileCoordinates( tile ) ) );
		return tileCoordinates;
	}
	public static TreeMap< Integer, int[] > getTilesCoordinatesMap( final TileInfo[] tiles ) throws RuntimeException
	{
		final TreeMap< Integer, int[] > coordinatesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			coordinatesMap.put( tile.getIndex(), getTileCoordinates( tile ) );
		return coordinatesMap;
	}

	public static long getTileTimestamp( final TileInfo tile ) throws RuntimeException
	{
		return getTileTimestamp( PathResolver.getFileName( tile.getFilePath() ) );
	}
	public static long getTileTimestamp( final String filename ) throws RuntimeException
	{
		final String timePatternStr = ".*_(\\d*)msecAbs.*";
		final Pattern timePattern = Pattern.compile( timePatternStr );
		final Matcher matcher = timePattern.matcher( filename );
		if ( !matcher.find() )
			throw new RuntimeException( "Can't parse timestamp" );

		final long timestamp = Long.parseLong( matcher.group( 1 ) );
		return timestamp;
	}
	public static List< Pair< TileInfo, Long > > getTilesTimestamps( final TileInfo[] tiles ) throws RuntimeException
	{
		final List< Pair< TileInfo, Long > > tileTimestamps = new ArrayList<>();
		for ( final TileInfo tile : tiles )
			tileTimestamps.add( new ValuePair<>( tile, getTileTimestamp( tile ) ) );
		return tileTimestamps;
	}
	public static TreeMap< Integer, Long > getTilesTimestampsMap( final TileInfo[] tiles ) throws RuntimeException
	{
		final TreeMap< Integer, Long > timestampsMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			timestampsMap.put( tile.getIndex(), getTileTimestamp( tile ) );
		return timestampsMap;
	}

	public static List< TileInfo > sortTilesByTimestamp( final TileInfo[] tiles ) throws RuntimeException
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

	public static double[] normalizeVoxelDimensions( final double[] voxelDimensions )
	{
		final double[] normalizedVoxelDimensions = new double[ voxelDimensions.length ];
		double voxelDimensionsMinValue = Double.MAX_VALUE;
		for ( int d = 0; d < normalizedVoxelDimensions.length; d++ )
			voxelDimensionsMinValue = Math.min( voxelDimensions[ d ], voxelDimensionsMinValue );
		for ( int d = 0; d < normalizedVoxelDimensions.length; d++ )
			normalizedVoxelDimensions[ d ] = voxelDimensions[ d ] / voxelDimensionsMinValue;
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
}
