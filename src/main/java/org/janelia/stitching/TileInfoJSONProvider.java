package org.janelia.stitching;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.janelia.saalfeldlab.n5.bdv.AffineTransform3DJsonAdapter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.imglib2.realtransform.AffineTransform3D;

/**
 * Provides convenience methods for loading tiles configuration and storing it on a disk in JSON format.
 *
 * Supports two different types of data:
 * 1. A set of {@link TileInfo} objects which form a tile configuration.
 * 2. A set of {@link SerializablePairWiseStitchingResult} objects that represent pairwise similarity and best possible shift between two tiles.
 *
 * @author Igor Pisarev
 */

public class TileInfoJSONProvider
{
	public static boolean isTilesConfiguration( final Reader reader ) throws IOException
	{
		final TileInfo[] tiles = loadTilesConfiguration( reader );
		return ( tiles != null && tiles.length > 0 && !tiles[ 0 ].isNull() );
	}

	public static TileInfo[] loadTilesConfiguration( final Reader reader ) throws IOException
	{
		try ( final Reader closeableReader = reader )
		{
			return createGson().fromJson( closeableReader, TileInfo[].class );
		}
	}

	public static void saveTilesConfiguration( final TileInfo[] tiles, final Writer writer ) throws IOException
	{
		try ( final Writer closeableWriter = writer )
		{
			closeableWriter.write( createGson().toJson( tiles ) );
		}
	}

	public static boolean isPairwiseConfiguration( final Reader reader ) throws IOException
	{
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = loadPairwiseShifts( reader );
		return ( pairwiseShifts != null && !pairwiseShifts.isEmpty() && !pairwiseShifts.get( 0 ).isNull() );
	}

	public static ArrayList< SerializablePairWiseStitchingResult > loadPairwiseShifts( final Reader reader ) throws IOException
	{
		try ( final Reader closeableReader = reader )
		{
			return new ArrayList<>( Arrays.asList( createGson().fromJson( closeableReader, SerializablePairWiseStitchingResult[].class ) ) );
		}
	}

	public static void savePairwiseShifts( final List< SerializablePairWiseStitchingResult > shifts, final Writer writer ) throws IOException
	{
		try ( final Writer closeableWriter = writer )
		{
			closeableWriter.write( createGson().toJson( shifts ) );
		}
	}

	public static ArrayList< SerializablePairWiseStitchingResult[] > loadPairwiseShiftsMulti( final Reader reader ) throws IOException
	{
		try ( final Reader closeableReader = reader )
		{
			return new ArrayList<>( Arrays.asList( createGson().fromJson( closeableReader, SerializablePairWiseStitchingResult[][].class ) ) );
		}
	}

	public static void savePairwiseShiftsMulti( final List< SerializablePairWiseStitchingResult[] > shiftsMulti, final Writer writer ) throws IOException
	{
		try ( final Writer closeableWriter = writer )
		{
			closeableWriter.write( createGson().toJson( shiftsMulti ) );
		}
	}

	private static Gson createGson()
	{
		return new GsonBuilder()
				.registerTypeAdapter( AffineTransform3D.class, new AffineTransform3DJsonAdapter() )
				.create();
	}
}
