package org.janelia.stitching;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;

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
			return new Gson().fromJson( closeableReader, TileInfo[].class );
		}
	}

	public static void saveTilesConfiguration( final Writer writer, final TileInfo[] tiles ) throws IOException
	{
		try ( final Writer closeableWriter = writer )
		{
			closeableWriter.write( new Gson().toJson( tiles ) );
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
			return new ArrayList<>( Arrays.asList( new Gson().fromJson( closeableReader, SerializablePairWiseStitchingResult[].class ) ) );
		}
	}

	public static void savePairwiseShifts( final Writer writer, final List< SerializablePairWiseStitchingResult > shifts ) throws IOException
	{
		try ( final Writer closeableWriter = writer )
		{
			closeableWriter.write( new Gson().toJson( shifts ) );
		}
	}

	public static ArrayList< SerializablePairWiseStitchingResult[] > loadPairwiseShiftsMulti( final Reader reader ) throws IOException
	{
		try ( final Reader closeableReader = reader )
		{
			return new ArrayList<>( Arrays.asList( new Gson().fromJson( closeableReader, SerializablePairWiseStitchingResult[][].class ) ) );
		}
	}

	public static void savePairwiseShiftsMulti( final Writer writer, final List< SerializablePairWiseStitchingResult[] > shiftsMulti ) throws IOException
	{
		try ( final Writer closeableWriter = writer )
		{
			closeableWriter.write( new Gson().toJson( shiftsMulti ) );
		}
	}
}