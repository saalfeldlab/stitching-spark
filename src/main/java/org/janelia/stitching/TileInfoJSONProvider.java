package org.janelia.stitching;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
	public static TileInfo[] loadTilesConfiguration( final String input ) throws IOException
	{
		System.out.println( "Loading tiles configuration from " + input );
		try ( final FileReader reader = new FileReader( new File( input ) ) ) {
			return new Gson().fromJson( reader, TileInfo[].class );
		}
	}

	public static void saveTilesConfiguration( final TileInfo[] tiles, String output ) throws IOException
	{
		if ( !output.endsWith( ".json" ) )
			output += ".json";

		System.out.println( "Saving updated tiles configuration to " + output );
		try ( final FileWriter writer = new FileWriter( output ) ) {
			writer.write( new Gson().toJson( tiles ) );
		}
	}

	public static ArrayList< SerializablePairWiseStitchingResult > loadPairwiseShifts( final String input ) throws IOException
	{
		System.out.println( "Loading pairwise shifts from " + input );
		try ( final FileReader reader = new FileReader( new File( input ) ) ) {
			return new ArrayList<>( Arrays.asList( new Gson().fromJson( reader, SerializablePairWiseStitchingResult[].class ) ) );
		}
	}

	public static void savePairwiseShifts( final List< SerializablePairWiseStitchingResult > shifts, String output ) throws IOException
	{
		if ( !output.endsWith( ".json" ) )
			output += ".json";

		System.out.println( "Saving pairwise shifts to " + output );
		try ( final FileWriter writer = new FileWriter( output ) ) {
			writer.write( new Gson().toJson( shifts ) );
		}
	}

	public static ArrayList< SerializablePairWiseStitchingResult[] > loadPairwiseShiftsMulti( final String input ) throws IOException
	{
		System.out.println( "Loading pairwise shifts (multiple) from " + input );
		try ( final FileReader reader = new FileReader( new File( input ) ) )
		{
			return new ArrayList<>( Arrays.asList( new Gson().fromJson( reader, SerializablePairWiseStitchingResult[][].class ) ) );
		}
	}

	public static void savePairwiseShiftsMulti( final List< SerializablePairWiseStitchingResult[] > shiftsMulti, String output ) throws IOException
	{
		if ( !output.endsWith( ".json" ) )
			output += ".json";

		System.out.println( "Saving pairwise shifts (multiple) to " + output );
		try ( final FileWriter writer = new FileWriter( output ) ) {
			writer.write( new Gson().toJson( shiftsMulti ) );
		}
	}

	public static void saveMultiscaledExportMetadata( final MultiscaledExportMetadata export, String output ) throws IOException
	{
		if ( !output.endsWith( ".json" ) )
			output += ".json";

		System.out.println( "Saving multiscaled export description to " + output );
		try ( final FileWriter writer = new FileWriter( output ) ) {
			writer.write( new GsonBuilder().setPrettyPrinting().create().toJson( export ) );
		}
	}

}