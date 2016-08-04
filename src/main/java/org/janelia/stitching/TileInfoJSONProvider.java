package org.janelia.stitching;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;

public class TileInfoJSONProvider
{
	public static TileInfo[] loadTilesConfiguration( final String input ) throws IOException
	{
		System.out.println( "Loading tiles configuration from " + input );
		try ( final FileReader reader = new FileReader( new File( input ) ) ) {
			return new Gson().fromJson( reader, TileInfo[].class );
		}
	}

	public static void saveTilesConfiguration( final TileInfo[] tiles, final String output ) throws IOException
	{
		System.out.println( "Saving updated tiles configuration to " + output );
		try ( final FileWriter writer = new FileWriter( output ) ) {
			writer.write( new Gson().toJson( tiles ) );
		}
	}

	public static List< SerializablePairWiseStitchingResult > loadPairwiseShifts( final String input ) throws IOException
	{
		System.out.println( "Loading pairwise shifts from " + input );
		try ( final FileReader reader = new FileReader( new File( input ) ) ) {
			return new ArrayList<>( Arrays.asList( new Gson().fromJson( reader, SerializablePairWiseStitchingResult[].class ) ) );
		}
	}

	public static void savePairwiseShifts( final List< SerializablePairWiseStitchingResult > shifts, final String output ) throws IOException
	{
		System.out.println( "Saving pairwise shifts to " + output );
		try ( final FileWriter writer = new FileWriter( output ) ) {
			writer.write( new Gson().toJson( shifts ) );
		}
	}
}