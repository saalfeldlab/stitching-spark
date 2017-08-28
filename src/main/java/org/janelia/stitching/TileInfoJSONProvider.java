package org.janelia.stitching;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

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
	public static boolean isTilesConfiguration( final String input )
	{
		try ( final FileReader reader = new FileReader( new File( input ) ) ) {
			final TileInfo[] tiles = createGson().fromJson( reader, TileInfo[].class );
			return ( tiles != null && tiles.length > 0 && !tiles[ 0 ].isNull() );
		} catch ( final IOException e ) {
			return false;
		}
	}

	public static TileInfo[] loadTilesConfiguration( final String input ) throws IOException
	{
		System.out.println( "Loading tiles configuration from " + input );
		try ( final FileReader reader = new FileReader( new File( input ) ) ) {
			return createGson().fromJson( reader, TileInfo[].class );
		}
	}

	public static void saveTilesConfiguration( final TileInfo[] tiles, String output ) throws IOException
	{
		if ( !output.endsWith( ".json" ) )
			output += ".json";

		System.out.println( "Saving updated tiles configuration to " + output );
		try ( final FileWriter writer = new FileWriter( output ) ) {
			writer.write( createGson().toJson( tiles ) );
		}
	}

	public static boolean isPairwiseConfiguration( final String input )
	{
		try ( final FileReader reader = new FileReader( new File( input ) ) ) {
			final List< SerializablePairWiseStitchingResult > pairwiseShifts = Arrays.asList( createGson().fromJson( reader, SerializablePairWiseStitchingResult[].class ) );
			return ( pairwiseShifts != null && !pairwiseShifts.isEmpty() && !pairwiseShifts.get( 0 ).isNull() );
		} catch ( final IOException e ) {
			return false;
		}
	}

	public static ArrayList< SerializablePairWiseStitchingResult > loadPairwiseShifts( final String input ) throws IOException
	{
		System.out.println( "Loading pairwise shifts from " + input );
		try ( final FileReader reader = new FileReader( new File( input ) ) ) {
			return new ArrayList<>( Arrays.asList( createGson().fromJson( reader, SerializablePairWiseStitchingResult[].class ) ) );
		}
	}

	public static void savePairwiseShifts( final List< SerializablePairWiseStitchingResult > shifts, String output ) throws IOException
	{
		if ( !output.endsWith( ".json" ) )
			output += ".json";

		System.out.println( "Saving pairwise shifts to " + output );
		try ( final FileWriter writer = new FileWriter( output ) ) {
			writer.write( createGson().toJson( shifts ) );
		}
	}

	public static ArrayList< SerializablePairWiseStitchingResult[] > loadPairwiseShiftsMulti( final String input ) throws IOException
	{
		System.out.println( "Loading pairwise shifts (multiple) from " + input );
		try ( final FileReader reader = new FileReader( new File( input ) ) )
		{
			return new ArrayList<>( Arrays.asList( createGson().fromJson( reader, SerializablePairWiseStitchingResult[][].class ) ) );
		}
	}

	public static void savePairwiseShiftsMulti( final List< SerializablePairWiseStitchingResult[] > shiftsMulti, String output ) throws IOException
	{
		if ( !output.endsWith( ".json" ) )
			output += ".json";

		System.out.println( "Saving pairwise shifts (multiple) to " + output );
		try ( final FileWriter writer = new FileWriter( output ) ) {
			writer.write( createGson().toJson( shiftsMulti ) );
		}
	}

	private static Gson createGson()
	{
		return new GsonBuilder()
				.excludeFieldsWithModifiers( Modifier.STATIC )
				.registerTypeAdapter( AffineTransform3D.class, new AffineTransform3DJsonAdapter() )
				.create();
	}

	private static class AffineTransform3DJsonAdapter implements JsonSerializer< AffineTransform3D >, JsonDeserializer< AffineTransform3D >
	{
		@Override
		public JsonElement serialize( final AffineTransform3D src, final Type typeOfSrc, final JsonSerializationContext context )
		{
			final JsonArray jsonMatrixArray = new JsonArray();
			for ( int row = 0; row < src.numDimensions(); ++row )
			{
				final JsonArray jsonRowArray = new JsonArray();
				for ( int col = 0; col < src.numDimensions() + 1; ++col )
					jsonRowArray.add( src.get( row, col ) );
				jsonMatrixArray.add( jsonRowArray );
			}
			return jsonMatrixArray;
		}

		@Override
		public AffineTransform3D deserialize( final JsonElement json, final Type typeOfT, final JsonDeserializationContext context ) throws JsonParseException
		{
			final AffineTransform3D affineTransform = new AffineTransform3D();
			final JsonArray jsonMatrixArray = json.getAsJsonArray();
			for ( int row = 0; row < jsonMatrixArray.size(); ++row )
			{
				final JsonArray jsonRowArray = jsonMatrixArray.get( row ).getAsJsonArray();
				for ( int col = 0; col < jsonRowArray.size(); ++col )
					affineTransform.set( jsonRowArray.get( col ).getAsDouble(), row, col );
			}
			return affineTransform;
		}
	}
}