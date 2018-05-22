package org.janelia.stitching;

import java.lang.reflect.Type;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import net.imglib2.realtransform.AffineGet;

public class AffineGetJsonAdapter implements JsonSerializer< AffineGet >, JsonDeserializer< AffineGet >
{
	@Override
	public JsonElement serialize( final AffineGet src, final Type typeOfSrc, final JsonSerializationContext context )
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
	public AffineGet deserialize( final JsonElement json, final Type typeOfT, final JsonDeserializationContext context ) throws JsonParseException
	{
		final JsonArray jsonMatrixArray = json.getAsJsonArray();
		final double[][] affineMatrix = new double[ jsonMatrixArray.size() ][];
		for ( int row = 0; row < jsonMatrixArray.size(); ++row )
		{
			final JsonArray jsonRowArray = jsonMatrixArray.get( row ).getAsJsonArray();
			affineMatrix[ row ] = new double[ jsonRowArray.size() ];
			for ( int col = 0; col < jsonRowArray.size(); ++col )
				affineMatrix[ row ][ col ] = jsonRowArray.get( col ).getAsDouble();
		}
		return TransformUtils.createTransform( affineMatrix );
	}
}
