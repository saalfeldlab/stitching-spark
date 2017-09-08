package org.janelia.stitching;

import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.Translation;
import net.imglib2.util.Intervals;

public class SlabTransformsToTileTransforms
{
	private static class SlabTransform
	{
		public final List< Integer > slabs;
		public final AffineTransform3D transform;
		public final String stitchedTileConfiguration;
		public final double[] translation;

		public SlabTransform( final List< Integer > slabs, final AffineTransform3D transform, final String stitchedTileConfiguration, final double[] translation )
		{
			this.slabs = slabs;
			this.transform = transform;
			this.stitchedTileConfiguration = stitchedTileConfiguration;
			this.translation = translation;
		}
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

	private static Gson createGson()
	{
		return new GsonBuilder()
				.excludeFieldsWithModifiers( Modifier.STATIC )
				.registerTypeAdapter( AffineTransform3D.class, new AffineTransform3DJsonAdapter() )
				.create();
	}

	public static void main( final String[] args ) throws Exception
	{
		final List< SlabTransform > slabTransforms = new ArrayList<>();
		try ( final FileReader reader = new FileReader( new File( args[ 0 ] ) ) )
		{
			slabTransforms.addAll( Arrays.asList( createGson().fromJson( reader, SlabTransform[].class ) ) );
		}
		if ( slabTransforms.isEmpty() )
			throw new Exception( "no transforms found" );

		// slab -> transform
		final Map< Integer, AffineTransform3D > slabTransformsMap = new TreeMap<>();
		for ( final SlabTransform slabTransform : slabTransforms )
		{
			if ( slabTransform.transform == null )
				throw new Exception( "transform is null" );
			for ( final Integer slab : slabTransform.slabs )
				slabTransformsMap.put( slab, slabTransform.transform );
		}

		// slab -> stitched tiles in this slab
		final Map< Integer, TileInfo[] > slabTilesMap = new TreeMap<>();
		for ( final SlabTransform slabTransform : slabTransforms )
		{
			final TileInfo[] slabTiles = TileInfoJSONProvider.loadTilesConfiguration( slabTransform.stitchedTileConfiguration );
			for ( final Integer slab : slabTransform.slabs )
				slabTilesMap.put( slab, slabTiles );

			final RealInterval boundingBox = TileOperations.getRealCollectionBoundaries( slabTiles );
			if ( boundingBox.realMin( 0 ) != 0 || boundingBox.realMin( 1 ) != 0 || boundingBox.realMin( 2 ) != 0 )
				throw new Exception( "slabs are not translated to origin" );
			System.out.println( "slabs " + slabTransform.slabs + " bounding box: min=" + Arrays.toString( Intervals.minAsDoubleArray( boundingBox ) ) + ", max=" + Arrays.toString( Intervals.maxAsDoubleArray( boundingBox ) ) );
		}

		// slab -> translation
		final Map< Integer, double[] > slabTranslationsMap = new TreeMap<>();
		for ( final SlabTransform slabTransform : slabTransforms )
			for ( final Integer slab : slabTransform.slabs )
				slabTranslationsMap.put( slab, slabTransform.translation );

		// set tile transforms
		final Map< Integer, TileInfo > transformedTiles = new TreeMap<>();
		for ( final Integer slab : slabTransformsMap.keySet() )
		{
			final AffineTransform3D slabTransform = slabTransformsMap.get( slab );
			final TileInfo[] slabTiles = slabTilesMap.get( slab );
			final Translation slabTranslation = new Translation( slabTranslationsMap.get( slab ) );

			for ( final TileInfo tile : slabTiles )
			{
				if ( !transformedTiles.containsKey( tile.getIndex() ) )
				{
					final Translation tileTranslation = new Translation( tile.getPosition() );
					final AffineTransform3D tileTransform = new AffineTransform3D();
					tileTransform.preConcatenate( tileTranslation ).preConcatenate( slabTranslation ).preConcatenate( slabTransform );
					tile.setTransform( tileTransform );
					transformedTiles.put( tile.getIndex(), tile );
				}
			}
		}

		// save new tile configuration
		TileInfoJSONProvider.saveTilesConfiguration( transformedTiles.values().toArray( new TileInfo[ 0 ] ), Paths.get( Paths.get( args[ 0 ] ).getParent().toString(), "transformed-tiles.json" ).toString() );
	}
}
