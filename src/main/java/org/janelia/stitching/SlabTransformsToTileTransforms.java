package org.janelia.stitching;

public class SlabTransformsToTileTransforms
{
	/*private static class SlabTransform
	{
		public List< Integer > slabs;
		public AffineTransform3D transform;
		public String stitchedTileConfiguration;
		public double[] translation;
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
		final String inputPath = args[ 0 ];
		final DataProvider dataProvider = DataProviderFactory.createByURI( URI.create( inputPath ) );

		final List< SlabTransform > slabTransforms = new ArrayList<>();
		try ( final Reader reader = dataProvider.getJsonReader( URI.create( inputPath ) ) )
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
			final TileInfo[] slabTiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( slabTransform.stitchedTileConfiguration ) ) );
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
		TileInfoJSONProvider.saveTilesConfiguration(
				transformedTiles.values().toArray( new TileInfo[ 0 ] ),
				dataProvider.getJsonWriter( URI.create(
						PathResolver.get(
								PathResolver.getParent( inputPath ),
								"transformed-tiles.json"
							)
					) )
			);
	}*/
}
