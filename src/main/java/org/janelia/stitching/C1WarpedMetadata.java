package org.janelia.stitching;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.janelia.dataaccess.DataProviderFactory;

import bdv.img.TpsTransformWrapper;
import bigwarp.Landmarks2Transform;
import jitk.spline.ThinPlateR2LogRSplineKernelTransform;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;

public class C1WarpedMetadata
{
	public static final int NUM_CHANNELS = 2;
	public static final int NUM_DIMENSIONS = 3;

	private static final Map< String, String > LANDMARKS;
	static
	{
		LANDMARKS = new LinkedHashMap<>();
		LANDMARKS.put( "3z", "ch0_03_serialized.txt" );
		LANDMARKS.put( "4z", "ch0_04_serialized.txt" );
		LANDMARKS.put( "5z", "ch0_05_serialized.txt" );
		LANDMARKS.put( "6z", "ch0_06_serialized.txt" );
		LANDMARKS.put( "7z", "ch0_07-08-09_serialized.txt" );
		LANDMARKS.put( "8z", "ch0_08-09_serialized.txt" );
		LANDMARKS.put( "9z", "ch0_09-10_serialized.txt" );
		LANDMARKS.put( "10z", "ch0_10-11_serialized.txt" );
		LANDMARKS.put( "11z", "ch0_11-12_serialized.txt" );
		LANDMARKS.put( "12z-group0", "ch0_12z0-12z1_landmarks_serialized.txt" );
		LANDMARKS.put( "12z-group1", null );
		LANDMARKS.put( "13z", "ch0_13-12_serialized.txt" );
		LANDMARKS.put( "14z", "ch0_14-13_serialized.txt" );
		LANDMARKS.put( "15z", "ch0_15-14_serialized.txt" );
		LANDMARKS.put( "16z", "ch0_16-15_serialized.txt" );
		LANDMARKS.put( "17z", "ch0_17-16_serialized.txt" );
		LANDMARKS.put( "18z", "ch0_18-17_serialized.txt" );
	}
	private static final String SLABS_DIR = "/nrs/saalfeld/igor/illumination-correction/Sample1_C1/stitching/updated-config-paths";
	private static final String LANDMARKS_DIR = "/groups/saalfeld/home/bogovicj/projects/igor_illumiation-correction/serialized_transforms";
	private static final String FLATFIELD_DIR = "/nrs/saalfeld/igor/illumination-correction/Sample1_C1/flatfield-new";
	private static final String NEW_TILES_LOCATION = "/nrs/saalfeld/igor/Rui/Sample1_C1_rawdata/Position1_LargeTile2";
	private static final String BASE_PATH = "/nrs/saalfeld/igor/illumination-correction/Sample1_C1/stitching/restitching-affine";
	private static final double[] PIXEL_RESOLUTION = new double[] { 0.097, 0.097, 0.18 };

	public static TileSlabMapping getTileSlabMapping() throws IOException
	{
		// fill tile metadata
		final List< Map< String, TileInfo[] > > slabsTilesChannels = new ArrayList<>();
		for ( int ch = 0; ch < NUM_CHANNELS; ++ch )
		{
			final Map< String, TileInfo[] > slabsTiles = new HashMap<>();
			for ( final String slab : getSlabs() )
				slabsTiles.put( slab, getSlabTiles( slab, ch ) );
			slabsTilesChannels.add( slabsTiles );
		}

		final int[] tileCount = new int[ slabsTilesChannels.size() ];
		for ( int ch = 0; ch < tileCount.length; ++ch )
			for ( final TileInfo[] slabTiles : slabsTilesChannels.get( ch ).values() )
				tileCount[ ch ] += slabTiles.length;
		for ( int ch = 0; ch < tileCount.length; ++ch )
			System.out.println( "Total tiles in ch" + ch + ": " + tileCount[ ch ] );

		// fill slab metadata
		final Map< String, double[] > slabsMin = new HashMap<>();
		final Map< String, TpsTransformWrapper > slabsTransforms = new HashMap<>();
		for ( final String slab : getSlabs() )
		{
			slabsMin.put( slab, getSlabMin( slab ) );
			slabsTransforms.put( slab, getSlabTransform( slab ) );
		}

		return new TileSlabMapping( slabsTilesChannels, slabsMin, slabsTransforms );
	}

	public static Set< String > getSlabs()
	{
		return Collections.unmodifiableSet( LANDMARKS.keySet() );
	}

	public static TpsTransformWrapper getSlabTransform( final String slab ) throws IOException
	{
		if ( LANDMARKS.get( slab ) == null )
			return new TpsTransformWrapper( NUM_DIMENSIONS );

		final Path landmarksPath = Paths.get( LANDMARKS_DIR, LANDMARKS.get( slab ) );
		final String data = new String( Files.readAllBytes( landmarksPath ) );
		final ThinPlateR2LogRSplineKernelTransform transformRaw = Landmarks2Transform.fromDataString( data );
		return new TpsTransformWrapper( transformRaw.getNumDims(), transformRaw );
	}

	public static double[] getSlabMin( final String slab ) throws IOException
	{
		// make sure that slabMin is the same for all channels
		double[] slabMin = null;
		for ( int ch = 0; ch < NUM_CHANNELS; ++ch )
		{
			final TileInfo[] chSlabTiles = getSlabTiles( slab, ch, false );
			final double[] chSlabMin = Intervals.minAsDoubleArray( TileOperations.getRealCollectionBoundaries( chSlabTiles ) );
			if ( slabMin == null )
			{
				slabMin = chSlabMin.clone();
			}
			else
			{
				for ( int d = 0; d < slabMin.length; ++d )
					if ( !Util.isApproxEqual( slabMin[ d ], chSlabMin[ d ], 1e-10 ) )
						throw new RuntimeException( "slabMin is different for ch0/ch1" );
			}
		}
		return slabMin;
	}

	public static TileInfo[] getSlabTiles( final String slab, final int channel ) throws IOException
	{
		return getSlabTiles( slab, channel, true );
	}

	private static TileInfo[] getSlabTiles( final String slab, final int channel, final boolean removeMissing ) throws IOException
	{
		final Path slabPath = Paths.get( SLABS_DIR, slab, "ch" + channel + "-" + slab + "-final.json" );
		final List< TileInfo > slabTiles = new ArrayList<>(
				Arrays.asList(
						TileInfoJSONProvider.loadTilesConfiguration( DataProviderFactory.createFSDataProvider().getJsonReader( URI.create( slabPath.toString() ) ) )
					)
			);

		for ( final TileInfo tile : slabTiles )
			if ( tile.getPixelResolution() == null )
				tile.setPixelResolution( PIXEL_RESOLUTION );

		// hack to update tile paths
		for ( final TileInfo tile : slabTiles )
			tile.setFilePath( Paths.get( NEW_TILES_LOCATION, Paths.get( tile.getFilePath() ).getFileName().toString() ).toString() );

		// new mode: if requested, remove missing tiles in both channels
		if ( removeMissing )
		{
			for ( final Iterator< TileInfo > it = slabTiles.iterator(); it.hasNext(); )
			{
				final TileInfo tile = it.next();
				if ( tile.getIndex().intValue() == 18808 )
				{
					it.remove();
					break;
				}
			}
		}

		return slabTiles.toArray( new TileInfo[ 0 ] );
	}

	public static TileInfo[] getTiles( final int channel ) throws IOException
	{
		final List< TileInfo > tiles = new ArrayList<>();
		for ( final String slab : getSlabs() )
			tiles.addAll( Arrays.asList( getSlabTiles( slab, channel ) ) );
		return tiles.toArray( new TileInfo[ 0 ] );
	}

	public static String getFlatfieldPath( final int channel )
	{
		return Paths.get( FLATFIELD_DIR, "ch" + channel ).toString();
	}

	public static String getBasePath()
	{
		return BASE_PATH;
	}
}
