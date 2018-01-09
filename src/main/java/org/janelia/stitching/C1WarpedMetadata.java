package org.janelia.stitching;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import bdv.img.TpsTransformWrapper;
import bigwarp.Landmarks2Transform;
import jitk.spline.ThinPlateR2LogRSplineKernelTransform;

public class C1WarpedMetadata
{
	public static final int NUM_CHANNELS = 2;
	public static final int NUM_DIMENSIONS = 3;

	private static final Map< String, String > LANDMARKS;
	static
	{
		LANDMARKS = new HashMap<>();
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
	private static final String SLABS_DIR = "/nrs/saalfeld/igor/illumination-correction/Sample1_C1/stitching/decon-export-slabs";
	private static final String LANDMARKS_DIR = "/groups/saalfeld/home/bogovicj/projects/igor_illumiation-correction/serialized_transforms";

	public static Set< String > getSlabs()
	{
		return Collections.unmodifiableSet( LANDMARKS.keySet() );
	}

	public static TpsTransformWrapper getTransform( final String slab ) throws IOException
	{
		if ( LANDMARKS.get( slab ) == null )
			return new TpsTransformWrapper( NUM_DIMENSIONS );

		final Path landmarksPath = Paths.get( LANDMARKS_DIR, LANDMARKS.get( slab ) );
		final String data = new String( Files.readAllBytes( landmarksPath ) );
		final ThinPlateR2LogRSplineKernelTransform transformRaw = Landmarks2Transform.fromDataString( data );
		return new TpsTransformWrapper( transformRaw.getNumDims(), transformRaw );
	}

	public static TileInfo[] getSlabTiles( final String slab, final int channel ) throws IOException
	{
		final Path slabPath = Paths.get( SLABS_DIR, slab, "ch" + channel + "-" + slab + "-final-decon.json" );
		return TileInfoJSONProvider.loadTilesConfiguration( slabPath.toString() );
	}

	public static TileInfo[] getTiles( final int channel ) throws IOException
	{
		final List< TileInfo > tiles = new ArrayList<>();
		for ( final String slab : getSlabs() )
			tiles.addAll( Arrays.asList( getSlabTiles( slab, channel ) ) );
		return tiles.toArray( new TileInfo[ 0 ] );
	}
}
