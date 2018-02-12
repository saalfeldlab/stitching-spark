package org.janelia.stitching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import bdv.img.TpsTransformWrapper;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;

public class C1WarpedTileCounterSpark
{
	public static void main( final String[] args ) throws IOException, PipelineExecutionException
	{
		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "C1WarpedTileCounter" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			run( sparkContext );
		}
	}

	private static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void run(
			final JavaSparkContext sparkContext ) throws IOException, PipelineExecutionException
	{
		final int outputCellSize = 128;
		final double[] voxelDimensions = new double[] { 0.097, 0.097, 0.18 };

		final List< Map< String, TileInfo[] > > slabsTilesChannels = new ArrayList<>();
		final Map< String, double[] > slabsMin = new HashMap<>();
		final Map< String, TpsTransformWrapper > slabsTransforms = new HashMap<>();

		final Map< String, TileInfo[] > slabsTiles = new HashMap<>();
		for ( final String slab : C1WarpedMetadata.getSlabs() )
		{
			final TileInfo[] slabTiles = C1WarpedMetadata.getSlabTiles( slab, 0 );

			// set default pixel resolution if null
			for ( final TileInfo tile : slabTiles )
				if ( tile.getPixelResolution() == null )
					tile.setPixelResolution( voxelDimensions );

			slabsTiles.put( slab, slabTiles );

			final double[] slabMin = Intervals.minAsDoubleArray( TileOperations.getRealCollectionBoundaries( slabTiles ) );
			slabsMin.put( slab, slabMin );

			if ( !slabsTransforms.containsKey( slab ) )
				slabsTransforms.put( slab, C1WarpedMetadata.getTransform( slab ) );
		}

		System.out.println();
		for ( int channel = 0; channel < slabsTilesChannels.size(); ++channel )
		{
			int numTiles = 0;
			for ( final Entry< String, TileInfo[] > entry : slabsTilesChannels.get( channel ).entrySet() )
				numTiles += entry.getValue().length;
			System.out.println( "ch" + channel + ": " + numTiles + " tiles" );
		}
		System.out.println();

		final String outputPath = "/nrs/saalfeld/igor/illumination-correction/Sample1_C1/stitching/warped-export";

		final WarpedTileCounter exporter = new WarpedTileCounter(
				sparkContext,
				slabsTiles,
				slabsMin,
				slabsTransforms,
				voxelDimensions,
				outputCellSize,
				outputPath
			);

		exporter.run();
	}
}
