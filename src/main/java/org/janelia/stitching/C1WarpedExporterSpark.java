package org.janelia.stitching;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import bdv.img.TpsTransformWrapper;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;

public class C1WarpedExporterSpark
{
	public static void main( final String[] args ) throws IOException, PipelineExecutionException
	{
		final String flatfieldPath = args.length > 0 ? args[ 0 ] : null;

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "C1WarpedExport" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			run( sparkContext, flatfieldPath );
		}
	}

	private static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void run(
			final JavaSparkContext sparkContext,
			final String flatfieldPath ) throws IOException, PipelineExecutionException
	{
		final int outputCellSize = 128;
		final double[] voxelDimensions = new double[] { 0.097, 0.097, 0.18 };

		final List< Map< String, TileInfo[] > > slabsTilesChannels = new ArrayList<>();
		final Map< String, double[] > slabsMin = new HashMap<>();
		final Map< String, TpsTransformWrapper > slabsTransforms = new HashMap<>();

		for ( int channel = 0; channel < C1WarpedMetadata.NUM_CHANNELS; ++channel )
		{
			final Map< String, TileInfo[] > slabsTiles = new HashMap<>();

			for ( final String slab : C1WarpedMetadata.getSlabs() )
			{
				final TileInfo[] slabTiles = C1WarpedMetadata.getSlabTiles( slab, channel );

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

			slabsTilesChannels.add( slabsTiles );
		}

		if ( flatfieldPath != null )
			System.out.println( "Exporting using flatfields from " + flatfieldPath );
		else
			System.out.println( "Exporting without flatfield correction" );

		final WarpedExporter< T, U > exporter = new WarpedExporter<>(
				sparkContext,
				slabsTilesChannels,
				slabsMin,
				slabsTransforms,
				voxelDimensions,
				outputCellSize,
				"/nrs/saalfeld/igor/illumination-correction/Sample1_C1/stitching/decon-warped-export",
				flatfieldPath
			);

//		final Interval subinterval = new FinalInterval( new long[] { 3000, 3000, 3000 }, new long[] { 3999, 3999, 3999 } );
//		exporter.setSubInterval( subinterval );

		exporter.run();
	}
}
