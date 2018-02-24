package org.janelia.stitching;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import bdv.img.TpsTransformWrapper;
import net.imglib2.util.Intervals;

public class C1WarpedStitchingSpark implements Serializable, AutoCloseable
{
	public static void main( final String[] args ) throws Exception
	{
		try ( final C1WarpedStitchingSpark driver = new C1WarpedStitchingSpark() )
		{
			driver.run();
		}
	}

	private static final long serialVersionUID = 6006962943789087537L;

	private C1WarpedStitchingJob job;
	private transient JavaSparkContext sparkContext;

	public void run() throws Exception
	{
		// load tile & slab metadata
		final List< Map< String, TileInfo[] > > slabsTilesChannels = new ArrayList<>();
		final Map< String, double[] > slabsMin = new HashMap<>();
		final Map< String, TpsTransformWrapper > slabsTransforms = new HashMap<>();

		for ( int channel = 0; channel < C1WarpedMetadata.NUM_CHANNELS; ++channel )
		{
			final Map< String, TileInfo[] > slabsTiles = new HashMap<>();

			for ( final String slab : C1WarpedMetadata.getSlabs() )
			{
				final TileInfo[] slabTiles = C1WarpedMetadata.getSlabTiles( slab, channel );

				for ( final TileInfo tile : slabTiles )
					if ( !Files.exists( Paths.get( tile.getFilePath() ) ) )
						throw new PipelineExecutionException( "Tile " + tile.getIndex() + " in ch" + channel + " cannot be found: " + tile.getFilePath() );

				slabsTiles.put( slab, slabTiles );

				final double[] slabMin = Intervals.minAsDoubleArray( TileOperations.getRealCollectionBoundaries( slabTiles ) );
				slabsMin.put( slab, slabMin );

				if ( !slabsTransforms.containsKey( slab ) )
					slabsTransforms.put( slab, C1WarpedMetadata.getTransform( slab ) );
			}

			slabsTilesChannels.add( slabsTiles );
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

		final TileSlabMapping tileSlabMapping = new TileSlabMapping( slabsTilesChannels, slabsMin, slabsTransforms );

		job = new C1WarpedStitchingJob( tileSlabMapping );

		final SerializableStitchingParameters params = new SerializableStitchingParameters();
		params.channel1 = 1;
		params.channel2 = 1;
		params.checkPeaks = 50;
		params.computeOverlap = true;
		params.subpixelAccuracy = true;
		params.virtual = false;
		params.absoluteThreshold = 5;
		params.relativeThreshold = 3;
		params.dimensionality = C1WarpedMetadata.NUM_DIMENSIONS;
		job.setParams( params );

		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "C1WarpedStitching" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);

		final WarpedStitchingExecutor warpedStitchingExecutor = new WarpedStitchingExecutor( job, sparkContext );
		warpedStitchingExecutor.run();

		System.out.println( "Done" );
	}

	@Override
	public void close()
	{
		if ( sparkContext != null )
		{
			sparkContext.close();
			sparkContext = null;
		}
	}
}
