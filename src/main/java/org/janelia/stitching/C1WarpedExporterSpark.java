package org.janelia.stitching;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.util.Conversions;

import bdv.img.TpsTransformWrapper;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;

public class C1WarpedExporterSpark
{
	public static void main( final String[] args ) throws IOException, PipelineExecutionException
	{
		final String flatfieldPath = args.length > 0 ? args[ 0 ] : null;
		final String filteredTilesStr = args.length > ( flatfieldPath == null ? 0 : 1 ) ? args[  (flatfieldPath == null ? 0 : 1 ) ] : null;
		final Set< Integer > filteredTiles;
		if ( filteredTilesStr != null )
			filteredTiles = new TreeSet<>( Arrays.asList( Conversions.toBoxedArray( Conversions.parseIntArray( filteredTilesStr.split( "," ) ) ) ) );
		else
			filteredTiles = null;

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "C1WarpedExport" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			run(
					sparkContext,
					flatfieldPath == null || flatfieldPath.isEmpty() ? null : flatfieldPath,
					filteredTiles
				);
		}
	}

	private static < T extends NativeType< T > & RealType< T >, U extends NativeType< U > & RealType< U > > void run(
			final JavaSparkContext sparkContext,
			final String flatfieldPath,
			final Set< Integer > filteredTiles ) throws IOException, PipelineExecutionException
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

				for ( final TileInfo tile : slabTiles )
					if ( !Files.exists( Paths.get( tile.getFilePath() ) ) )
						throw new PipelineExecutionException( "Tile " + tile.getIndex() + " in ch" + channel + " cannot be found: " + tile.getFilePath() );

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

		System.out.println();
		for ( int channel = 0; channel < slabsTilesChannels.size(); ++channel )
		{
			int numTiles = 0;
			for ( final Entry< String, TileInfo[] > entry : slabsTilesChannels.get( channel ).entrySet() )
				numTiles += entry.getValue().length;
			System.out.println( "ch" + channel + ": " + numTiles + " tiles" );
		}
		System.out.println();

		if ( flatfieldPath != null )
			System.out.println( "Exporting using flatfields from " + flatfieldPath );
		else
			System.out.println( "Exporting without flatfield correction" );

		final String outputPathFlatfieldSuffix = flatfieldPath != null ? "-flatfield" : "";
		final String outputPathFilteredTilesSuffix = filteredTiles != null ? "-" + Conversions.toCommaSeparatedString( filteredTiles ) : "";
		final String outputPath = "/nrs/saalfeld/igor/illumination-correction/Sample1_C1/stitching/warped-export" + outputPathFlatfieldSuffix + outputPathFilteredTilesSuffix;

		final WarpedExporter< T, U > exporter = new WarpedExporter<>(
				sparkContext,
				slabsTilesChannels,
				slabsMin,
				slabsTransforms,
				voxelDimensions,
				outputCellSize,
				outputPath,
				flatfieldPath,
				filteredTiles
			);

//		final Interval subinterval = new FinalInterval( new long[] { 3000, 3000, 3000 }, new long[] { 3999, 3999, 3999 } );
//		exporter.setSubInterval( subinterval );

		exporter.run();
	}
}
