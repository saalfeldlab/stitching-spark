package org.janelia.stitching;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.PathResolver;
import org.janelia.util.Conversions;

import mpicbg.models.AffineModel3D;
import mpicbg.models.Point;
import mpicbg.models.PointMatch;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.InvertibleRealTransform;
import net.imglib2.util.ValuePair;
import scala.Tuple2;

public class C1WarpedTilesToAffine implements Serializable, AutoCloseable
{
	private static final long serialVersionUID = -3225482154805368462L;

	private C1WarpedStitchingJob job;
	private transient JavaSparkContext sparkContext;

	public static void main( final String[] args ) throws Exception
	{
		final String outputConfigurationsPath = args[ 0 ];

		final String filteredTilesStr = args.length > 1 ? args[ 1 ] : null;
		final Set< Integer > filteredTiles;
		if ( filteredTilesStr != null )
			filteredTiles = new TreeSet<>( Arrays.asList( Conversions.toBoxedArray( Conversions.parseIntArray( filteredTilesStr.split( "," ) ) ) ) );
		else
			filteredTiles = null;

		try ( final C1WarpedTilesToAffine driver = new C1WarpedTilesToAffine() )
		{
			driver.run( outputConfigurationsPath, filteredTiles );
		}

		System.out.println( "Done" );
	}

	public void run( final String outputConfigurationsPath, final Set< Integer > filteredTiles ) throws Exception
	{
		job = new C1WarpedStitchingJob( C1WarpedMetadata.getTileSlabMapping(), filteredTiles );

		final SerializableStitchingParameters params = new SerializableStitchingParameters();
		params.channel1 = 1;
		params.channel2 = 1;
		params.checkPeaks = 100;
		params.computeOverlap = true;
		params.subpixelAccuracy = true;
		params.dimensionality = C1WarpedMetadata.NUM_DIMENSIONS;
		job.setParams( params );

		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "C1WarpedTilesToAffine" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			);

		final Map< Integer, ValuePair< AffineTransform3D, Double > > tileAffineTransforms = sparkContext
				.parallelize( Arrays.asList( job.getTiles( 0 ) ), job.getTiles( 0 ).length )
				.mapToPair( tile ->
					{
						final InvertibleRealTransform tileWarpingTransform = WarpedTileOperations.getTileTransform( tile, job.getTileSlabMapping() );

						final List< PointMatch > pointMatches = new ArrayList<>();
						final int[] cornerDims = new int[ tile.numDimensions() ];
						Arrays.fill( cornerDims, 2 );
						final IntervalIterator cornerIterator = new IntervalIterator( cornerDims );

						while ( cornerIterator.hasNext() )
						{
							cornerIterator.fwd();

							final double[] pointLocal = new double[ tile.numDimensions() ];
							for ( int d = 0; d < pointLocal.length; ++d )
								pointLocal[ d ] = cornerIterator.getIntPosition( d ) == 0 ? 0 : tile.getSize( d ) - 1;

							final double[] pointWorld = new double[ tile.numDimensions() ];
							tileWarpingTransform.apply( pointLocal, pointWorld );

							pointMatches.add( new PointMatch( new Point( pointLocal ), new Point( pointWorld ) ) );
						}

						final AffineModel3D affineModel = new AffineModel3D();
						affineModel.fit( pointMatches );

						PointMatch.apply( pointMatches, affineModel );
						final double avgError = PointMatch.meanDistance( pointMatches ), maxError = PointMatch.maxDistance( pointMatches );

						System.out.println( String.format(
								"Estimated affine transform for tile %d (%s), avg.error=%.2f, max.error=%.2f",
								tile.getIndex(),
								job.getTileSlabMapping().getSlab( tile ),
								avgError,
								maxError
							) );

						final double[][] affineMatrix = new double[ 3 ][ 4 ];
						affineModel.toMatrix( affineMatrix );

						final AffineTransform3D tileAffineTransform = new AffineTransform3D();
						tileAffineTransform.set( affineMatrix );

						return new Tuple2<>( tile.getIndex(), new ValuePair<>( tileAffineTransform, new Double( avgError ) ) );
					} )
				.collectAsMap();

		sparkContext.close();
		sparkContext = null;

		System.out.println( System.lineSeparator() );
		System.out.println( "-------------------------------------" );
		System.out.println( System.lineSeparator() );

		double globalAvgError = 0, globalMaxError = 0;
		for ( final ValuePair< AffineTransform3D, Double > entry : tileAffineTransforms.values() )
		{
			globalAvgError += entry.getB();
			globalMaxError = Math.max( entry.getB(), globalMaxError );
		}
		globalAvgError /= tileAffineTransforms.size();
		System.out.println( String.format( "Estimated affine transformations for %d tiles", tileAffineTransforms.size() ) );
		System.out.println( String.format( "Global avg.error=%.2f, global max.error=%.2f", globalAvgError, globalMaxError ) );

		final DataProvider dataProvider = job.getDataProvider();

		for ( int channel = 0; channel < job.getChannels(); ++channel )
		{
			final TileInfo[] tiles = job.getTiles( channel );
			final List< TileInfo > newTiles = new ArrayList<>();
			for ( final TileInfo tile : tiles )
			{
				final TileInfo newTile = tile.clone();
				newTile.setTransform( tileAffineTransforms.get( newTile.getIndex() ).getA() );
				newTiles.add( newTile );
			}

			TileInfoJSONProvider.saveTilesConfiguration(
					newTiles.toArray( new TileInfo[ 0 ] ),
					dataProvider.getJsonWriter( URI.create( PathResolver.get( outputConfigurationsPath, "ch" + channel + "-affine-approx.json" ) ) )
				);
		}
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
