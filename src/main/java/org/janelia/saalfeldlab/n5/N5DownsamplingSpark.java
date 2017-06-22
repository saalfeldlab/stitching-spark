package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.export.Downsample;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import scala.Tuple2;

public class N5DownsamplingSpark< T extends NativeType< T > & RealType< T > >
{
	public static void main( final String[] args ) throws IOException
	{
		final String basePath = args[ 0 ];
		final String datasetPath = args[ 1 ];
		final String pixelResolutionStr = args.length > 2 ? args[ 2 ] : null;

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "N5Downsampling" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
			) )
		{
			if ( pixelResolutionStr == null )
			{
				new N5DownsamplingSpark<>( sparkContext ).downsampleIsotropic( basePath, datasetPath );
			}
			else
			{
				final String[] tokens = pixelResolutionStr.split( "," );
				final double[] pixelResolution = new double[ tokens.length ];
				for ( int i = 0; i < pixelResolution.length; ++i )
					pixelResolution[ i ] = Double.parseDouble( tokens[ i ] );

				new N5DownsamplingSpark<>( sparkContext ).downsample( basePath, datasetPath, pixelResolution );
			}
		}
	}

	private final transient JavaSparkContext sparkContext;

	public N5DownsamplingSpark( final JavaSparkContext sparkContext )
	{
		this.sparkContext = sparkContext;
	}

	public void downsampleIsotropic( final String basePath, final String datasetPath ) throws IOException
	{
		downsample( basePath, datasetPath, null );
	}

	// NOTE: assumes that pixelResolutionX = pixelResolutionY
	public void downsample( final String basePath, final String datasetPath, final double[] pixelResolution ) throws IOException
	{
		final double pixelResolutionZToXY = ( pixelResolution == null ? 1 : pixelResolution[ 2 ] / Math.max( pixelResolution[ 0 ], pixelResolution[ 1 ] ) );
		downsampleXY( basePath, datasetPath );
		downsampleZ( basePath, datasetPath, pixelResolutionZToXY );
		deleteXY( basePath, datasetPath );
	}

	// TODO: unify with downsampleZ as these two methods share a lot of similar code
	private void downsampleXY(final String basePath, final String datasetPath ) throws IOException
	{
		final String fullScaleLevelPath = Paths.get( datasetPath ).resolve( "s0" ).toString();

		final N5Writer n5 = N5.openFSWriter( basePath );
		final DatasetAttributes attributes = n5.getDatasetAttributes( fullScaleLevelPath );
		final long[] fullScaleDimensions = attributes.getDimensions();
		final int[] cellSize = attributes.getBlockSize();
		final int dim = fullScaleDimensions.length;

		final String xyGroupPath = Paths.get( datasetPath ).resolve( "xy" ).toString();
		n5.createGroup( xyGroupPath );
		// loop over scale levels
		String previousScaleLevel = fullScaleLevelPath;
		final long[] previousDimensions = fullScaleDimensions.clone();
		final long[] downsampledDimensions = new long[ dim ];
		for ( int scaleLevel = 1; ; ++scaleLevel )
		{
			for ( int i = 0; i < 2; i++ )
				downsampledDimensions[ i ] = fullScaleDimensions[ i ] >> scaleLevel;
			System.arraycopy( fullScaleDimensions, 2, downsampledDimensions, 2, fullScaleDimensions.length - 2 );

			if ( Math.min( downsampledDimensions[ 0 ], downsampledDimensions[ 1 ] ) <= 1 || Math.max( downsampledDimensions[ 0 ], downsampledDimensions[ 1 ] ) <= Math.max( cellSize[ 0 ], cellSize[ 1 ] ) )
				break;

			final String scaleLevelPath = Paths.get( xyGroupPath ).resolve( "s" + scaleLevel ).toString();
			n5.createDataset( scaleLevelPath, downsampledDimensions, cellSize, attributes.getDataType(), attributes.getCompressionType() );

			final List< Tuple2< Interval, Interval > > sourceAndTargetIntervals = new ArrayList<>();
			final long[] max = Intervals.maxAsLongArray( new FinalInterval( downsampledDimensions ) );
			final long[] offset = new long[ dim ], sourceMin = new long[ dim ], sourceMax = new long[ dim ], targetMin = new long[ dim ], targetMax = new long[ dim ];
			for ( int d = 0; d < dim; )
			{
				for ( int i = 0; i < 2; i++ )
				{
					targetMin[ i ] = offset[ i ];
					targetMax[ i ] = Math.min( targetMin[ i ] + cellSize[ i ] - 1, max[ i ] );
					sourceMin[ i ] = targetMin[ i ] * 2;
					sourceMax[ i ] = targetMax[ i ] * 2 + 1;
				}
				for ( int i = 2; i < dim; i++ )
				{
					targetMin[ i ] = sourceMin[ i ] = offset[ i ];
					targetMax[ i ] = sourceMax[ i ] = Math.min( targetMin[ i ] + cellSize[ i ] - 1, max[ i ] );
				}

				sourceAndTargetIntervals.add( new Tuple2<>( new FinalInterval( sourceMin, sourceMax ), new FinalInterval( targetMin, targetMax ) ) );

				for ( d = 0; d < dim; ++d )
				{
					offset[ d ] += cellSize[ d ];
					if ( offset[ d ] <= max[ d ] )
						break;
					else
						offset[ d ] = 0;
				}
			}

			final int[] downsamplingFactors = new int[ dim ];
			Arrays.fill( downsamplingFactors, 1 );
			for ( int i = 0; i < 2; i++ )
				downsamplingFactors[ i ] = 2;

			final String previousScaleLevelSpark = previousScaleLevel;
			sparkContext.parallelize( sourceAndTargetIntervals ).foreach( sourceAndTargetInterval ->
			{
				final N5Writer n5Local = N5.openFSWriter( basePath );

				// TODO: skip this target block if all source blocks are empty (not present)

				final RandomAccessibleInterval< T > previousScaleLevelImg = N5Utils.open( n5Local, previousScaleLevelSpark );
				final RandomAccessibleInterval< T > source = Views.offsetInterval( previousScaleLevelImg, sourceAndTargetInterval._1() );
				final Img< T > target = new ArrayImgFactory< T >().create( sourceAndTargetInterval._2(), Util.getTypeFromInterval( source ) );
				Downsample.downsample( source, target, downsamplingFactors );
				final long[] gridPosition = cellCoordinatesToGridPosition( Intervals.minAsLongArray( sourceAndTargetInterval._2() ), cellSize );
				N5Utils.saveBlock( target, n5Local, scaleLevelPath, gridPosition );
			} );

			previousScaleLevel = scaleLevelPath;
			System.arraycopy( downsampledDimensions, 0, previousDimensions, 0, downsampledDimensions.length );
		}
	}

	// TODO: unify with downsampleXY as these two methods share a lot of similar code
	private void downsampleZ(final String basePath, final String datasetPath, final double pixelResolutionZToXY ) throws IOException
	{
		final N5Writer n5 = N5.openFSWriter( basePath );

		// loop over scale levels
		for ( int scaleLevel = 1; ; ++scaleLevel )
		{
			final String xyScaleLevelPath = Paths.get( datasetPath ).resolve( "xy" ).resolve( "s" + scaleLevel ).toString();
			final String scaleLevelPath = Paths.get( datasetPath ).resolve( "s" + scaleLevel ).toString();

			if ( !n5.datasetExists( xyScaleLevelPath ) )	// XY limit reached
				break;

			final DatasetAttributes attributes = n5.getDatasetAttributes( xyScaleLevelPath );
			final long[] xyScaleDimensions = attributes.getDimensions();
			final int[] xyCellSize = attributes.getBlockSize();
			final int dim = xyScaleDimensions.length;

			final Tuple2< Integer, Integer > zCellSizeAndDownsamplingFactor = getOptimalZCellSizeAndDownsamplingFactor( scaleLevel, Math.max( xyCellSize[ 0 ], xyCellSize[ 1 ] ), pixelResolutionZToXY );

			final long[] downsampledDimensions = new long[ dim ];
			for ( int d = 2; d < downsampledDimensions.length; ++d )
				downsampledDimensions[ d ] = xyScaleDimensions[ d ] / zCellSizeAndDownsamplingFactor._2();
			System.arraycopy( xyScaleDimensions, 0, downsampledDimensions, 0, 2 );

			final int[] cellSize = new int[ dim ];
			Arrays.fill( cellSize, zCellSizeAndDownsamplingFactor._1() );
			System.arraycopy( xyCellSize, 0, cellSize, 0, 2 );

			if ( Arrays.stream( downsampledDimensions ).min().getAsLong() <= 1 || Arrays.stream( downsampledDimensions ).max().getAsLong() <= Math.max( cellSize[ 0 ], cellSize[ 1 ] ) )
				break;

			n5.createDataset( scaleLevelPath, downsampledDimensions, cellSize, attributes.getDataType(), attributes.getCompressionType() );

			final List< Tuple2< Interval, Interval > > sourceAndTargetIntervals = new ArrayList<>();
			final long[] max = Intervals.maxAsLongArray( new FinalInterval( downsampledDimensions ) );
			final long[] offset = new long[ dim ], sourceMin = new long[ dim ], sourceMax = new long[ dim ], targetMin = new long[ dim ], targetMax = new long[ dim ];
			for ( int d = 0; d < dim; )
			{

				for ( int i = 0; i < 2; i++ )
				{
					targetMin[ i ] = sourceMin[ i ] = offset[ i ];
					targetMax[ i ] = sourceMax[ i ] = Math.min( targetMin[ i ] + cellSize[ i ] - 1, max[ i ] );
				}
				for ( int i = 2; i < dim; i++ )
				{
					targetMin[ i ] = offset[ i ];
					targetMax[ i ] = Math.min( targetMin[ i ] + cellSize[ i ] - 1, max[ i ] );
					sourceMin[ i ] = targetMin[ i ] * zCellSizeAndDownsamplingFactor._2();
					sourceMax[ i ] = targetMax[ i ] * zCellSizeAndDownsamplingFactor._2() + ( zCellSizeAndDownsamplingFactor._2() - 1 );
				}

				sourceAndTargetIntervals.add( new Tuple2<>( new FinalInterval( sourceMin, sourceMax ), new FinalInterval( targetMin, targetMax ) ) );

				for ( d = 0; d < dim; ++d )
				{
					offset[ d ] += cellSize[ d ];
					if ( offset[ d ] <= max[ d ] )
						break;
					else
						offset[ d ] = 0;
				}
			}

			final int[] downsamplingFactors = new int[ dim ];
			Arrays.fill( downsamplingFactors, 1 );
			for ( int i = 2; i < dim; ++i )
				downsamplingFactors[ i ] = zCellSizeAndDownsamplingFactor._2();

			sparkContext.parallelize( sourceAndTargetIntervals ).foreach( sourceAndTargetInterval ->
			{
				final N5Writer n5Local = N5.openFSWriter( basePath );
				final RandomAccessibleInterval< T > xyScaleLevelImg = N5Utils.open( n5Local, xyScaleLevelPath );
				final RandomAccessibleInterval< T > source = Views.offsetInterval( xyScaleLevelImg, sourceAndTargetInterval._1() );
				final Img< T > target = new ArrayImgFactory< T >().create( sourceAndTargetInterval._2(), Util.getTypeFromInterval( source ) );
				Downsample.downsample( source, target, downsamplingFactors );
				final long[] gridPosition = cellCoordinatesToGridPosition( Intervals.minAsLongArray( sourceAndTargetInterval._2() ), cellSize );
				N5Utils.saveBlock( target, n5Local, scaleLevelPath, gridPosition );
			} );
		}
	}

	private void deleteXY( final String basePath, final String datasetPath ) throws IOException
	{
		final String xyGroupPath = Paths.get( datasetPath ).resolve( "xy" ).toString();
		final N5Writer n5 = N5.openFSWriter( basePath );
		n5.remove( xyGroupPath );
	}

	private static Tuple2< Integer, Integer > getOptimalZCellSizeAndDownsamplingFactor( final int scaleLevel, final int cellSizeXY, final double pixelResolutionZToXY )
	{
		final int isotropicScaling = ( int ) Math.round( ( 1 << scaleLevel ) / pixelResolutionZToXY );
		final int zDownsamplingFactor = Math.max( isotropicScaling, 1 );
		final int fullScaleOptimalCellSize = ( int ) Math.round( cellSizeXY / pixelResolutionZToXY );
		final int zCellSize = ( fullScaleOptimalCellSize << scaleLevel ) / zDownsamplingFactor;
		return new Tuple2<>( zCellSize, zDownsamplingFactor );
	}

	private static long[] cellCoordinatesToGridPosition( final long[] cellCoordinates, final int[] cellSize )
	{
		final long[] gridPosition = new long[ cellCoordinates.length ];
		for ( int d = 0; d < gridPosition.length; d++ )
			gridPosition[ d ] = cellCoordinates[ d ] / cellSize[ d ];
		return gridPosition;
	}
}
