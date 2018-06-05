package org.janelia.stitching;

import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.NotImplementedException;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadata;
import org.janelia.saalfeldlab.n5.bdv.N5ExportMetadataReader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import ij.IJ;
import ij.ImagePlus;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.exception.ImgLibException;
import net.imglib2.img.imageplus.ImagePlusImg;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.realtransform.Translation;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.IntervalsHelper;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class StitchingTestGenerator
{
	private static enum Mode
	{
		Unchanged,
		Shifted,
		Transformed
	}

	public static void main( final String[] args ) throws Exception
	{
		final String n5Path = args[ 0 ];
		final String datasetPath = args[ 1 ];
		final String outputPath = args[ 2 ];

		final Mode mode = Mode.valueOf( args[ 3 ].substring( 0, 1 ).toUpperCase() + args[ 3 ].substring( 1 ).toLowerCase() ); // the exact match is needed

		final AbstractTestGenerator< ? > testGen;
		switch ( mode )
		{
		case Unchanged:
			testGen = new UnchangedTestGenerator<>( n5Path, datasetPath, outputPath );
			break;
		case Shifted:
			testGen = new ShiftedTestGenerator<>( n5Path, datasetPath, outputPath );
			break;
		default:
			throw new NotImplementedException();
		}

		testGen.run();
		System.out.println( "Done" );
	}

	private static abstract class AbstractTestGenerator< T extends NativeType< T > & RealType< T > >
	{
		protected static final Dimensions tileDimensions = new FinalDimensions( 500, 400, 300 );
		protected static final double overlapRatio = 0.1;
		protected static final double cropRatio = 0.25;

		protected final String n5Path;
		protected final String datasetPath;
		protected final String outputPath;

		AbstractTestGenerator(
				final String n5Path,
				final String datasetPath,
				final String outputPath )
		{
			this.n5Path = n5Path;
			this.datasetPath = datasetPath;
			this.outputPath = outputPath;
		}

		abstract void run() throws Exception;

		protected static Interval getCropInterval( final Interval interval, final double cropRatio )
		{
			final long[] cropMin = new long[ interval.numDimensions() ], cropMax = new long[ interval.numDimensions() ];
			for ( int d = 0; d < interval.numDimensions(); ++d )
			{
				cropMin[ d ] = interval.min( d ) + Math.round( interval.dimension( d ) * ( 0.5 - cropRatio / 2 ) );
				cropMax[ d ] = cropMin[ d ] + Math.round( interval.dimension( d ) * cropRatio ) - 1;
			}
			return new FinalInterval( cropMin, cropMax );
		}

		protected static < T extends NativeType< T > & RealType< T > > ImagePlus copyToImage( final RandomAccessibleInterval< T > srcImg ) throws ImgLibException
		{
			final ImagePlusImg< T, ? > dstImg = new ImagePlusImgFactory< T >().create( srcImg, Util.getTypeFromInterval( srcImg ) );
			final Cursor< T > srcImgCursor = Views.flatIterable( srcImg ).cursor();
			final Cursor< T > dstImgCursor = Views.flatIterable( dstImg ).cursor();
			while ( srcImgCursor.hasNext() || dstImgCursor.hasNext() )
				dstImgCursor.next().set( srcImgCursor.next() );

			final ImagePlus dstImp = dstImg.getImagePlus();
			Utils.workaroundImagePlusNSlices( dstImp );
			return dstImp;
		}
	}

	private static class UnchangedTestGenerator< T extends NativeType< T > & RealType< T > > extends AbstractTestGenerator< T >
	{
		UnchangedTestGenerator(
				final String n5Path,
				final String datasetPath,
				final String outputPath )
		{
			super( n5Path, datasetPath, outputPath );
		}

		@Override
		public void run() throws Exception
		{
			final DataProvider dataProvider = DataProviderFactory.createByURI( URI.create( n5Path ) );
			final N5Reader n5 = dataProvider.createN5Reader( URI.create( n5Path ), N5ExportMetadata.getGsonBuilder() );

			final N5ExportMetadataReader exportMetadata = N5ExportMetadata.openForReading( n5 );
			final VoxelDimensions voxelDimensions = exportMetadata.getPixelResolution( 0 );
			final double[] pixelResolution = new double[ voxelDimensions.numDimensions() ];
			voxelDimensions.dimensions( pixelResolution );

			final RandomAccessibleInterval< T > img = N5Utils.open( n5, datasetPath );
			final Interval cropInterval = getCropInterval( img, cropRatio );
			System.out.println( "crop dimensions = " + Arrays.toString( Intervals.dimensionsAsLongArray( cropInterval ) ) );

			final List< Interval > nonOverlappingIntervals = TileOperations.divideSpaceIgnoreSmaller( cropInterval, tileDimensions );
			final List< TileInfo > tiles = new ArrayList<>(), groundtruthTiles = new ArrayList<>();

			final String outputTileImagesDir = Paths.get( outputPath, "imgs" ).toString();
			Paths.get( outputTileImagesDir ).toFile().mkdirs();

			System.out.println( "intervals: " + nonOverlappingIntervals.size() );

			for ( final Interval nonOverlappingInterval : nonOverlappingIntervals )
			{
				final long[] offset = new long[ nonOverlappingInterval.numDimensions() ];
				for ( int d = 0; d < offset.length; ++d )
					offset[ d ] = Math.round( ( ( nonOverlappingInterval.min( d ) - cropInterval.min( d ) ) / tileDimensions.dimension( d ) ) * tileDimensions.dimension( d ) * overlapRatio );

				final Interval overlappingInterval = IntervalsHelper.offset( nonOverlappingInterval, offset );

				final TileInfo tile = new TileInfo( 3 );
				tile.setIndex( tiles.size() );

				tile.setStagePosition( Intervals.minAsDoubleArray( overlappingInterval ) );
				tile.setSize( Intervals.dimensionsAsLongArray( overlappingInterval ) );

				final RandomAccessibleInterval< T > cropImg = Views.interval( img, overlappingInterval );
				final ImagePlus tileImp = copyToImage( cropImg );
				final String tileImpPath = Paths.get( outputTileImagesDir, "tile_" + tile.getIndex() + ".tif" ).toString();
				IJ.saveAsTiff( tileImp, tileImpPath );

				tile.setFilePath( tileImpPath );
				tile.setType( ImageType.valueOf( tileImp.getType() ) );
				tile.setPixelResolution( pixelResolution.clone() );

				tiles.add( tile );

				final TileInfo groundtruthTile = tile.clone();
				groundtruthTile.setTransform( new Translation( groundtruthTile.getStagePosition() ) );
				groundtruthTiles.add( groundtruthTile );
			}

			final String outputTilesConfigPath = Paths.get( outputPath, "tiles.json" ).toString();
			TileInfoJSONProvider.saveTilesConfiguration( tiles.toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter(
					URI.create( outputTilesConfigPath )
				) );

			TileInfoJSONProvider.saveTilesConfiguration( groundtruthTiles.toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter(
					URI.create( Utils.addFilenameSuffix( outputTilesConfigPath, "-groundtruth" ) )
				) );
		}
	}


	private static class ShiftedTestGenerator< T extends NativeType< T > & RealType< T > > extends AbstractTestGenerator< T >
	{
		private static final double shiftRatio = 0.15;

		private final Random rnd = new Random( 69997 ); // repeatable results

		ShiftedTestGenerator(
				final String n5Path,
				final String datasetPath,
				final String outputPath )
		{
			super( n5Path, datasetPath, outputPath );
		}

		@Override
		public void run() throws Exception
		{
			final DataProvider dataProvider = DataProviderFactory.createByURI( URI.create( n5Path ) );
			final N5Reader n5 = dataProvider.createN5Reader( URI.create( n5Path ), N5ExportMetadata.getGsonBuilder() );

			final N5ExportMetadataReader exportMetadata = N5ExportMetadata.openForReading( n5 );
			final VoxelDimensions voxelDimensions = exportMetadata.getPixelResolution( 0 );
			final double[] pixelResolution = new double[ voxelDimensions.numDimensions() ];
			voxelDimensions.dimensions( pixelResolution );

			final RandomAccessibleInterval< T > img = N5Utils.open( n5, datasetPath );
			final Interval cropInterval = getCropInterval( img, cropRatio );
			System.out.println( "crop dimensions = " + Arrays.toString( Intervals.dimensionsAsLongArray( cropInterval ) ) );

			final List< Interval > nonOverlappingIntervals = TileOperations.divideSpaceIgnoreSmaller( cropInterval, tileDimensions );
			final List< TileInfo > tiles = new ArrayList<>(), groundtruthTiles = new ArrayList<>();

			final String outputTileImagesDir = Paths.get( outputPath, "imgs" ).toString();
			Paths.get( outputTileImagesDir ).toFile().mkdirs();

			System.out.println( "intervals: " + nonOverlappingIntervals.size() );

			for ( final Interval nonOverlappingInterval : nonOverlappingIntervals )
			{
				final long[] offset = new long[ nonOverlappingInterval.numDimensions() ];
				for ( int d = 0; d < offset.length; ++d )
					offset[ d ] = Math.round( ( ( nonOverlappingInterval.min( d ) - cropInterval.min( d ) ) / tileDimensions.dimension( d ) ) * tileDimensions.dimension( d ) * overlapRatio );

				final long[] shift = new long[ nonOverlappingInterval.numDimensions() ];
				for ( int d = 0; d < shift.length; ++d )
				{
					final int maxShiftPixels = ( int ) Math.round( tileDimensions.dimension( d ) * shiftRatio );
					shift[ d ] = rnd.nextInt( maxShiftPixels ) - maxShiftPixels / 2;
				}
				System.out.println( "tile " + tiles.size() + ": shift=" + Arrays.toString( shift ) );

				final Interval overlappingInterval = IntervalsHelper.offset( nonOverlappingInterval, offset );
				final Interval shiftedInterval = IntervalsHelper.translate( overlappingInterval, shift );

				final TileInfo tile = new TileInfo( 3 );
				tile.setIndex( tiles.size() );

				tile.setStagePosition( Intervals.minAsDoubleArray( overlappingInterval ) );
				tile.setSize( Intervals.dimensionsAsLongArray( overlappingInterval ) );

				final RandomAccessibleInterval< T > cropImg = Views.interval( img, shiftedInterval );
				final ImagePlus tileImp = copyToImage( cropImg );
				final String tileImpPath = Paths.get( outputTileImagesDir, "tile_" + tile.getIndex() + ".tif" ).toString();
				IJ.saveAsTiff( tileImp, tileImpPath );

				tile.setFilePath( tileImpPath );
				tile.setType( ImageType.valueOf( tileImp.getType() ) );
				tile.setPixelResolution( pixelResolution.clone() );

				tiles.add( tile );

				final TileInfo groundtruthTile = tile.clone();
				groundtruthTile.setTransform( new Translation( Intervals.minAsDoubleArray( shiftedInterval ) ) );
				groundtruthTiles.add( groundtruthTile );
			}

			final String outputTilesConfigPath = Paths.get( outputPath, "tiles.json" ).toString();
			TileInfoJSONProvider.saveTilesConfiguration( tiles.toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter(
					URI.create( outputTilesConfigPath )
				) );

			TileInfoJSONProvider.saveTilesConfiguration( groundtruthTiles.toArray( new TileInfo[ 0 ] ), dataProvider.getJsonWriter(
					URI.create( Utils.addFilenameSuffix( outputTilesConfigPath, "-groundtruth" ) )
				) );
		}
	}
}
