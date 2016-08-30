package org.janelia.stitching;

import static bdv.img.hdf5.Util.reorder;

import java.io.File;
import java.util.Arrays;

import bdv.img.h5.H5Utils;
import ch.systemsx.cisd.base.mdarray.MDShortArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5ShortWriter;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Dimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

/**
 * Saves a collection of {@link TileInfo} objects as a HDF5 dataset.
 *
 * @author Igor Pisarev
 */

public class Hdf5Creator
{
	final static private int[] cellSize = new int[]{ 64, 64, 64 };

	static public void createSignedShortDataset(
			final Dimensions dimensions,
			final IHDF5Writer writer,
			final String dataset,
			final int[] cellDimensions )
	{
		final long[] size = Intervals.dimensionsAsLongArray( dimensions );
		final IHDF5ShortWriter int16Writer = writer.uint16();
		if ( !writer.exists( dataset ) )
			int16Writer.createMDArray(
					dataset,
					reorder( size ),
					reorder( cellDimensions ),
					HDF5IntStorageFeatures.INT_AUTO_SCALING_DEFLATE );
	}

	static public void saveUnsignedShort(
			final RandomAccessibleInterval< UnsignedShortType > sourceBlock,
			final IHDF5ShortWriter int16Writer,
			final String dataset,
			final int[] cellDimensions )
	{
		final int n = sourceBlock.numDimensions();

		final long[] offset = new long[ n ];
		final long[] translatedOffset = new long[ n ];
		final long[] sourceCellDimensions = new long[ n ];
		for ( int d = 0; d < n; )
		{
			for ( int k = 0; k < n; ++k )
				translatedOffset[ k ] = offset[ k ] + sourceBlock.min( k );

			H5Utils.cropCellDimensions( sourceBlock, offset, cellDimensions, sourceCellDimensions );
			final RandomAccessibleInterval< UnsignedShortType > sourceBlockBlock = Views.offsetInterval( sourceBlock, translatedOffset, sourceCellDimensions );
			final MDShortArray targetCell = new MDShortArray( reorder( sourceCellDimensions ) );

			int i = 0;
			for ( final UnsignedShortType t : Views.flatIterable( sourceBlockBlock ) )
				targetCell.set( UnsignedShortType.getCodedSignedShort( t.get() ), i++ );

			int16Writer.writeMDArrayBlockWithOffset( dataset, targetCell, reorder( translatedOffset ) );

			for ( d = 0; d < n; ++d )
			{
				offset[ d ] += cellDimensions[ d ];
				if ( offset[ d ] < sourceBlock.dimension( d ) )
					break;
				else
					offset[ d ] = 0;
			}
		}
	}

	public static void createHdf5( final TileInfo[] tiles, final String out )
	{
		final String datasetName = "/volumes/raw";
		final File outFile = new File( out );
		final IHDF5Writer writer = HDF5Factory.open( outFile );

		final Boundaries space = TileOperations.getCollectionBoundaries( tiles );
		createSignedShortDataset( space, writer, datasetName, cellSize  );
		System.out.println( "space min=" + Arrays.toString( space.getMin() ) + " dimensions=" + Arrays.toString( space.getDimensions() ) );

		final IHDF5ShortWriter int16Writer = writer.int16();

		for ( final TileInfo tile : tiles )
		{
			System.out.println( "Open tile " + tile.getIndex() );
			System.out.println( "position=" + Arrays.toString( tile.getPosition() ) );

			final ImagePlus img = IJ.openImage( tile.getFilePath() );
			final Img< UnsignedShortType > in = ImageJFunctions.wrapShort( img );

			final RandomAccessibleInterval< UnsignedShortType > view = Views.translate(
					in,
					Math.round( tile.getPosition( 0 ) - space.min( 0 ) ),
					Math.round( tile.getPosition( 1 ) - space.min( 1 ) ),
					Math.round( tile.getPosition( 2 ) - space.min( 2 ) ) );

			System.out.println( "Save tile " + tile.getIndex() );

			saveUnsignedShort( view, int16Writer, datasetName, cellSize);
		}

		writer.close();
	}
}
