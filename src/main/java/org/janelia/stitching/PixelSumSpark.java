package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.IterableInterval;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

/**
 * Combines two input tile configurations by summing pixel intensities for every pair of corresponding tiles.
 * It is assumed that tiles are in the correct order and have equal positions.
 *
 * @author Igor Pisarev
 */

public class PixelSumSpark implements Runnable, Serializable
{
	public static void main( final String[] args )
	{
		final PixelSumSpark driver = new PixelSumSpark( args[ 0 ], args[ 1 ] );
		driver.run();
	}

	private static final long serialVersionUID = -2854452029608640037L;

	private final transient String input1, input2;
	private transient JavaSparkContext sparkContext;

	public PixelSumSpark( final String input1, final String input2 )
	{
		this.input1 = input1;
		this.input2 = input2;
	}

	@Override
	public void run()
	{
		final ArrayList< TilePair > tilePairs = new ArrayList<>();
		try
		{
			final TileInfo[] tiles1 = TileInfoJSONProvider.loadTilesConfiguration( input1 );
			final TileInfo[] tiles2 = TileInfoJSONProvider.loadTilesConfiguration( input2 );

			validateConfigurations( tiles1, tiles2 );

			for ( int i = 0; i < tiles1.length; i++ )
				tilePairs.add( new TilePair( tiles1[ i ], tiles2[ i ] ) );
		}
		catch ( final IOException e )
		{
			System.out.println( "Aborted: " + e.getMessage() );
			e.printStackTrace();
			System.exit( 1 );
		}

		sparkContext = new JavaSparkContext( new SparkConf().setAppName( "PixelSum" ) );

		performPixelSumSpark( tilePairs );

		sparkContext.close();
		System.out.println( "done" );
	}

	private void performPixelSumSpark( final List< TilePair > tilePairs )
	{
		final String outFolder = new File( input1 ).getParent() + "/pixelsum";
		new File( outFolder ).mkdirs();

		final JavaRDD< TilePair > rdd = sparkContext.parallelize( tilePairs );
		final JavaRDD< TileInfo > task = rdd.map(
				new Function< TilePair, TileInfo >()
				{
					private static final long serialVersionUID = 2430838372859095279L;

					@Override
					public TileInfo call( final TilePair tilePair ) throws Exception
					{
						System.out.println( "Processing tiles " + tilePair.first().getIndex() + " and " + tilePair.second().getIndex() );
						final TileInfo outputTile = tilePair.first().clone();
						outputTile.setFilePath( outFolder + "/" + "tile" + tilePair.first().getIndex() + ".tif" );

						performPixelSum( tilePair, outputTile.getFilePath() );

						return outputTile;
					}
				});

		final List< TileInfo > tilesCombined = task.collect();

		System.out.println( "Obtained resulting tiles" );

		final TileInfo[] tilesArr = tilesCombined.toArray( new TileInfo[ 0 ] );
		try {
			TileInfoJSONProvider.saveTilesConfiguration( tilesArr, Utils.addFilenameSuffix( input1, "_sum" ) );
		} catch ( final IOException e ) {
			e.printStackTrace();
		}
	}

	private < T extends RealType< T > & NativeType< T > > void performPixelSum( final TilePair tilePair, final String outputFilePath ) throws Exception
	{
		final ImagePlus imp1 = IJ.openImage( tilePair.first().getFilePath() );
		final ImagePlus imp2 = IJ.openImage( tilePair.second().getFilePath() );

		Utils.workaroundImagePlusNSlices( imp1 );
		Utils.workaroundImagePlusNSlices( imp2 );

		final Img< T > in1 = ImagePlusImgs.from( imp1 );
		final Img< T > in2 = ImagePlusImgs.from( imp2 );

		final IterableInterval< T > iterIn1 = Views.flatIterable( in1 );
		final IterableInterval< T > iterIn2 = Views.flatIterable( in2 );

		final Cursor< T > cursorIn1 = iterIn1.cursor();
		final Cursor< T > cursorIn2 = iterIn2.cursor();

		// Create output image
		final long[] outSize = new long[ tilePair.first().numDimensions() ];
		for ( int d = 0; d < outSize.length; d++ )
			outSize[ d ] = Math.min( tilePair.first().getSize( d ), tilePair.second().getSize( d ) );
		final Img< T > out = new ImagePlusImgFactory< T >().create( outSize, ( T ) tilePair.first().getType().getType() );
		final IterableInterval< T > iterOut = Views.flatIterable( out );
		final Cursor< T > cursorOut = iterOut.cursor();

		while ( cursorIn1.hasNext() && cursorIn2.hasNext() && cursorOut.hasNext() )
			cursorOut.next().setReal( cursorIn1.next().getRealDouble() + cursorIn2.next().getRealDouble() );

		//if ( cursorIn1.hasNext() || cursorIn2.hasNext() || cursorOut.hasNext() )
		//	throw new Exception( "Different intervals for tiles " + tilePair.first().getIndex() + " and " + tilePair.second().getIndex() );

		final ImagePlus outImg = ImageJFunctions.wrap( out, "" );
		Utils.workaroundImagePlusNSlices( outImg );
		IJ.saveAsTiff( outImg, outputFilePath );

		imp1.close();
		imp2.close();
		outImg.close();
	}

	private void validateConfigurations( final TileInfo[] tiles1, final TileInfo[] tiles2 )
	{
		if ( tiles1.length != tiles2.length )
		{
			System.out.println( "Aborted: a number of tiles doesn't match" );
			System.exit( 2 );
		}

		for ( int i = 0; i < tiles1.length; i++ )
		{
			if ( tiles1[ i ].numDimensions() != tiles2[ i ].numDimensions() )
			{
				System.out.println( "Aborted: dimensionality doesn't match" );
				System.exit( 3 );
			}
		}
	}
}
