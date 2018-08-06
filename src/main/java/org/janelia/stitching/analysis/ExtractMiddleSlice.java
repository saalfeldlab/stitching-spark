package org.janelia.stitching.analysis;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class ExtractMiddleSlice
{
	public static void main( final String[] args ) throws Exception
	{
		final String input = args[ 0 ];
		final String outFolder = args.length > 1 ? args[ 1 ] : Paths.get( Paths.get( input ).getParent().toString(), "middle" ).toString();
		new File( outFolder ).mkdirs();

		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( DataProviderFactory.createFSDataProvider().getJsonReader( input ) );
		final List< TileInfo > middleSliceTiles;

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf().setAppName( "ExtractMiddleSlice" ) ) )
		{
			final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
			final JavaRDD< TileInfo > task = rdd.map(
					new Function< TileInfo, TileInfo >()
					{
						private static final long serialVersionUID = 4991255417353136684L;

						@Override
						public TileInfo call( final TileInfo tile ) throws Exception
						{
							if ( !Files.exists( Paths.get( tile.getFilePath() ) ) )
								return null;

							final String outPath = outFolder+"/MID_"+Paths.get(tile.getFilePath()).getFileName().toString();
							extractMiddleSlice( tile, outPath );

							final TileInfo middleSliceTile = new TileInfo(tile.numDimensions() - 1);
							middleSliceTile.setIndex( tile.getIndex() );
							middleSliceTile.setType( tile.getType() );
							middleSliceTile.setPosition( new double[] { tile.getPosition(0), tile.getPosition(1) } );
							middleSliceTile.setSize( new long[] { tile.getSize(0), tile.getSize(1) } );
							middleSliceTile.setFilePath( outPath );

							return middleSliceTile;
						}
					});

			middleSliceTiles = new ArrayList<>( task.collect() );
			middleSliceTiles.removeAll( Collections.singleton( null ) );
		}

		TileInfoJSONProvider.saveTilesConfiguration(
				middleSliceTiles.toArray( new TileInfo[ 0 ] ),
				DataProviderFactory.createFSDataProvider().getJsonWriter( Utils.addFilenameSuffix( input, "_middle" ) )
			);
		System.out.println("Done");
	}

	private static < T extends RealType< T > & NativeType< T > > void extractMiddleSlice( final TileInfo tile, final String outPath )
	{
		final ImagePlus imp = IJ.openImage(tile.getFilePath());
		Utils.workaroundImagePlusNSlices(imp);

		final long[] outputDim = tile.getSize().clone();
		outputDim[ 2 ] = 1;

		final Img< T > img = ImagePlusImgs.from(imp);
		final Img< T > out = new ImagePlusImgFactory< T >().create( outputDim, Util.getTypeFromInterval( img ) );

		final int slice = (int)tile.getSize(2) / 2;
		final Interval interval = new FinalInterval( new long[] { 0, 0, slice }, new long[] { tile.getSize(0)-1, tile.getSize(1)-1, slice } );

		final Cursor< T > imgCursor = Views.flatIterable( Views.offsetInterval(img, interval) ).cursor();
		final Cursor< T > outCursor = Views.flatIterable( out ).cursor();

		while ( outCursor.hasNext() || imgCursor.hasNext() )
			outCursor.next().set( imgCursor.next() );

		final ImagePlus res = ImageJFunctions.wrap(out, "");
		Utils.workaroundImagePlusNSlices(res);
		IJ.saveAsTiff(res, outPath);
	}
}
