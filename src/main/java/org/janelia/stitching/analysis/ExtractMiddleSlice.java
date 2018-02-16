package org.janelia.stitching.analysis;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.ImageType;
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
import net.imglib2.view.Views;

public class ExtractMiddleSlice implements Serializable {

	private static final long serialVersionUID = -5089082519880731804L;

	private transient final JavaSparkContext sparkContext;
	private final String outFolder;

	public static void main( final String[] args ) throws Exception
	{
		new ExtractMiddleSlice( args[ 0 ] );
	}

	public ExtractMiddleSlice( final String input ) throws Exception
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( DataProviderFactory.createFSDataProvider().getJsonReader( URI.create( input ) ) );

		sparkContext = new JavaSparkContext( new SparkConf().setAppName( "ExtractMiddleSlice" ));
		outFolder = Paths.get(input).getParent().toString()+"/middle";
		new File(outFolder).mkdirs();

		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
		final JavaRDD< TileInfo > task = rdd.map(
				new Function< TileInfo, TileInfo >()
				{
					private static final long serialVersionUID = 4991255417353136684L;

					@Override
					public TileInfo call( final TileInfo tile ) throws Exception
					{
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

		final TileInfo[] middleSliceTiles = task.collect().toArray( new TileInfo[0] );
		TileInfoJSONProvider.saveTilesConfiguration( middleSliceTiles, DataProviderFactory.createFSDataProvider().getJsonWriter( URI.create( Utils.addFilenameSuffix( input, "_middle" ) ) ) );

		System.out.println("Done");
	}

	private < T extends RealType< T > & NativeType< T > > void extractMiddleSlice( final TileInfo tile, final String outPath )
	{
		final ImagePlus imp = IJ.openImage(tile.getFilePath());
		Utils.workaroundImagePlusNSlices(imp);

		final T type = (T) ImageType.valueOf(imp.getType()).getType();

		final long[] outputDim = tile.getSize().clone();
		outputDim[ 2 ] = 1;

		final Img< T > img = ImagePlusImgs.from(imp);
		final Img< T > out = new ImagePlusImgFactory< T >().create( outputDim, type.createVariable() );

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
