package org.janelia.stitching.analysis;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.janelia.stitching.ImageType;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.janelia.util.ComparablePair;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.Views;

public class RescaleData {
	
	public static void main( String[] args ) throws Exception
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args [ 0 ] );
		final TileInfo[] deconTiles = TileInfoJSONProvider.loadTilesConfiguration( args [ 1 ] );
		final int tileIndex = 1;
		TileInfo tile = tiles[ tileIndex ];
		TileInfo deconTile = deconTiles[ tileIndex ];
		IJ.saveAsTiff( rescaleData( tile, deconTile ), Paths.get( args[0] ).getParent().toString()+"/"+ Utils.addFilenameSuffix( Paths.get(tile.getFilePath()).getFileName().toString(), "_rescaled" ) );
		System.out.println("Done");
	}
	
	private static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > ImagePlus rescaleData( final TileInfo tile, final TileInfo deconTile )
	{
		ImagePlus origImp = IJ.openImage(tile.getFilePath());
		Utils.workaroundImagePlusNSlices(origImp);
		final Img< T > origImg = ImagePlusImgs.from(origImp);
		final Cursor< T > origImgCursor = Views.flatIterable( origImg ).cursor();
		double origMin=Double.MAX_VALUE,origMax=-Double.MAX_VALUE;
		while ( origImgCursor.hasNext() )
		{
			double val = origImgCursor.next().getRealDouble();
			if (origMin>val)
				origMin=val;
			if (origMax<val)
				origMax=val;
		}
		System.out.println("origMin="+origMin+", origMax="+origMax);
		
		
		ImagePlus deconImp = IJ.openImage(deconTile.getFilePath());
		Utils.workaroundImagePlusNSlices(deconImp);
		
		final Img< U > deconImg = ImagePlusImgs.from(deconImp);
		final Cursor< U > deconImgCursor = Views.flatIterable( deconImg ).cursor();
		double deconMin=Double.MAX_VALUE,deconMax=-Double.MAX_VALUE;
		while ( deconImgCursor.hasNext() )
		{
			double val = deconImgCursor.next().getRealDouble();
			if (deconMin>val)
				deconMin=val;
			if (deconMax<val)
				deconMax=val;
		}
		System.out.println("deconMin="+deconMin+", deconMax="+deconMax);

		final Img< T > out = new ImagePlusImgFactory< T >().create( origImg, (T) ImageType.valueOf(origImp.getType()).getType().createVariable() );
		final Cursor< T > outCursor = Views.flatIterable( out ).cursor();
		deconImgCursor.reset();
		
		while ( outCursor.hasNext() || deconImgCursor.hasNext() )
			outCursor.next().setReal( Math.round( (deconImgCursor.next().getRealDouble()-deconMin)/(deconMax-deconMin) * (origMax-origMin) + origMin ) );
		
		ImagePlus res = ImageJFunctions.wrap(out, "");
		Utils.workaroundImagePlusNSlices(res);
		return res;
	}
}
