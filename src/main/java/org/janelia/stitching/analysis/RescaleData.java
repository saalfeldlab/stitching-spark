package org.janelia.stitching.analysis;

import java.nio.file.Paths;

import org.janelia.stitching.ImageType;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.img.imageplus.ImagePlusImgFactory;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

public class RescaleData {

	public static void main( final String[] args ) throws Exception
	{
		final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( args [ 0 ] );
		final TileInfo[] deconTiles = TileInfoJSONProvider.loadTilesConfiguration( args [ 1 ] );
		final int tileIndex = 1;
		final TileInfo tile = tiles[ tileIndex ];
		final TileInfo deconTile = deconTiles[ tileIndex ];
		IJ.saveAsTiff( rescaleData( tile, deconTile ), Paths.get( args[0] ).getParent().toString()+"/"+ Utils.addFilenameSuffix( Paths.get(tile.getFilePath()).getFileName().toString(), "_rescaled" ) );
		System.out.println("Done");
	}

	private static < T extends RealType< T > & NativeType< T >, U extends RealType< U > & NativeType< U > > ImagePlus rescaleData( final TileInfo tile, final TileInfo deconTile )
	{
		final ImagePlus origImp = IJ.openImage(tile.getFilePath());
		Utils.workaroundImagePlusNSlices(origImp);
		final Img< T > origImg = ImagePlusImgs.from(origImp);
		final Cursor< T > origImgCursor = Views.flatIterable( origImg ).cursor();
		double origMin=Double.MAX_VALUE,origMax=-Double.MAX_VALUE;
		while ( origImgCursor.hasNext() )
		{
			final double val = origImgCursor.next().getRealDouble();
			if (origMin>val)
				origMin=val;
			if (origMax<val)
				origMax=val;
		}
		System.out.println("origMin="+origMin+", origMax="+origMax);


		final ImagePlus deconImp = IJ.openImage(deconTile.getFilePath());
		Utils.workaroundImagePlusNSlices(deconImp);

		final Img< U > deconImg = ImagePlusImgs.from(deconImp);
		final Cursor< U > deconImgCursor = Views.flatIterable( deconImg ).cursor();
		double deconMin=Double.MAX_VALUE,deconMax=-Double.MAX_VALUE;
		while ( deconImgCursor.hasNext() )
		{
			final double val = deconImgCursor.next().getRealDouble();
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

		final ImagePlus res = ImageJFunctions.wrap(out, "");
		Utils.workaroundImagePlusNSlices(res);
		return res;
	}
}
