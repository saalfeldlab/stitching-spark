package org.janelia.stitching.analysis;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.janelia.util.TiffSliceReader;

import ij.IJ;

public class ExtractSlice
{
	public static void main( final String[] args ) throws IOException
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final String input = args[ 0 ];
		final int slice = Integer.parseInt( args[ 1 ] );

		try ( final JavaSparkContext sparkContext = new JavaSparkContext( new SparkConf().setAppName( "ExtractSlice" ) ) )
		{
			final String outFolder = Paths.get( input ).getParent().toString() + "/slice" + slice;
			new File(outFolder).mkdirs();

			final TileInfo[] tiles = TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( input ) );
			final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
			final TileInfo[] sliceTiles = rdd.map( tile ->
				{
					final String outPath = outFolder + String.format( "/Slice%d_", slice ) + Paths.get( tile.getFilePath() ).getFileName().toString();
					IJ.saveAsTiff( TiffSliceReader.readSlice( tile.getFilePath(), slice ), outPath );

					final TileInfo sliceTile = new TileInfo( tile.numDimensions() - 1 );
					sliceTile.setIndex( tile.getIndex() );
					sliceTile.setType( tile.getType() );
					sliceTile.setPosition( new double[] { tile.getPosition( 0 ), tile.getPosition( 1 ) } );
					sliceTile.setSize( new long[] { tile.getSize( 0 ), tile.getSize( 1 ) } );
					sliceTile.setFilePath( outPath );
					return sliceTile;
				}
			).collect().toArray( new TileInfo[ 0 ] );

			TileInfoJSONProvider.saveTilesConfiguration( sliceTiles, dataProvider.getJsonWriter( Utils.addFilenameSuffix( input, "_slice" + slice ) ) );
		}

		System.out.println("Done");
	}
}
