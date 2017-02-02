package org.janelia.stitching;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.janelia.util.ComparablePair;

import ij.IJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;


public class BackgroundTilesFiltering implements Serializable
{
	public static void main( final String[] args ) throws Exception
	{
		final BackgroundTilesFiltering driver = new BackgroundTilesFiltering( args[ 0 ] );
		driver.run();
		driver.shutdown();
		System.out.println("Done");
	}

	private static final long serialVersionUID = -2854452029608640037L;

	private final transient String input;
	private final transient JavaSparkContext sparkContext;
	private final transient TileInfo[] tiles;

	public BackgroundTilesFiltering( final String input ) throws Exception
	{
		this.input = input;
		tiles = TileInfoJSONProvider.loadTilesConfiguration(input);
		sparkContext = new JavaSparkContext( new SparkConf()
				.setAppName( "BackgroundTilesFiltering" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.registerKryoClasses( new Class[] { Double.class, ComparablePair.class, TileInfo.class } ) 
				);
	}

	public void shutdown()
	{
		if ( sparkContext != null )
			sparkContext.close();
	}
	
	public < T extends RealType< T > & NativeType< T > > void run() throws Exception
	{
		if ( Files.exists(Paths.get( Paths.get(input).getParent().toString()+"/"+Paths.get(input).getFileName().toString()+"_stats.txt") ))
		{
			Scanner sc = new Scanner( new File(Paths.get(input).getParent().toString()+"/"+Paths.get(input).getFileName().toString()+"_stats.txt" ) );
			int cnt_background = 0, processed = 0;
			double min_std = Double.MAX_VALUE;
			while( sc.hasNext() )
			{
				String[] tokens = sc.nextLine().split(" ");
				double std =Double.parseDouble(tokens[1]);
				min_std = Math.min(std, min_std);
				processed++;
				if ( std < 4 )
					cnt_background++;
			}
			System.out.println("min_std = " + min_std );
			System.out.println("processed="+processed + ",  cnt_background = " + cnt_background );
			return;
		}
			
		
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
		final JavaRDD< ComparablePair< Double, Double > > task = rdd.map(
				new Function< TileInfo, ComparablePair< Double, Double > >()
				{
					private static final long serialVersionUID = 2430838372859095279L;

					@Override
					public ComparablePair< Double, Double > call( final TileInfo tile )
					{
						final ImagePlus imp = IJ.openImage(tile.getFilePath());
						Utils.workaroundImagePlusNSlices(imp);
						final Img< T > img = ImagePlusImgs.from(imp);
						final Cursor< T > cursor = Views.iterable( img ).cursor();
						double sum = 0, sum2 = 0;
						while ( cursor.hasNext() )
						{
							final double val = cursor.next().getRealDouble();
							sum += val;
							sum2 += val*val;
						}
						final double mean = sum / img.size();
						final double std = Math.sqrt( sum2 / img.size() - mean * mean );
						return new ComparablePair<>( mean, std );
					}
				});

		final List< ComparablePair< Double, Double > > stats = task.collect();
		final PrintWriter writer = new PrintWriter( Paths.get(input).getParent().toString()+"/"+Paths.get(input).getFileName().toString()+"_stats.txt" );
		for ( final ComparablePair< Double, Double > stat : stats )
			writer.println( String.format("%f %f", stat.a, stat.b ) );
		writer.close();
	}
	
	/*public < T extends RealType< T > & NativeType< T > > void run() throws Exception
	{
		final JavaRDD< TileInfo > rdd = sparkContext.parallelize( Arrays.asList( tiles ) );
		final JavaRDD< TreeMap< Integer, Integer > > task = rdd.map(
				new Function< TileInfo, TreeMap< Integer, Integer > >()
				{
					private static final long serialVersionUID = 2430838372859095279L;

					@Override
					public TreeMap< Integer, Integer > call( final TileInfo tile ) throws Exception
					{
						final ImagePlus imp = IJ.openImage(tile.getFilePath());
						Utils.workaroundImagePlusNSlices(imp);
						final Img< T > img = ImagePlusImgs.from(imp);
						final Cursor< T > cursor = Views.iterable( img ).cursor();
						final TreeMap< Integer, Integer > ret = new TreeMap<>();
						while ( cursor.hasNext() )
						{
							final int val = (int) cursor.next().getRealDouble();
							ret.put( val, ret.getOrDefault( val, 0 ) + 1 );
						}
						return ret;
					}
				});

		final List< TreeMap< Integer, Integer > > histograms = task.collect();
		final PrintWriter writer = new PrintWriter( Paths.get(input).getParent().toString()+"/"+Paths.get(input).getFileName().toString()+"_range.txt" );
		for ( final TreeMap< Integer, Integer > hist : histograms )
		{
			double mean = 0;
			long count = 0;
			for ( final Entry< Integer, Integer > entry : hist.entrySet() )
			{
				mean += (double)entry.getKey() * entry.getValue();
				count += entry.getValue();
			}
			mean /= count;
			
			double std = 0;
			for ( final Entry< Integer, Integer > entry : hist.entrySet() )
				std += Math.pow( entry.getKey() - mean, 2 ) * entry.getValue();
			std = Math.sqrt( std / count );
			
			writer.println( String.format("%d %d %f %f", hist.firstKey(), hist.lastKey(), mean, std ) );
		}
		writer.close();
	}*/
}
