package org.janelia.stitching;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.TreeMap;

import org.janelia.util.MultithreadedExecutor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import net.imglib2.img.list.ListImg;


public class HistogramsConversion implements Serializable
{
	private static final long serialVersionUID = -8987192045944606043L;

	private static final double WINDOW_POINTS_PERCENT = 0.25;
	private static final double INTERPOLATION_LAMBDA = 0.1;

	private enum ModelType
	{
		AffineModel,
		FixedScalingAffineModel,
		FixedTranslationAffineModel
	}
	private enum RegularizerModelType
	{
		AffineModel,
		IdentityModel
	}

	private final String subfolder = "hierarchical";
	private final String inputFilepath, histogramsPath, solutionPath;

	private transient TileInfo[] tiles;
	private long[] fullSize;

	public static void main( final String[] args ) throws Exception
	{
		final HistogramsConversion driver = new HistogramsConversion( args[ 0 ] );
		driver.run();
		driver.shutdown();
		System.out.println("Done");
	}


	public HistogramsConversion( final String inputFilepath )
	{
		this.inputFilepath = inputFilepath;

		final String outputPath = Paths.get( inputFilepath ).getParent().toString() + "/" + subfolder;
		histogramsPath = outputPath + "/" + "histograms";
		solutionPath   = outputPath + "/" + "solution";
	}

	public void shutdown()
	{
	}


	public <
		V extends TreeMap< Short, Integer > >
	void run() throws Exception
	{
		try {
			tiles = TileInfoJSONProvider.loadTilesConfiguration( inputFilepath );
		} catch (final IOException e) {
			e.printStackTrace();
			return;
		}

		fullSize = getMinSize( tiles );

		final MultithreadedExecutor pool = new MultithreadedExecutor( 3 );

		pool.run( slice ->
			{
				final TreeMap< Short, Integer >[] sliceArr = readSliceHistogramsArrayFromDisk(0, slice+1);
				try {
					saveSliceHistogramsToDisk(0, slice+1, sliceArr);
				} catch (final Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			, getNumSlices() );

		pool.shutdown();
	}

	private < V extends TreeMap< Short, Integer > >void saveSliceHistogramsToDisk( final int scale, final int slice, final V[] hist ) throws Exception
	{
		final String path = histogramsPath + "-newkryo/" + scale + "/" + slice + ".hist";

		new File( Paths.get( path ).getParent().toString() ).mkdirs();

		final OutputStream os = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream( path )
						)
				);

		//final Kryo kryo = kryoSerializer.newKryo();
		final Kryo kryo = new Kryo();
		final MapSerializer serializer = new MapSerializer();
		serializer.setKeysCanBeNull( false );
		serializer.setKeyClass( Short.class, kryo.getSerializer( Short.class ) );
		serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
		kryo.register( TreeMap.class, serializer );

		//try ( final Output output = kryoSerializer.newKryoOutput() )
		//{
		//	output.setOutputStream( os );
		try ( final Output output = new Output( os ) )
		{
			kryo.writeClassAndObject( output, hist );
		}
	}
	/*private < V extends TreeMap< Short, Integer > >void saveSliceHistogramsToDisk( final int scale, final int slice, final V[] hist ) throws Exception
	{
		System.out.println( "Saving slice " + slice );
		final String path = histogramsPath + "-javaser/" + scale + "/" + slice + ".hist";

		new File( Paths.get( path ).getParent().toString() ).mkdirs();

		final FileOutputStream fout = new FileOutputStream(path);
		final ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(hist);
		oos.close();
	}*/


	private < V extends TreeMap< Short, Integer > > ListImg< V > readSliceHistogramsFromDisk( final int slice )
	{
		return new ListImg<>( Arrays.asList( readSliceHistogramsArrayFromDisk( 0, slice ) ), new long[] { fullSize[0],fullSize[1] } );
	}
	private < V extends TreeMap< Short, Integer > > V[] readSliceHistogramsArrayFromDisk( final int scale, final int slice )
	{
		System.out.println( "Loading slice " + slice );
		final String path = histogramsPath + "-javaser/" + scale + "/" + slice + ".hist";

		if ( !Files.exists(Paths.get(path)) )
			return null;

		try
		{
			final InputStream is = new DataInputStream(
					new BufferedInputStream(
							new FileInputStream( path )
							)
					);

			try ( final ObjectInputStream ois = new ObjectInputStream( is ) )
			{
				return ( V[] ) ois.readObject();
			}
			catch (final ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		catch ( final IOException e )
		{
			e.printStackTrace();
			return null;
		}
	}
	/*private < V extends TreeMap< Short, Integer > > V[] readSliceHistogramsArrayFromDisk( final int scale, final int slice )
	{
		System.out.println( "Loading slice " + slice );
		final String path = histogramsPath + "-oldkryo/" + scale + "/" + slice + ".hist";

		if ( !Files.exists(Paths.get(path)) )
			return null;

		try
		{
			final InputStream is = new DataInputStream(
					new BufferedInputStream(
							new FileInputStream( path )
							)
					);

			//final Kryo kryo = kryoSerializer.newKryo();
			final Kryo kryo = new Kryo();

			final MapSerializer serializer = new MapSerializer();
			serializer.setKeysCanBeNull( false );
			serializer.setKeyClass( Short.class, kryo.getSerializer( Short.class ) );
			serializer.setValueClass( Integer.class, kryo.getSerializer( Integer.class) );
			kryo.register( TreeMap.class, serializer );
			kryo.register( TreeMap[].class );

			try ( final Input input = new Input( is ) )
			{
				return ( V[] ) kryo.readClassAndObject( input );
			}
		}
		catch ( final IOException e )
		{
			e.printStackTrace();
			return null;
		}
	}*/


	private int getNumSlices()
	{
		return ( int ) ( fullSize.length == 3 ? fullSize[ 2 ] : 1 );
	}

	private long[] getMinSize( final TileInfo[] tiles )
	{
		final long[] minSize = tiles[ 0 ].getSize().clone();
		for ( final TileInfo tile : tiles )
			for ( int d = 0; d < minSize.length; d++ )
				if (minSize[ d ] > tile.getSize( d ))
					minSize[ d ] = tile.getSize( d );
		return minSize;
	}
}
