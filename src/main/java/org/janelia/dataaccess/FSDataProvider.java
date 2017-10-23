package org.janelia.dataaccess;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.janelia.saalfeldlab.n5.N5;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.stitching.Utils;
import org.janelia.util.ImageImporter;

import com.google.gson.GsonBuilder;

import ij.IJ;
import ij.ImagePlus;

/**
 * Provides filesystem-based access to data stored on a local or network drive.
 *
 * @author Igor Pisarev
 */
class FSDataProvider implements DataProvider
{
	@Override
	public DataProviderType getType()
	{
		return DataProviderType.FILESYSTEM;
	}

	@Override
	public URI getUri( final String path ) throws URISyntaxException
	{
		return new URI( path );
	}

	@Override
	public boolean fileExists( final URI uri )
	{
		return Files.exists( Paths.get( uri ) );
	}

	@Override
	public void deleteFile( final URI uri ) throws IOException
	{
		Files.delete( Paths.get( uri.toString() ) );
	}

	@Override
	public void deleteFolder( final URI uri ) throws IOException
	{
		Files.walkFileTree( Paths.get( uri.toString() ), new SimpleFileVisitor< Path >()
			{
			   @Override
			   public FileVisitResult visitFile( final Path file, final BasicFileAttributes attrs ) throws IOException
			   {
			       Files.delete( file );
			       return FileVisitResult.CONTINUE;
			   }

			   @Override
			   public FileVisitResult postVisitDirectory( final Path dir, final IOException exc ) throws IOException
			   {
			       Files.delete( dir );
			       return FileVisitResult.CONTINUE;
			   }
			}
		);
	}

	@Override
	public InputStream getInputStream( final URI uri) throws IOException
	{
		return new FileInputStream( uri.toString() );
	}

	@Override
	public OutputStream getOutputStream( final URI uri ) throws IOException
	{
		createDirs( uri );
		return new FileOutputStream( uri.toString() );
	}

	@Override
	public ImagePlus loadImage( final URI uri )
	{
		return ImageImporter.openImage( uri.toString() );
	}

	@Override
	public void saveImage( final ImagePlus imp, final URI uri )
	{
		createDirs( uri );
		Utils.workaroundImagePlusNSlices( imp );
		IJ.saveAsTiff( imp, uri.toString() );
	}

	@Override
	public Reader getJsonReader( final URI uri ) throws IOException
	{
		return new FileReader( uri.toString() );
	}

	@Override
	public Writer getJsonWriter( final URI uri ) throws IOException
	{
		createDirs( uri );
		return new FileWriter( uri.toString() );
	}

	@Override
	public N5Reader createN5Reader( final URI baseUri )
	{
		return N5.openFSReader( baseUri.toString() );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri ) throws IOException
	{
		return N5.openFSWriter( baseUri.toString() );
	}

	@Override
	public N5Reader createN5Reader( final URI baseUri, final GsonBuilder gsonBuilder )
	{
		return N5.openFSReader( baseUri.toString(), gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException
	{
		return N5.openFSWriter( baseUri.toString(), gsonBuilder );
	}

	private static boolean createDirs( final URI uri )
	{
		return Paths.get( uri.toString() ).getParent().toFile().mkdirs();
	}
}
