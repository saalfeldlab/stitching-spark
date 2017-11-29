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
import java.nio.file.StandardCopyOption;
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
		return Files.exists( getPath( uri ) );
	}

	@Override
	public void deleteFile( final URI uri ) throws IOException
	{
		Files.delete( getPath( uri ) );
	}

	@Override
	public void deleteFolder( final URI uri ) throws IOException
	{
		Files.walkFileTree( getPath( uri ), new SimpleFileVisitor< Path >()
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
	public void copyFile( final URI uriSrc, final URI uriDst ) throws IOException
	{
		Files.copy(
				getPath( uriSrc ),
				getPath( uriDst ),
				StandardCopyOption.REPLACE_EXISTING
			);
	}

	@Override
	public void moveFile( final URI uriSrc, final URI uriDst ) throws IOException
	{
		Files.move(
				getPath( uriSrc ),
				getPath( uriDst )
			);
	}

	@Override
	public InputStream getInputStream( final URI uri) throws IOException
	{
		return new FileInputStream( getPath( uri ).toFile() );
	}

	@Override
	public OutputStream getOutputStream( final URI uri ) throws IOException
	{
		createDirs( uri );
		return new FileOutputStream( getPath( uri ).toFile() );
	}

	@Override
	public ImagePlus loadImage( final URI uri ) throws IOException
	{
		return ImageImporter.openImage( getCanonicalPathString( uri ) );
	}

	@Override
	public void saveImage( final ImagePlus imp, final URI uri ) throws IOException
	{
		createDirs( uri );
		Utils.workaroundImagePlusNSlices( imp );
		IJ.saveAsTiff( imp, getCanonicalPathString( uri ) );
	}

	@Override
	public Reader getJsonReader( final URI uri ) throws IOException
	{
		return new FileReader( getPath( uri ).toFile() );
	}

	@Override
	public Writer getJsonWriter( final URI uri ) throws IOException
	{
		createDirs( uri );
		return new FileWriter( getPath( uri ).toFile() );
	}

	@Override
	public N5Reader createN5Reader( final URI baseUri ) throws IOException
	{
		return N5.openFSReader( getCanonicalPathString( baseUri ) );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri ) throws IOException
	{
		return N5.openFSWriter( getCanonicalPathString( baseUri ) );
	}

	@Override
	public N5Reader createN5Reader( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException
	{
		return N5.openFSReader( getCanonicalPathString( baseUri ), gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException
	{
		return N5.openFSWriter( getCanonicalPathString( baseUri ), gsonBuilder );
	}

	private static boolean createDirs( final URI uri )
	{
		return getPath( uri ).getParent().toFile().mkdirs();
	}

	private static Path getPath( final URI uri )
	{
		return uri.getScheme() != null ? Paths.get( uri ) : Paths.get( uri.toString() );
	}
	private static String getCanonicalPathString( final URI uri ) throws IOException
	{
		return getPath( uri ).toFile().getCanonicalPath();
	}
}
