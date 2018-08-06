package org.janelia.dataaccess.fs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
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
public class FSDataProvider implements DataProvider
{
	@Override
	public DataProviderType getType()
	{
		return DataProviderType.FILESYSTEM;
	}

	@Override
	public URI getUri( final String path ) throws URISyntaxException
	{
		try
		{
			return new URI( URLEncoder.encode( path, StandardCharsets.UTF_8.name() ) );
		}
		catch ( final UnsupportedEncodingException e )
		{
			throw new URISyntaxException( path, e.getMessage() );
		}
	}

	@Override
	public boolean fileExists( final URI uri )
	{
		return Files.exists( getPath( uri ) );
	}

	@Override
	public void createFolder( final URI uri ) throws IOException
	{
		createDirs( getPath( uri ) );
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
	public void copyFolder( final URI uriSrc, final URI uriDst ) throws IOException
	{
		copyFile( uriSrc, uriDst );
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
	public void moveFolder( final URI uriSrc, final URI uriDst ) throws IOException
	{
		moveFile( uriSrc, uriDst );
	}

	@Override
	public InputStream getInputStream( final URI uri) throws IOException
	{
		return new FileInputStream( getPath( uri ).toFile() );
	}

	@Override
	public OutputStream getOutputStream( final URI uri ) throws IOException
	{
		createDirs( getPath( uri ).getParent() );
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
		createDirs( getPath( uri ).getParent() );
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
		createDirs( getPath( uri ).getParent() );
		return new FileWriter( getPath( uri ).toFile() );
	}

	@Override
	public N5Reader createN5Reader( final URI baseUri ) throws IOException
	{
		return new N5FSReader( getCanonicalPathString( baseUri ) );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri ) throws IOException
	{
		return new N5FSWriter( getCanonicalPathString( baseUri ) );
	}

	@Override
	public N5Reader createN5Reader( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5FSReader( getCanonicalPathString( baseUri ), gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5FSWriter( getCanonicalPathString( baseUri ), gsonBuilder );
	}

	private static Path getPath( final URI uri )
	{
		try
		{
			return uri.getScheme() != null ? Paths.get( uri ) : Paths.get( URLDecoder.decode( uri.toString(), StandardCharsets.UTF_8.name() ) );
		}
		catch ( final UnsupportedEncodingException e )
		{
			throw new IllegalArgumentException( e.getMessage(), e );
		}
	}

	private static boolean createDirs( final Path path )
	{
		return path.toFile().mkdirs();
	}

	private static String getCanonicalPathString( final URI uri ) throws IOException
	{
		return getPath( uri ).toFile().getCanonicalPath();
	}
}
