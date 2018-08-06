package org.janelia.dataaccess.fs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
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
	public boolean fileExists( final String link )
	{
		return Files.exists( Paths.get( link ) );
	}

	@Override
	public void createFolder( final String link ) throws IOException
	{
		createDirs( Paths.get( link ) );
	}

	@Override
	public void deleteFile( final String link ) throws IOException
	{
		Files.delete( Paths.get( link ) );
	}

	@Override
	public void deleteFolder( final String link ) throws IOException
	{
		Files.walkFileTree( Paths.get( link ), new SimpleFileVisitor< Path >()
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
	public void copyFile( final String srcLink, final String dstLink ) throws IOException
	{
		Files.copy(
				Paths.get( srcLink ),
				Paths.get( dstLink ),
				StandardCopyOption.REPLACE_EXISTING
			);
	}

	@Override
	public void copyFolder( final String srcLink, final String dstLink ) throws IOException
	{
		copyFile( srcLink, dstLink );
	}

	@Override
	public void moveFile( final String srcLink, final String dstLink ) throws IOException
	{
		Files.move(
				Paths.get( srcLink ),
				Paths.get( dstLink )
			);
	}

	@Override
	public void moveFolder( final String srcLink, final String dstLink ) throws IOException
	{
		moveFile( srcLink, dstLink );
	}

	@Override
	public InputStream getInputStream( final String link ) throws IOException
	{
		return new FileInputStream( link );
	}

	@Override
	public OutputStream getOutputStream( final String link ) throws IOException
	{
		createDirs( Paths.get( link ).getParent() );
		return new FileOutputStream( link );
	}

	@Override
	public ImagePlus loadImage( final String link ) throws IOException
	{
		return ImageImporter.openImage( getCanonicalPathString( link ) );
	}

	@Override
	public void saveImage( final ImagePlus imp, final String link ) throws IOException
	{
		createDirs( Paths.get( link ).getParent() );
		Utils.workaroundImagePlusNSlices( imp );
		IJ.saveAsTiff( imp, getCanonicalPathString( link ) );
	}

	@Override
	public Reader getJsonReader( final String link ) throws IOException
	{
		return new FileReader( link );
	}

	@Override
	public Writer getJsonWriter( final String link ) throws IOException
	{
		createDirs( Paths.get( link ).getParent() );
		return new FileWriter( link );
	}

	@Override
	public N5Reader createN5Reader( final String baseLink ) throws IOException
	{
		return new N5FSReader( getCanonicalPathString( baseLink ) );
	}

	@Override
	public N5Writer createN5Writer( final String baseLink ) throws IOException
	{
		return new N5FSWriter( getCanonicalPathString( baseLink ) );
	}

	@Override
	public N5Reader createN5Reader( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5FSReader( getCanonicalPathString( baseLink ), gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5FSWriter( getCanonicalPathString( baseLink ), gsonBuilder );
	}

	private static boolean createDirs( final Path path )
	{
		return path.toFile().mkdirs();
	}

	private static String getCanonicalPathString( final String link ) throws IOException
	{
		return Paths.get( link ).toFile().getCanonicalPath();
	}
}
