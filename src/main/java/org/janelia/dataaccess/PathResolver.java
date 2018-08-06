package org.janelia.dataaccess;

import java.net.URI;
import java.nio.file.Paths;

public class PathResolver
{
	/**
	 * Combines base and relative paths. Supports both links and filesystem paths.
	 *
	 * @param basePath
	 * @param relativePaths
	 * @return
	 */
	public static String get( final String basePath, final String... relativePaths )
	{
		if ( CloudURI.isCloudURI( basePath ) )
		{
			URI combinedUri = URI.create( basePath );
			for ( final String relativePath : relativePaths )
			{
				// base URI has to end with a slash, otherwise relative path replaces the last part of the base URI
				if ( !combinedUri.toString().endsWith( "/" ) )
					combinedUri = URI.create( combinedUri.toString() + "/" );

				combinedUri = combinedUri.resolve( relativePath );
			}
			return combinedUri.toString();
		}
		else
		{
			return Paths.get( basePath, relativePaths ).toString();
		}
	}

	public static String getParent( final String path )
	{
		if ( CloudURI.isCloudURI( path ) )
		{
			// if a given path is a link, need to use URI functionality because Paths.get() in this case swallows the scheme
			final URI uri = URI.create( path );

			// based on https://stackoverflow.com/a/10159309
			final URI parentUri = uri.getPath().endsWith( "/" ) ? uri.resolve( ".." ) : uri.resolve( "." );
			return parentUri.toString();
		}
		else
		{
			return Paths.get( path ).getParent().toString();
		}
	}

	public static String getFileName( final String path )
	{
		// no need to preserve scheme, Paths.get() works in this case
		return Paths.get( path ).getFileName().toString();
	}
}
