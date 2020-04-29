package org.janelia.dataaccess;

import ij.ImagePlus;
import org.janelia.saalfeldlab.n5.spark.util.TiffUtils;

import java.io.IOException;

/**
 * Allows to use n5-spark TIFF utilities with various backends (filesystem and cloud).
 */
public class MultiBackendTiffWriter implements TiffUtils.TiffWriter
{
    @Override
    public void saveTiff( final ImagePlus imp, final String outputPath, final TiffUtils.TiffCompression compression ) throws IOException
    {
        if ( compression != TiffUtils.TiffCompression.NONE )
            throw new UnsupportedOperationException( "Saving TIFF images with compression is not supported yet" );

        final DataProviderType dataProviderType = DataProviderFactory.detectType( outputPath );
        DataProviderFactory.create( dataProviderType ).saveImage( imp, outputPath );
    }

    @Override
    public void createDirs( final String path ) throws IOException
    {
        final DataProviderType dataProviderType = DataProviderFactory.detectType( path );
        DataProviderFactory.create( dataProviderType ).createFolder( path );
    }

    @Override
    public String combinePaths( final String basePath, final String other )
    {
        return PathResolver.get( basePath, other );
    }
}
