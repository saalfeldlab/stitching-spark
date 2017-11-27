package org.janelia.dataaccess;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3;
import org.janelia.stitching.Utils;
import org.janelia.util.ImageImporter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.gson.GsonBuilder;

import ij.IJ;
import ij.ImagePlus;

/**
 * Provides access to data stored on Amazon Web Services S3 storage.
 *
 * @author Igor Pisarev
 */
class AmazonS3DataProvider implements DataProvider
{
	private class S3ObjectOutputStream extends ByteArrayOutputStream
	{
		private final AmazonS3URI uri;
		private boolean closed;

		public S3ObjectOutputStream( final AmazonS3URI uri )
		{
	        this.uri = uri;
	    }

		@Override
		public void close() throws IOException
		{
			if ( !closed )
			{
				super.flush();
				final byte[] bytes = toByteArray();

				final ObjectMetadata objectMetadata = new ObjectMetadata();
				objectMetadata.setContentLength( bytes.length );
				try ( final InputStream data = new ByteArrayInputStream( bytes ) )
				{
					s3.putObject( uri.getBucket(), uri.getKey(), data, objectMetadata );
				}

				super.close();
				closed = true;
			}
		}
	}

	private static final String s3Protocol = "s3";

	private final AmazonS3 s3;
	private final TransferManager s3TransferManager;

	public AmazonS3DataProvider( final AmazonS3 s3 )
	{
		this.s3 = s3;
		s3TransferManager = TransferManagerBuilder.standard().withS3Client( s3 ).build();
	}

	@Override
	public DataProviderType getType()
	{
		return DataProviderType.AMAZON_S3;
	}

	@Override
	public URI getUri( final String path ) throws URISyntaxException
	{
		final URI uri = new URI( path );
		if ( s3Protocol.equalsIgnoreCase( uri.getScheme() ) )
			return uri;
		return new URI( s3Protocol, null, path );
	}

	@Override
	public boolean fileExists( final URI uri ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( uri );
		return s3.doesObjectExist( s3Uri.getBucket(), s3Uri.getKey() );
	}

	@Override
	public void deleteFile( final URI uri ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( uri );
		s3.deleteObject( s3Uri.getBucket(), s3Uri.getKey() );
	}

	@Override
	public void deleteFolder( final URI uri ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( uri );
		final String prefix = s3Uri.getKey().endsWith( "/" ) ? s3Uri.getKey() : s3Uri.getKey() + "/";
		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName( s3Uri.getBucket() )
				.withPrefix( prefix );
		ListObjectsV2Result objectsListing;
		do
		{
			objectsListing = s3.listObjectsV2( listObjectsRequest );
			final List< String > objectsToDelete = new ArrayList<>();
			for ( final S3ObjectSummary object : objectsListing.getObjectSummaries() )
				objectsToDelete.add(object.getKey());

			if ( !objectsToDelete.isEmpty() )
			{
				s3.deleteObjects( new DeleteObjectsRequest( s3Uri.getBucket() )
						.withKeys( objectsToDelete.toArray( new String[objectsToDelete.size() ] ) )
					);
			}
			listObjectsRequest.setContinuationToken( objectsListing.getNextContinuationToken() );
		}
		while ( objectsListing.isTruncated() );
	}

	@Override
	public void copyFile( final URI uriSrc, final URI uriDst ) throws IOException
	{
		final AmazonS3URI s3UriSrc = decodeS3Uri( uriSrc );
		final AmazonS3URI s3UriDst = decodeS3Uri( uriDst );
		s3.copyObject(
				s3UriSrc.getBucket(), s3UriSrc.getKey(),
				s3UriDst.getBucket(), s3UriDst.getKey()
			);
	}

	@Override
	public void moveFile( final URI uriSrc, final URI uriDst ) throws IOException
	{
		copyFile( uriSrc, uriDst );
		deleteFile( uriSrc );
	}

	@Override
	public InputStream getInputStream( final URI uri ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( uri );
		return s3.getObject( s3Uri.getBucket(), s3Uri.getKey() ).getObjectContent();
	}

	@Override
	public OutputStream getOutputStream( final URI uri ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( uri );
		return new S3ObjectOutputStream( s3Uri );
	}

	@Override
	public ImagePlus loadImage( final URI uri )
	{
		final String uriStr = uri.toString();
		if ( uriStr.endsWith( ".tif" ) || uriStr.endsWith( ".tiff" ) )
			return ImageImporter.openImage( uriStr );
		throw new NotImplementedException( "Only TIFF images are supported at the moment" );
	}

	@Override
	public void saveImage( final ImagePlus imp, final URI uri ) throws IOException
	{
		Utils.workaroundImagePlusNSlices( imp );
		// Need to save as a local TIFF file and then upload to S3. IJ does not provide a way to convert ImagePlus to TIFF byte array.
		Path tempPath = null;
		try
		{
			tempPath = Files.createTempFile( null, ".tif" );
			IJ.saveAsTiff( imp, tempPath.toString() );

			final AmazonS3URI s3Uri = decodeS3Uri( uri );
			final Upload s3Upload = s3TransferManager.upload( s3Uri.getBucket(), s3Uri.getKey(), tempPath.toFile() );
			try
			{
				s3Upload.waitForCompletion();
			}
			catch ( final InterruptedException e )
			{
				e.printStackTrace();
			}
		}
		finally
		{
			if ( tempPath != null )
				tempPath.toFile().delete();
		}
	}

	@Override
	public Reader getJsonReader( final URI uri ) throws IOException
	{
		return new InputStreamReader( getInputStream( uri ) );
	}

	@Override
	public Writer getJsonWriter( final URI uri ) throws IOException
	{
		return new OutputStreamWriter( getOutputStream( uri ) );
	}

	@Override
	public N5Reader createN5Reader( final URI baseUri ) throws IOException
	{
		return N5AmazonS3.openS3Reader( s3, decodeS3Uri( baseUri ).getBucket() );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri ) throws IOException
	{
		return N5AmazonS3.openS3Writer( s3, decodeS3Uri( baseUri ).getBucket() );
	}

	@Override
	public N5Reader createN5Reader( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException
	{
		return N5AmazonS3.openS3Reader( s3, decodeS3Uri( baseUri ).getBucket(), gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException
	{
		return N5AmazonS3.openS3Writer( s3, decodeS3Uri( baseUri ).getBucket(), gsonBuilder );
	}

	public static AmazonS3URI decodeS3Uri( final URI uri ) throws IOException
	{
		return new AmazonS3URI( URLDecoder.decode( uri.toString(), StandardCharsets.UTF_8.name() ) );
	}
}
