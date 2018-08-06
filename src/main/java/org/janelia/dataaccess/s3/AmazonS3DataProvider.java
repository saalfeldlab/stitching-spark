package org.janelia.dataaccess.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Reader;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Writer;
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
public class AmazonS3DataProvider implements DataProvider
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
	public boolean fileExists( final String link ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( link );
		return s3.doesObjectExist( s3Uri.getBucket(), s3Uri.getKey() );
	}

	@Override
	public void createFolder( final String link ) throws IOException
	{
		// folders are reflected by the object key structure, so no need to create them explicitly
	}

	@Override
	public void deleteFile( final String link ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( link );
		s3.deleteObject( s3Uri.getBucket(), s3Uri.getKey() );
	}

	@Override
	public void deleteFolder( final String link ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( link );
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
				objectsToDelete.add( object.getKey() );

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
	public void copyFile( final String srcLink, final String dstLink ) throws IOException
	{
		final AmazonS3URI srcS3Uri = decodeS3Uri( srcLink );
		final AmazonS3URI dstS3Uri = decodeS3Uri( dstLink );
		s3.copyObject(
				srcS3Uri.getBucket(), srcS3Uri.getKey(),
				dstS3Uri.getBucket(), dstS3Uri.getKey()
			);
	}

	@Override
	public void copyFolder( final String srcLink, final String dstLink ) throws IOException
	{
		final AmazonS3URI srcS3Uri = decodeS3Uri( srcLink );
		final AmazonS3URI dstS3Uri = decodeS3Uri( dstLink );

		final String prefix = srcS3Uri.getKey().endsWith( "/" ) ? srcS3Uri.getKey() : srcS3Uri.getKey() + "/";
		final ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName( srcS3Uri.getBucket() )
				.withPrefix( prefix );
		ListObjectsV2Result objectsListing;
		do
		{
			objectsListing = s3.listObjectsV2( listObjectsRequest );
			for ( final S3ObjectSummary object : objectsListing.getObjectSummaries() )
			{
				final String objectPath = object.getKey();
				if ( !objectPath.startsWith( prefix ) )
					throw new RuntimeException( "requested prefix does not match with actual prefix" );
				final String objectRelativePath = objectPath.substring( prefix.length() );
				final String objectNewPath = PathResolver.get( dstS3Uri.getKey(), objectRelativePath );
				s3.copyObject(
						srcS3Uri.getBucket(), objectPath,
						dstS3Uri.getBucket(), objectNewPath
					);
			}
			listObjectsRequest.setContinuationToken( objectsListing.getNextContinuationToken() );
		}
		while ( objectsListing.isTruncated() );
	}

	@Override
	public void moveFile( final String srcLink, final String dstLink ) throws IOException
	{
		copyFile( srcLink, dstLink );
		deleteFile( srcLink );
	}

	@Override
	public void moveFolder( final String srcLink, final String dstLink ) throws IOException
	{
		copyFolder( srcLink, dstLink );
		deleteFolder( srcLink );
	}

	@Override
	public InputStream getInputStream( final String link ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( link );
		return s3.getObject( s3Uri.getBucket(), s3Uri.getKey() ).getObjectContent();
	}

	@Override
	public OutputStream getOutputStream( final String link ) throws IOException
	{
		final AmazonS3URI s3Uri = decodeS3Uri( link );
		return new S3ObjectOutputStream( s3Uri );
	}

	@Override
	public ImagePlus loadImage( final String link )
	{
		if ( link.endsWith( ".tif" ) || link.endsWith( ".tiff" ) )
			return ImageImporter.openImage( link );
		throw new NotImplementedException( "Only TIFF images are supported at the moment" );
	}

	@Override
	public void saveImage( final ImagePlus imp, final String link ) throws IOException
	{
		Utils.workaroundImagePlusNSlices( imp );
		// Need to save as a local TIFF file and then upload to S3. IJ does not provide a way to convert ImagePlus to TIFF byte array.
		Path tempPath = null;
		try
		{
			tempPath = Files.createTempFile( null, ".tif" );
			IJ.saveAsTiff( imp, tempPath.toString() );

			final AmazonS3URI s3Uri = decodeS3Uri( link );
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
	public Reader getJsonReader( final String link ) throws IOException
	{
		return new InputStreamReader( getInputStream( link ) );
	}

	@Override
	public Writer getJsonWriter( final String link ) throws IOException
	{
		return new OutputStreamWriter( getOutputStream( link ) );
	}

	@Override
	public N5Reader createN5Reader( final String baseLink ) throws IOException
	{
		return new N5AmazonS3Reader( s3, getBucketName( baseLink ) );
	}

	@Override
	public N5Writer createN5Writer( final String baseLink ) throws IOException
	{
		return new N5AmazonS3Writer( s3, getBucketName( baseLink ) );
	}

	@Override
	public N5Reader createN5Reader( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5AmazonS3Reader( s3, getBucketName( baseLink ), gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5AmazonS3Writer( s3, getBucketName( baseLink ), gsonBuilder );
	}

	public static AmazonS3URI decodeS3Uri( final String link ) throws IOException
	{
		return new AmazonS3URI( URLDecoder.decode( link, StandardCharsets.UTF_8.name() ) );
	}

	private String getBucketName( final String link ) throws IOException
	{
		final AmazonS3URI uri = decodeS3Uri( link );
		if ( uri.getKey() != null && !uri.getKey().isEmpty() )
			throw new IllegalArgumentException( "expected link to a bucket" );
		return uri.getBucket();
	}
}
