package org.janelia.dataaccess.googlecloud;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderType;
import org.janelia.dataaccess.PathResolver;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.googlecloud.N5GoogleCloudStorageReader;
import org.janelia.saalfeldlab.n5.googlecloud.N5GoogleCloudStorageWriter;
import org.janelia.stitching.Utils;
import org.janelia.util.ImageImporter;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.gson.GsonBuilder;

import ij.IJ;
import ij.ImagePlus;

public class GoogleCloudDataProvider implements DataProvider
{
	private class BlobOutputStream extends ByteArrayOutputStream
	{
		private final GoogleCloudStorageURI uri;
		private boolean closed;

		public BlobOutputStream( final GoogleCloudStorageURI uri )
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

				final BlobInfo blobInfo = BlobInfo.newBuilder( uri.getBucket(), uri.getKey() ).build();
				storage.create( blobInfo, bytes );

				super.close();
				closed = true;
			}
		}
	}

	private static final String googleCloudProtocol = "gs";

	private final Storage storage;

	public GoogleCloudDataProvider( final Storage storage )
	{
		this.storage = storage;
	}

	@Override
	public DataProviderType getType()
	{
		return DataProviderType.GOOGLE_CLOUD;
	}

	@Override
	public boolean fileExists( final String link ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( link );
		final Blob blob = storage.get( BlobId.of( googleCloudUri.getBucket(), googleCloudUri.getKey() ) );
		return blob != null && blob.exists();
	}

	@Override
	public void createFolder( final String link ) throws IOException
	{
		// folders are reflected by the object key structure, so no need to create them explicitly
	}

	@Override
	public void copyFile( final String srcLink, final String dstLink ) throws IOException
	{
		final GoogleCloudStorageURI srcGoogleCloudUri = new GoogleCloudStorageURI( srcLink );
		final GoogleCloudStorageURI dstGoogleCloudUri = new GoogleCloudStorageURI( dstLink );
		final CopyRequest request = CopyRequest.newBuilder()
				.setSource( BlobId.of( srcGoogleCloudUri.getBucket(), srcGoogleCloudUri.getKey() ) )
				.setTarget( BlobId.of( dstGoogleCloudUri.getBucket(), dstGoogleCloudUri.getKey() ) )
				.build();
		storage.copy( request ).getResult();
	}

	@Override
	public void copyFolder( final String srcLink, final String dstLink ) throws IOException
	{
		final GoogleCloudStorageURI srcGoogleCloudUri = new GoogleCloudStorageURI( srcLink );
		final GoogleCloudStorageURI dstGoogleCloudUri = new GoogleCloudStorageURI( dstLink );

		final String prefix = srcGoogleCloudUri.getKey().endsWith( "/" ) ? srcGoogleCloudUri.getKey() : srcGoogleCloudUri.getKey() + "/";
		final Page< Blob > blobListing = storage.list( srcGoogleCloudUri.getBucket(), BlobListOption.prefix( prefix ) );
		for ( final Iterator< Blob > blobIterator = blobListing.iterateAll().iterator(); blobIterator.hasNext(); )
		{
			final BlobId blobId = blobIterator.next().getBlobId();

			final String objectPath = blobId.getName();
			if ( !objectPath.startsWith( prefix ) )
				throw new RuntimeException( "requested prefix does not match with actual prefix" );
			final String objectRelativePath = objectPath.substring( prefix.length() );
			final String objectNewPath = PathResolver.get( dstGoogleCloudUri.getKey(), objectRelativePath );

			final CopyRequest request = CopyRequest.newBuilder()
					.setSource( blobId )
					.setTarget( BlobId.of( dstGoogleCloudUri.getBucket(), objectNewPath ) )
					.build();
			storage.copy( request ).getResult();
		}
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
	public void deleteFile( final String link ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( link );
		storage.delete( BlobId.of( googleCloudUri.getBucket(), googleCloudUri.getKey() ) );
	}

	@Override
	public void deleteFolder( final String link ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( link );
		final String prefix = googleCloudUri.getKey().endsWith( "/" ) ? googleCloudUri.getKey() : googleCloudUri.getKey() + "/";
		final List< BlobId > subBlobs = new ArrayList<>();
		final Page< Blob > blobListing = storage.list( googleCloudUri.getBucket(), BlobListOption.prefix( prefix ) );
		for ( final Iterator< Blob > blobIterator = blobListing.iterateAll().iterator(); blobIterator.hasNext(); )
			subBlobs.add( blobIterator.next().getBlobId() );
		storage.delete( subBlobs );
	}

	@Override
	public InputStream getInputStream( final String link ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( link );
		final Blob blob = storage.get( BlobId.of( googleCloudUri.getBucket(), googleCloudUri.getKey() ) );
		final byte[] bytes = blob.getContent();
		return new ByteArrayInputStream( bytes );
	}

	@Override
	public OutputStream getOutputStream( final String link ) throws IOException
	{
		return new BlobOutputStream( new GoogleCloudStorageURI( link ) );
	}

	@Override
	public ImagePlus loadImage( final String link ) throws IOException
	{
		if ( link.endsWith( ".tif" ) || link.endsWith( ".tiff" ) )
			return ImageImporter.openImage( link );
		throw new NotImplementedException( "Only TIFF images are supported at the moment" );
	}

	@Override
	public void saveImage( final ImagePlus imp, final String link ) throws IOException
	{
		Utils.workaroundImagePlusNSlices( imp );
		// Need to save as a local TIFF file and then upload to Google Cloud. IJ does not provide a way to convert ImagePlus to TIFF byte array.
		Path tempPath = null;
		try
		{
			tempPath = Files.createTempFile( null, ".tif" );
			IJ.saveAsTiff( imp, tempPath.toString() );

			final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( link );
			final BlobInfo blobInfo = BlobInfo.newBuilder( googleCloudUri.getBucket(), googleCloudUri.getKey() ).build();
			final byte[] bytes = Files.readAllBytes( tempPath );
			storage.create( blobInfo, bytes );
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
		return new N5GoogleCloudStorageReader( storage, getBucketName( baseLink ) );
	}

	@Override
	public N5Writer createN5Writer( final String baseLink ) throws IOException
	{
		return new N5GoogleCloudStorageWriter( storage, getBucketName( baseLink ) );
	}

	@Override
	public N5Reader createN5Reader( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5GoogleCloudStorageReader( storage, getBucketName( baseLink ), gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5GoogleCloudStorageWriter( storage, getBucketName( baseLink ), gsonBuilder );
	}

	private String getBucketName( final String link )
	{
		final GoogleCloudStorageURI uri = new GoogleCloudStorageURI( link );
		if ( uri.getKey() != null && !uri.getKey().isEmpty() )
			throw new IllegalArgumentException( "expected link to a bucket" );
		return uri.getBucket();
	}
}
