package org.janelia.dataaccess.googlecloud;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.janelia.dataaccess.AbstractJSONDataProvider;
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

public class GoogleCloudDataProvider extends AbstractJSONDataProvider
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
	public boolean exists( final String link ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( link );
		final Blob blob = storage.get( BlobId.of( googleCloudUri.getBucket(), googleCloudUri.getKey() ) );
		if ( blob != null && blob.exists() )
			return true;

		// Could be a directory, meaning that the bucket has an object with the key containing the given link as a prefix.
		// Try to find such an object.
		final String prefix = googleCloudUri.getKey().isEmpty() ? "" : addTrailingSlash( googleCloudUri.getKey() );
		final BlobListOption[] blobListOptions = {
				BlobListOption.prefix( prefix ),
				BlobListOption.pageSize( 1 )
			};
		final Page< Blob > blobListing = storage.list( googleCloudUri.getBucket(), blobListOptions );
		return blobListing.getValues().iterator().hasNext();
	}

	@Override
	public void createFolder( final String link ) throws IOException
	{
		// In Google Cloud Storage, there are no explicit folders, and they are implicitly represented by the '/' delimeters in the object key.
		// But, there is a way to create the folder in this representation by creating an empty object ending with '/'.
		// Do this only for the target path specified by the link parameter, but not for the intermediate folders
		// since it's not necessary, and it may have potential problems with access permissions in upper levels of the bucket.

		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( link );
		final BlobInfo blobInfo = BlobInfo.newBuilder(
				googleCloudUri.getBucket(),
				addTrailingSlash( googleCloudUri.getKey() )
			).build();
		storage.create( blobInfo, ( byte[] ) null );
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
	public N5Reader createN5Reader( final String baseLink ) throws IOException
	{
		return new N5GoogleCloudStorageReader( storage, new GoogleCloudStorageURI( baseLink ) );
	}

	@Override
	public N5Writer createN5Writer( final String baseLink ) throws IOException
	{
		return new N5GoogleCloudStorageWriter( storage, new GoogleCloudStorageURI( baseLink ) );
	}

	@Override
	public N5Reader createN5Reader( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5GoogleCloudStorageReader( storage, new GoogleCloudStorageURI( baseLink ), gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException
	{
		return new N5GoogleCloudStorageWriter( storage, new GoogleCloudStorageURI( baseLink ), gsonBuilder );
	}

	private static String addTrailingSlash( final String link )
	{
		return link.endsWith( "/" ) ? link : link + "/";
	}
}
