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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.googlecloud.N5GoogleCloudStorage;
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

class GoogleCloudDataProvider implements DataProvider
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
	public URI getUri( final String path ) throws URISyntaxException
	{
		final URI uri = new URI( path );
		if ( googleCloudProtocol.equals( uri.getScheme() ) )
			return uri;
		return new URI( googleCloudProtocol, null, path );
	}

	@Override
	public boolean fileExists( final URI uri ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( uri );
		final Blob blob = storage.get( BlobId.of( googleCloudUri.getBucket(), googleCloudUri.getKey() ) );
		return blob != null && blob.exists();
	}

	@Override
	public void copyFile( final URI uriSrc, final URI uriDst ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUriSrc = new GoogleCloudStorageURI( uriSrc );
		final GoogleCloudStorageURI googleCloudUriDst = new GoogleCloudStorageURI( uriDst );
		final CopyRequest request = CopyRequest.newBuilder()
				.setSource( BlobId.of( googleCloudUriSrc.getBucket(), googleCloudUriSrc.getKey() ) )
				.setTarget( BlobId.of( googleCloudUriDst.getBucket(), googleCloudUriDst.getKey() ) )
				.build();
		storage.copy( request ).getResult();
	}

	@Override
	public void moveFile( final URI uriSrc, final URI uriDst ) throws IOException
	{
		copyFile( uriSrc, uriDst );
		deleteFile( uriSrc );
	}

	@Override
	public void deleteFile( final URI uri ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( uri );
		storage.delete( BlobId.of( googleCloudUri.getBucket(), googleCloudUri.getKey() ) );
	}

	@Override
	public void deleteFolder( final URI uri ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( uri );
		final String prefix = googleCloudUri.getKey().endsWith( "/" ) ? googleCloudUri.getKey() : googleCloudUri.getKey() + "/";
		final List< BlobId > subBlobs = new ArrayList<>();
		final Page< Blob > blobListing = storage.list( googleCloudUri.getBucket(), BlobListOption.prefix( prefix ) );
		for ( final Iterator< Blob > blobIterator = blobListing.iterateAll().iterator(); blobIterator.hasNext(); )
			subBlobs.add( blobIterator.next().getBlobId() );
		storage.delete( subBlobs );
	}

	@Override
	public InputStream getInputStream( final URI uri ) throws IOException
	{
		final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( uri );
		final Blob blob = storage.get( BlobId.of( googleCloudUri.getBucket(), googleCloudUri.getKey() ) );
		final byte[] bytes = blob.getContent();
		return new ByteArrayInputStream( bytes );
	}

	@Override
	public OutputStream getOutputStream( final URI uri ) throws IOException
	{
		return new BlobOutputStream( new GoogleCloudStorageURI( uri ) );
	}

	@Override
	public ImagePlus loadImage( final URI uri ) throws IOException
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
		// Need to save as a local TIFF file and then upload to Google Cloud. IJ does not provide a way to convert ImagePlus to TIFF byte array.
		Path tempPath = null;
		try
		{
			tempPath = Files.createTempFile( null, ".tif" );
			IJ.saveAsTiff( imp, tempPath.toString() );

			final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI( uri );
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
		return N5GoogleCloudStorage.openCloudStorageReader( storage, new GoogleCloudStorageURI( baseUri ).getBucket() );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri ) throws IOException
	{
		return N5GoogleCloudStorage.openCloudStorageWriter( storage, new GoogleCloudStorageURI( baseUri ).getBucket() );
	}

	@Override
	public N5Reader createN5Reader( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException
	{
		return N5GoogleCloudStorage.openCloudStorageReader( storage, new GoogleCloudStorageURI( baseUri ).getBucket(), gsonBuilder );
	}

	@Override
	public N5Writer createN5Writer( final URI baseUri, final GsonBuilder gsonBuilder ) throws IOException
	{
		return N5GoogleCloudStorage.openCloudStorageWriter( storage, new GoogleCloudStorageURI( baseUri ).getBucket(), gsonBuilder );
	}
}
