package org.janelia.dataaccess;

import java.io.IOException;
import java.util.Arrays;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudResourceManagerClient;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageClient;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;

public class CloudN5WriterSupplier extends CloudN5ReaderSupplier implements N5WriterSupplier
{
	private static final long serialVersionUID = -1199787780776971335L;

	public CloudN5WriterSupplier( final String n5Link ) throws IOException
	{
		super( n5Link, Arrays.asList(
				GoogleCloudResourceManagerClient.ProjectsScope.READ_ONLY,
				GoogleCloudStorageClient.StorageScope.READ_WRITE
			) );
	}

	@Override
	public N5Writer get() throws IOException
	{
		return getDataProvider().createN5Writer( n5Link );
	}
}
