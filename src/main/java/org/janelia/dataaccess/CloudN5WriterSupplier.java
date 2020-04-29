package org.janelia.dataaccess;

import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;

import java.io.IOException;

public class CloudN5WriterSupplier extends CloudN5ReaderSupplier implements N5WriterSupplier
{
	public CloudN5WriterSupplier( final String n5Link )
	{
		super( n5Link );
	}

	@Override
	public N5Writer get() throws IOException
	{
		return getDataProvider().createN5Writer( n5Link );
	}
}
