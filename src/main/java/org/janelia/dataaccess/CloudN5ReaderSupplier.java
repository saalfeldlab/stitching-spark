package org.janelia.dataaccess;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier;

import java.io.IOException;

public class CloudN5ReaderSupplier implements N5ReaderSupplier
{
	protected final String n5Link;

	public CloudN5ReaderSupplier( final String n5Link )
	{
		this.n5Link = n5Link;
	}

	protected DataProvider getDataProvider()
	{
		return DataProviderFactory.create( DataProviderFactory.detectType( n5Link ) );
	}

	@Override
	public N5Reader get() throws IOException
	{
		return getDataProvider().createN5Reader( n5Link );
	}
}
