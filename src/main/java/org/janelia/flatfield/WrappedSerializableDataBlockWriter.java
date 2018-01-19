package org.janelia.flatfield;

import java.io.IOException;
import java.io.Serializable;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;

public class WrappedSerializableDataBlockWriter< T extends Serializable > extends AbstractWrappedSerializableDataBlockReader< N5Writer, T >
{
	public WrappedSerializableDataBlockWriter( final N5Writer n5, final String pathName, final long[] gridPosition ) throws IOException
	{
		super( n5, pathName, gridPosition );
	}

	public void save() throws IOException
	{
		final DatasetAttributes datasetAttributes = n5.getDatasetAttributes( pathName );
		n5.writeBlock( pathName, datasetAttributes, dataBlock );
	}
}
