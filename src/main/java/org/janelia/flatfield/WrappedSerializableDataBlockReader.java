package org.janelia.flatfield;

import java.io.IOException;
import java.io.Serializable;

import org.janelia.saalfeldlab.n5.N5Reader;

public class WrappedSerializableDataBlockReader< T extends Serializable > extends AbstractWrappedSerializableDataBlockReader< N5Reader, T >
{
	public WrappedSerializableDataBlockReader( final N5Reader n5, final String pathName, final long[] gridPosition ) throws IOException
	{
		super( n5, pathName, gridPosition, OpenMode.READ );
	}
}
