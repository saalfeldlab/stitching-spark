package org.janelia.flatfield;

import java.io.IOException;
import java.io.Serializable;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;

public class WrappedSerializableDataBlockWriter< T extends Serializable > extends AbstractWrappedSerializableDataBlockReader< N5Writer, T >
{
	@SuppressWarnings( "unchecked" )
	public WrappedSerializableDataBlockWriter( final N5Writer n5, final String pathName, final long[] gridPosition ) throws IOException
	{
		super( n5, pathName, gridPosition, OpenMode.READ_WRITE );

		if ( !wasLoadedSuccessfully )
		{
			// compute the block size accounting for the border blocks that can be smaller than regular blocks
			final DatasetAttributes datasetAttributes = n5.getDatasetAttributes( pathName );
			final int[] blockSize = new int[ datasetAttributes.getNumDimensions() ];
			for ( int d = 0; d < blockSize.length; ++d )
				blockSize[ d ] = ( int ) Math.min( datasetAttributes.getDimensions()[ d ] - gridPosition[ d ] * datasetAttributes.getBlockSize()[ d ], datasetAttributes.getBlockSize()[ d ] );
			this.dataBlock = ( DataBlock< T[] > ) datasetAttributes.getDataType().createDataBlock( blockSize, gridPosition );
		}
	}

	public void save() throws IOException
	{
		final DatasetAttributes datasetAttributes = n5.getDatasetAttributes( pathName );
		n5.writeBlock( pathName, datasetAttributes, dataBlock );
	}
}
