package org.janelia.flatfield;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;

import net.imglib2.FinalDimensions;
import net.imglib2.img.list.WrappedListImg;
import net.imglib2.util.Intervals;

abstract public class AbstractWrappedSerializableDataBlockReader< N5 extends N5Reader, T extends Serializable >
{
	protected final N5 n5;
	protected final String pathName;
	protected DataBlock< T[] > dataBlock;
	protected boolean wasLoadedSuccessfully;

	@SuppressWarnings( "unchecked" )
	public AbstractWrappedSerializableDataBlockReader( final N5 n5, final String pathName, final long[] gridPosition ) throws IOException
	{
		this.n5 = n5;
		this.pathName = pathName;

		final DatasetAttributes datasetAttributes = n5.getDatasetAttributes( pathName );
		try
		{
			final DataBlock< T[] > loadedDataBlock = ( DataBlock< T[] > ) n5.readBlock( pathName, datasetAttributes, gridPosition );
			wasLoadedSuccessfully = loadedDataBlock != null;
			dataBlock = loadedDataBlock;
		}
		catch ( final IOException e )
		{
			wasLoadedSuccessfully = false;
		}

		if ( !wasLoadedSuccessfully )
		{
			// compute the block size accounting for the border blocks that can be smaller than regular blocks
			final int[] blockSize = new int[ datasetAttributes.getNumDimensions() ];
			for ( int d = 0; d < blockSize.length; ++d )
				blockSize[ d ] = ( int ) Math.min( datasetAttributes.getDimensions()[ d ] - gridPosition[ d ] * datasetAttributes.getBlockSize()[ d ], datasetAttributes.getBlockSize()[ d ] );
			this.dataBlock = ( DataBlock< T[] > ) datasetAttributes.getDataType().createDataBlock( blockSize, gridPosition );
		}
	}

	public boolean wasLoadedSuccessfully()
	{
		return wasLoadedSuccessfully;
	}

	public WrappedListImg< T > wrap()
	{
		final T[] data = dataBlock.getData();
		final List< T > dataAsList = Arrays.asList( data );
		final long[] blockDimensions = Intervals.dimensionsAsLongArray( new FinalDimensions( dataBlock.getSize() ) );
		final WrappedListImg< T > listImg = new WrappedListImg<>( dataAsList, blockDimensions );
		return listImg;
	}
}
