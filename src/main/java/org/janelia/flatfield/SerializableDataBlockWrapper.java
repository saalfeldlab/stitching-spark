package org.janelia.flatfield;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.FinalDimensions;
import net.imglib2.img.list.WrappedListImg;
import net.imglib2.util.Intervals;

public class SerializableDataBlockWrapper< T extends Serializable >
{
	private final N5Writer n5;
	private final String pathName;
	private final DataBlock< ? > dataBlock;

	public SerializableDataBlockWrapper( final N5Writer n5, final String pathName, final long[] gridPosition ) throws IOException
	{
		this.n5 = n5;
		this.pathName = pathName;

		final DatasetAttributes datasetAttributes = n5.getDatasetAttributes( pathName );
		final DataBlock< ? > loadedDataBlock = n5.readBlock( pathName, datasetAttributes, gridPosition );
		if ( loadedDataBlock != null )
		{
			dataBlock = loadedDataBlock;
		}
		else
		{
			dataBlock = datasetAttributes.getDataType().createDataBlock( datasetAttributes.getBlockSize(), gridPosition );
		}
	}

	public WrappedListImg< T > wrap()
	{
		@SuppressWarnings( "unchecked" )
		final T[] data = ( T[] ) dataBlock.getData();
		final List< T > dataAsList = Arrays.asList( data );
		final long[] blockDimensions = Intervals.dimensionsAsLongArray( new FinalDimensions( dataBlock.getSize() ) );
		final WrappedListImg< T > listImg = new WrappedListImg<>( dataAsList, blockDimensions );
		return listImg;
	}

	public void save() throws IOException
	{
		final DatasetAttributes datasetAttributes = n5.getDatasetAttributes( pathName );
		n5.writeBlock( pathName, datasetAttributes, dataBlock );
	}
}
