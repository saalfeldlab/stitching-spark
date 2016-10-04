package org.janelia.stitching;

import java.io.Serializable;

public class MultiscaledExportMetadata implements Serializable
{
	public final String baseFolder;
	public final int numScales;
	public final long[] imageDimensions;
	public final int[] cellDimensions;
	public final double[] offset;

	public MultiscaledExportMetadata(
			final String baseFolder,
			final int numScales,
			final long[] imageDimensions,
			final int[] cellDimensions )
	{
		this( baseFolder, numScales, imageDimensions, cellDimensions, null );
	}

	public MultiscaledExportMetadata(
			final String baseFolder,
			final int numScales,
			final long[] imageDimensions,
			final int[] cellDimensions,
			final double[] offset )
	{
		this.baseFolder = baseFolder;
		this.numScales = numScales;
		this.imageDimensions = imageDimensions;
		this.cellDimensions = cellDimensions;
		this.offset = offset;

		assert imageDimensions.length == cellDimensions.length;
		if ( offset != null )
			assert imageDimensions.length == offset.length;
	}
}
