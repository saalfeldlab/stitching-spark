package org.janelia.stitching;

import mpicbg.imglib.custom.OffsetConverter;

public final class FinalOffsetConverter implements OffsetConverter
{
	// for transforming 'ROI offset' to 'tile offset'
	private final long[][] roiToTileOffset;

	// for transforming 'tile offset' to 'global offset'
	private final double[] globalOffset;

	public FinalOffsetConverter( final long[][] roiToTileOffset, final double[] globalOffset )
	{
		this.roiToTileOffset = roiToTileOffset;
		this.globalOffset = globalOffset;
	}

	@Override
	public long[] roiOffsetToTileOffset( final int[] roiOffset )
	{
		final double[] roiOffsetDouble = new double[ roiOffset.length ];
		for ( int d = 0; d < roiOffset.length; ++d )
			roiOffsetDouble[ d ] = roiOffset[ d ];

		final double[] tileOffsetDouble = roiOffsetToTileOffset( roiOffsetDouble );

		final long[] tileOffset = new long[ tileOffsetDouble.length ];
		for ( int d = 0; d < tileOffset.length; ++d )
			tileOffset[ d ] = Math.round( tileOffsetDouble[ d ] );

		return tileOffset;
	}

	@Override
	public double[] roiOffsetToTileOffset( final double[] roiOffset )
	{
		final double[] tileOffset = roiOffset.clone();
		for ( int i = 0; i < 2; ++i )
			for ( int d = 0; d < tileOffset.length; ++d )
				tileOffset[ d ] += ( i == 0 ? 1 : -1 ) * roiToTileOffset[ i ][ d ]; // plus for fixed tile, minus for moving tile
		return tileOffset;
	}

	@Override
	public double[] tileOffsetToGlobalPosition( final long[] tileOffset )
	{
		final double[] globalPosition = new double[ tileOffset.length ];
		for ( int d = 0; d < globalPosition.length; ++d )
			globalPosition[ d ] = tileOffset[ d ] + globalOffset[ d ];
		return globalPosition;
	}
}
