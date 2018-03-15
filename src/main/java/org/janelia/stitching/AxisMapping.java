package org.janelia.stitching;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public final class AxisMapping implements Serializable
{
	private static final long serialVersionUID = 2407124829248425056L;

	public final int[] axisMapping;
	public final boolean[] flip;

	public AxisMapping( final String[] axisMappingStr )
	{
		final List< String > axesStr = Arrays.asList( "x", "y", "z" );
		axisMapping = new int[ axisMappingStr.length ];
		flip = new boolean[ axisMappingStr.length ];
		for ( int d = 0; d < axisMappingStr.length; ++d )
		{
			flip[ d ] = axisMappingStr[ d ].startsWith( "-" );
			final String axisStr = axisMappingStr[ d ].substring( flip[ d ] ? 1 : 0 ).toLowerCase();
			final int dMap = axesStr.indexOf( axisStr );
			if ( dMap == -1 )
				throw new IllegalArgumentException( "expected x/y/z, got " + axisStr );
			axisMapping[ d ] = dMap;
		}

		// check that all axes are included
		final int[] axesIncluded = new int[ axisMapping.length ];
		for ( int d = 0; d < axesIncluded.length; ++d )
			axesIncluded[ axisMapping[ d ] ] = 1;
		if ( Arrays.stream( axesIncluded ).min().getAsInt() == 0 )
			throw new IllegalArgumentException( "axis mapping is not consistent as it does not include all x/y/z axes" );
	}
}