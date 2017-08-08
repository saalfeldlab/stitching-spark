package org.janelia.stitching;

import java.util.TreeMap;

import ij.ImagePlus;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;

/**
 * Convenience mapping from ImagePlus integer constant values of possible types to a enum.
 *
 * @author Igor Pisarev
 */

public enum ImageType
{
	/** 8-bit grayscale (unsigned) */
	GRAY8( ImagePlus.GRAY8, new UnsignedByteType() ),

	/** 16-bit grayscale (unsigned) */
	GRAY16( ImagePlus.GRAY16, new UnsignedShortType() ),

	/** 32-bit floating-point grayscale */
	GRAY32( ImagePlus.GRAY32, new FloatType() );

	private final int val;
	private final RealType< ? > type;

	private ImageType( final int val, final RealType< ? > type )
	{
		this.val = val;
		this.type = type;
	}

	public RealType< ? > getType()
	{
		return type;
	}

	// for creating ImageType object from an integer value
	private static final TreeMap< Integer, ImageType > map = new TreeMap<>();
	static
	{
		for ( final ImageType type : values() )
			map.put( type.val, type );
	}
	public static ImageType valueOf( final int val )
	{
		return map.get( val );
	}
}
