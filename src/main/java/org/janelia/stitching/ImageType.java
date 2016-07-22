package org.janelia.stitching;

import java.util.TreeMap;

import ij.ImagePlus;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;

/**
 * @author pisarevi
 *
 */

//Convenient enum class over the ImagePlus type values
public enum ImageType
{
	/** 8-bit grayscale (unsigned) */
	GRAY8( ImagePlus.GRAY8, new UnsignedByteType() ),

	/** 16-bit grayscale (unsigned) */
	GRAY16( ImagePlus.GRAY16, new UnsignedShortType() ),

	/** 32-bit floating-point grayscale */
	GRAY32( ImagePlus.GRAY32, new FloatType() ),

	/** 8-bit indexed color */
	COLOR_256( ImagePlus.COLOR_256, new UnsignedByteType() ),

	/** 32-bit RGB color */
	COLOR_RGB( ImagePlus.COLOR_RGB, new ARGBType() );

	private final int val;
	private final NumericType< ? > type;

	private ImageType( final int val, final NumericType< ? > type )
	{
		this.val = val;
		this.type = type;
	}

	public int getValue()
	{
		return val;
	}

	public NumericType< ? > getType()
	{
		return type;
	}
	
	// for creating ImageType object from an integer value
	private static final TreeMap< Integer, ImageType > map = new TreeMap<>();
    static 
    {
        for ( final ImageType type : values() )
            map.put( type.getValue(), type );
    }
    public static ImageType valueOf( final int val ) 
    {
		return map.get( val );
    }
}
