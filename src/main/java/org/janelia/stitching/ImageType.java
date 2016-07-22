package org.janelia.stitching;

import java.util.TreeMap;

//Convenient enum class over the ImagePlus type values
public enum ImageType {
	
	/** 8-bit grayscale (unsigned)*/
	GRAY8( 0 ),
	
	/** 16-bit grayscale (unsigned) */
	GRAY16( 1 ),
	
	/** 32-bit floating-point grayscale */
	GRAY32( 2 ),
	
	/** 8-bit indexed color */
	COLOR_256( 3 ),
	
	/** 32-bit RGB color */
	COLOR_RGB( 4 );
	
	
	
	private final int val;
    private ImageType( final int val ) { this.val = val; }
    public int getValue() { return val; }
    
    private static final TreeMap< Integer, ImageType > map = new TreeMap<>();
    static {
        for ( final ImageType type : values() ) {
            map.put( type.getValue(), type );
        }
    }
    public static ImageType valueOf( final int val ) {
		return map.get( val );
    }
}