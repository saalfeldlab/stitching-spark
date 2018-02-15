package bigwarp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import bigwarp.landmarks.LandmarkTableModel;
import jitk.spline.ThinPlateR2LogRSplineKernelTransform;

import org.apache.commons.codec.binary.Base64;

/**
 * Converts a bigwarp landmark csv to a serialized ThinPlateSplineTransform
 *  
 * @author John Bogovic &lt;bogovicj@janelia.hhmi.org&gt;
 *
 */
public class Landmarks2Transform
{

	public static void main( String[] args ) throws IOException
	{
		int ndims = Integer.parseInt( args[ 0 ] );
		String pts_fn = args[ 1 ];
		String xfm_out = args[ 2 ];

		LandmarkTableModel ltm = new LandmarkTableModel( ndims );
		// read the existing point pairs 
		ltm.load( new File( pts_fn) );

		ThinPlateR2LogRSplineKernelTransform ltmXfm = ltm.getTransform();

		Path outPath = Paths.get( xfm_out );
		try (BufferedWriter writer = Files.newBufferedWriter( outPath )) 
		{
		    writer.write( toDataString( ltmXfm ));
		}

		System.out.println( "finished" );
		System.exit( 0 );
	}


	public static String toDataString( ThinPlateR2LogRSplineKernelTransform tps )
	{
		final StringBuilder data = new StringBuilder();
		toDataString( data, tps );
		return data.toString();
	}

	private static final void toDataString( final StringBuilder data,
			ThinPlateR2LogRSplineKernelTransform tps )
	{

		data.append( "ThinPlateSplineR2LogR" );

		final int ndims = tps.getNumDims();
		final int nLm = tps.getNumLandmarks();

		data.append( ' ' ).append( ndims ); // dimensions
		data.append( ' ' ).append( nLm ); // landmarks

		if ( tps.getAffine() == null )
		{
			data.append( ' ' ).append( "null" ); // aMatrix
		} else
		{
			final double[][] aMtx = tps.getAffine();
			final double[] bVec = tps.getTranslation();

			final double[] buffer = new double[ ndims * ndims + ndims ];
			int k = -1;
			for ( int i = 0; i < ndims; i++ )
				for ( int j = 0; j < ndims; j++ )
				{
					buffer[ ++k ] = aMtx[ i ][ j ];
				}
			for ( int i = 0; i < ndims; i++ )
			{
				buffer[ ++k ] = bVec[ i ];
			}

			data.append( ' ' ).append( encodeBase64( buffer ) );
		}

		final double[][] srcPts = tps.getSourceLandmarks();
		final double[] dMtxDat = tps.getKnotWeights();

		final double[] buffer = new double[ 2 * nLm * ndims ];
		int k = -1;
		for ( int l = 0; l < nLm; l++ )
			for ( int d = 0; d < ndims; d++ )
				buffer[ ++k ] = srcPts[ d ][ l ];

		for ( int i = 0; i < ndims * nLm; i++ )
		{
			buffer[ ++k ] = dMtxDat[ i ];
		}
		data.append( ' ' ).append( encodeBase64( buffer ) );
	}

	public static ThinPlateR2LogRSplineKernelTransform fromDataString( final String data ) throws NumberFormatException
	{

		final String[] fields = data.split( "\\s+" );

		int i = 0;

		final int ndims = Integer.parseInt( fields[ ++i ] );
		final int nLm = Integer.parseInt( fields[ ++i ] );

		double[][] aMtx = null;
		double[] bVec = null;
		if ( fields[ i + 1 ].equals( "null" ) )
		{
			// System.out.println(" No affines " );
			++i;
		} else
		{
			aMtx = new double[ ndims ][ ndims ];
			bVec = new double[ ndims ];

			final double[] values;
			try
			{
				values = decodeBase64( fields[ ++i ], ndims * ndims + ndims );
			} catch ( final DataFormatException e )
			{
				throw new NumberFormatException( "Failed decoding affine matrix." );
			}
			int l = -1;
			for ( int k = 0; k < ndims; k++ )
				for ( int j = 0; j < ndims; j++ )
				{
					aMtx[ k ][ j ] = values[ ++l ];
				}
			for ( int j = 0; j < ndims; j++ )
			{
				bVec[ j ] = values[ ++l ];
			}
		}

		final double[] values;
		try
		{
			values = decodeBase64( fields[ ++i ], 2 * nLm * ndims );
		} catch ( final DataFormatException e )
		{
			throw new NumberFormatException( "Failed decoding landmarks and weights." );
		}
		int k = -1;

		// parse control points
		final double[][] srcPts = new double[ ndims ][ nLm ];
		for ( int l = 0; l < nLm; l++ )
			for ( int d = 0; d < ndims; d++ )
			{
				srcPts[ d ][ l ] = values[ ++k ];
			}

		// parse control point coordinates
		int m = -1;
		final double[] dMtxDat = new double[ nLm * ndims ];
		for ( int l = 0; l < nLm; l++ )
			for ( int d = 0; d < ndims; d++ )
			{
				dMtxDat[ ++m ] = values[ ++k ];
			}

		return new ThinPlateR2LogRSplineKernelTransform( srcPts, aMtx, bVec, dMtxDat );
	}

	static private String encodeBase64(final double[] src) {
		final byte[] bytes = new byte[src.length * 8];
		for (int i = 0, j = -1; i < src.length; ++i) {
			final long bits = Double.doubleToLongBits(src[i]);
			bytes[++j] = (byte)(bits >> 56);
			bytes[++j] = (byte)((bits >> 48) & 0xffL);
			bytes[++j] = (byte)((bits >> 40) & 0xffL);
			bytes[++j] = (byte)((bits >> 32) & 0xffL);
			bytes[++j] = (byte)((bits >> 24) & 0xffL);
			bytes[++j] = (byte)((bits >> 16) & 0xffL);
			bytes[++j] = (byte)((bits >> 8) & 0xffL);
			bytes[++j] = (byte)(bits & 0xffL);
		}
		final Deflater deflater = new Deflater();
		deflater.setInput(bytes);
		deflater.finish();
		final byte[] zipped = new byte[bytes.length];
		final int n = deflater.deflate(zipped);
		if (n == bytes.length)
			return '@' + Base64.encodeBase64String(bytes);
		else
			return Base64.encodeBase64String(Arrays.copyOf(zipped, n));
	}

	static private double[] decodeBase64(final String src, final int n)
			throws DataFormatException {
		final byte[] bytes;
		if (src.charAt(0) == '@') {
			bytes = Base64.decodeBase64(src.substring(1));
		} else {
			bytes = new byte[n * 8];
			final byte[] zipped = Base64.decodeBase64(src);
			final Inflater inflater = new Inflater();
			inflater.setInput(zipped, 0, zipped.length);

			inflater.inflate(bytes);
			inflater.end();
		}
		final double[] doubles = new double[n];
		for (int i = 0, j = -1; i < n; ++i) {
			long bits = 0L;
			bits |= (bytes[++j] & 0xffL) << 56;
			bits |= (bytes[++j] & 0xffL) << 48;
			bits |= (bytes[++j] & 0xffL) << 40;
			bits |= (bytes[++j] & 0xffL) << 32;
			bits |= (bytes[++j] & 0xffL) << 24;
			bits |= (bytes[++j] & 0xffL) << 16;
			bits |= (bytes[++j] & 0xffL) << 8;
			bits |= bytes[++j] & 0xffL;
			doubles[i] = Double.longBitsToDouble(bits);
		}
		return doubles;
}
}
