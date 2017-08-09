package net.imglib2.realtransform;

import java.util.Arrays;

import org.janelia.stitching.Boundaries;
import org.janelia.stitching.TileInfo;

import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.Localizable;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.util.ConstantUtils;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;

public class TransformBoundingBox
{
	

	public static <T extends NumericType<T>> RandomAccessibleInterval<T> warpRai(
			final RandomAccessibleInterval<T> in,
			final InterpolatorFactory<T,RandomAccessible<T>> interpolator,
			final InvertibleRealTransform xfm )
	{
		return Views.interval(
					Views.raster( RealViews.transformReal(
						Views.interpolate(
								Views.extendZero(in),
								interpolator ),
						xfm )),
					boundingBoxCorners( in, xfm ));
	}

	public static TileInfo[] tileInfosToBoundingBoxes( TileInfo[] in, InvertibleRealTransform invxfm )
	{
		TileInfo[] out = new TileInfo[ in.length ];
		int i = 0;
		for( TileInfo ti : in )
		{
			// compute the new bounding box
			Boundaries boundary = ti.getBoundaries();
			FinalInterval newbbox = boundingBoxCorners( boundary, invxfm );
			
			final TileInfo bboxTile = ti.clone();

			// set bbox relevant stuff
			bboxTile.setPosition( Intervals.minAsDoubleArray( newbbox ) );
			bboxTile.setSize( Intervals.dimensionsAsLongArray(newbbox) );

			
			out[ i++ ] = bboxTile;
		}
		return out;
	}

	/**
	 * Determines the bounding box of the input "inverse tps."
	 * 
	 * Given an interval, and a thin plate spline transform, this method returns a new 
	 * interval that contains
	 * 
	 * This method assumes the thin plate spline maps from target space
	 * to moving space (as it does in BigWarp, for example ).
	 * 
	 * 
	 * @param interval the interval
	 * @param invxfm the transform
	 * @return the output bounding box
	 */
	public static FinalInterval boundingBoxCorners( Interval interval, InvertibleRealTransform invxfm )
	{
//		assert( interval.numDimensions() == invtps.getNumDims() );
		
		int nd = interval.numDimensions();
		long[] min = new long[ nd ];
		Arrays.fill( min, Long.MAX_VALUE );

		long[] max = new long[ nd ];
		Arrays.fill( max, Long.MIN_VALUE );

		double[] ptOrig = new double[ nd ];
		double[] ptWarp = new double[ nd ];

		// inverse transform each corner of the input interval 
		// and keep track of where they end up
		Cursor<?> cornerCursor = cornerIterator( interval );
		while( cornerCursor.hasNext() )
		{
			cornerCursor.fwd();
			cornerPoint( ptOrig, interval, cornerCursor );
			invxfm.applyInverse( ptWarp, ptOrig );

//			System.out.println( "error: " + error );
//			System.out.println( "corner: " + ptOrig[ 0 ] + " " + ptOrig[ 1 ] + " " + ptOrig[ 2 ]);
//			System.out.println( "goes to: " + ptWarp[ 0 ] + " " + ptWarp[ 1 ] + " " + ptWarp[ 2 ]);

			updateMin( min, ptWarp );
			updateMax( max, ptWarp );
		}

		return new FinalInterval( min, max );
	}

	public static FinalInterval boundingBoxFowardCorners( Interval interval, RealTransform xfm )
	{
//		assert( interval.numDimensions() == invtps.getNumDims() );

		int nd = interval.numDimensions();
		long[] min = new long[ nd ];
		Arrays.fill( min, Long.MAX_VALUE );

		long[] max = new long[ nd ];
		Arrays.fill( max, Long.MIN_VALUE );

		double[] ptOrig = new double[ nd ];
		double[] ptWarp = new double[ nd ];

		// inverse transform each corner of the input interval 
		// and keep track of where they end up
		Cursor<?> cornerCursor = cornerIterator( interval );
		while( cornerCursor.hasNext() )
		{
			cornerCursor.fwd();
			cornerPoint( ptOrig, interval, cornerCursor );
			xfm.apply( ptOrig, ptWarp );

//			System.out.println( "error: " + error );
//			System.out.println( "corner: " + ptOrig[ 0 ] + " " + ptOrig[ 1 ] + " " + ptOrig[ 2 ]);
//			System.out.println( "goes to: " + ptWarp[ 0 ] + " " + ptWarp[ 1 ] + " " + ptWarp[ 2 ] );
			
			updateMin( min, ptWarp );
			updateMax( max, ptWarp );
		}

		return new FinalInterval( min, max );
	}
	
	public static FinalRealInterval boundingBoxCorners( RealInterval interval, InvertibleRealTransform invxfm )
	{
//		assert( interval.numDimensions() == invtps.getNumDims() );
		
		int nd = interval.numDimensions();
		double[] min = new double[ nd ];
		double[] max = new double[ nd ];
		
		double[] ptOrig = new double[ nd ];
		double[] ptWarp = new double[ nd ];

		// inverse transform each corner of the input interval 
		// and keep track of where they end up
		Cursor<?> cornerCursor = cornerIterator( interval );
		while( cornerCursor.hasNext() )
		{
			cornerCursor.fwd();
			cornerPointReal( ptOrig, interval, cornerCursor );
//			double error = invxfm.inverseTol( ptOrig, ptWarp, 0.05, 3000 );
			invxfm.applyInverse( ptWarp, ptOrig );
//			ptWarp = invtps.inverse(ptOrig, 0.05 );

//			System.out.println( "error: " + error );
			System.out.println( "corner: " + ptOrig[ 0 ] + " " + ptOrig[ 1 ]);
			System.out.println( "goes to: " + ptWarp[ 0 ] + " " + ptWarp[ 1 ]);
			
			updateMin( min, ptWarp );
			updateMax( max, ptWarp );
		}

		return new FinalRealInterval( min, max );
	}
	
	private static void updateMin( double[] min, double[] pt )
	{
		for( int i = 0; i < min.length; i++ )
		{
			double x = pt[ i ];
			if( x < min[ i ])
			{
				min[ i ] = x;
			}
		}
	}

	private static void updateMax( double[] max, double[] pt )
	{
		for( int i = 0; i < max.length; i++ )
		{
			double x = pt[ i ];
			if( x > max[ i ])
			{
				max[ i ] = x;
			}
		}
	}
	
	private static void updateMin( long[] min, double[] pt )
	{
		for( int i = 0; i < min.length; i++ )
		{
			long x = (long)Math.floor( pt[i] );
			if( x < min[ i ])
			{
				min[ i ] = x;
			}
		}
	}

	private static void updateMax( long[] max, double[] pt )
	{
		for( int i = 0; i < max.length; i++ )
		{
			long x = (long)Math.ceil( pt[i] );
			if( x > max[ i ])
			{
				max[ i ] = x;
			}
		}
	}

	public static void cornerPointReal( double[] thePoint, RealInterval interval, Localizable cornerIndicator )
	{
		for( int i = 0; i < interval.numDimensions(); i++ )
		{
			if( cornerIndicator.getIntPosition( i ) == 0 )
			{
				thePoint[ i ] = interval.realMin( i );
			}
			else
			{
				thePoint[ i ] = interval.realMax( i );
			}
		}
	}
	
	public static void cornerPoint( double[] thePoint, Interval interval, Localizable cornerIndicator )
	{
		for( int i = 0; i < interval.numDimensions(); i++ )
		{
			if( cornerIndicator.getIntPosition( i ) == 0 )
			{
				thePoint[ i ] = interval.min( i );
			}
			else
			{
				thePoint[ i ] = interval.max( i );
			}
		}
	}

	public static Cursor<?> cornerIterator( RealInterval interval )
	{
		int nd = interval.numDimensions();
		long[] unitSize = new long[ nd ];
		Arrays.fill( unitSize, 2 );
		FinalInterval boxInterval = new FinalInterval( unitSize );

		ByteType constant = new ByteType( (byte)1 );
		return Views.flatIterable( 
				ConstantUtils.constantRandomAccessibleInterval( constant, nd, boxInterval )).cursor();
		
//		long[] step = Intervals.dimensionsAsLongArray( interval );
//		for( int i = 0 ; i < nd; i++ )
//		{
//			step[ i ] = 1;
//		}
//		
//		System.out.println( "step: " + 
//				step[0] + " " +
//				step[1] + " " +
//				step[2] + "\n" );
//		
//		ByteType constant = new ByteType( (byte)1 );
//		return Views.flatIterable(
//				Views.subsample(
//						ConstantUtils.constantRandomAccessibleInterval( constant, nd, interval ),
//						step )).cursor();
	}

}
